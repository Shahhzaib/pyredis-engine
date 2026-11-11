"""Immutable, bounded ACL users; only the owning executor installs a new policy."""

from __future__ import annotations

import json
import os
import secrets
import stat
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

from ..protocol import Error
from .credentials import MAX_PASSWORD_BYTES, PasswordHash
from .permissions import ACL_SUBCOMMANDS, KEY_COMMANDS, MatchBudget, Pattern, command_keys

if TYPE_CHECKING:
    from ..commands.registry import Registry
    from ..engine.transactions import ClientState

NOAUTH = Error(b"NOAUTH Authentication required.")
WRONGPASS = Error(b"WRONGPASS invalid username-password pair or user is disabled.")
UNSUPPORTED = Error(b"ERR unsupported ACL syntax or subcommand")
MAX_USERS, MAX_PATTERNS, MAX_RULES, MAX_PASSWORDS = 64, 32, 64, 4
MAX_POLICY_BYTES = 65536


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate ACL configuration field")
        result[key] = value
    return result


def valid_username(name: bytes) -> bool:
    return 1 <= len(name) <= 64 and all(33 <= value <= 126 for value in name)


@dataclass(frozen=True, slots=True)
class User:
    name: bytes
    enabled: bool = False
    nopass: bool = False
    commands: frozenset[bytes] = frozenset()
    keys: tuple[Pattern, ...] = ()
    channels: tuple[Pattern, ...] = ()
    passwords: tuple[PasswordHash, ...] = field(default=(), repr=False)
    identity: object = field(default_factory=object, repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class Authentication:
    user: User | None
    valid: bool
    password_only: bool


@dataclass(frozen=True, slots=True, repr=False)
class PolicyUpdate:
    previous: User | None
    replacement: User


class AccessControl:
    def __init__(self, registry: Registry, acl_file: str | None,
                 secure_replication: bool = False) -> None:
        self.registry = registry
        if registry.names != KEY_COMMANDS:
            raise RuntimeError("registry commands require explicit ACL key specifications")
        self.command_names = registry.names | {b"INFO"}
        self.all_commands = (self.command_names - {b"ACL"}) | {b"PYREDIS.SYNC"} | {
            b"ACL|" + name for name in ACL_SUBCOMMANDS
        }
        self.file_protected = acl_file is not None
        self.secure_replication = secure_replication
        self.users: dict[bytes, User] = {
            b"default": User(
                b"default",
                enabled=not self.file_protected,
                nopass=not self.file_protected,
                commands=self.all_commands if not self.file_protected else frozenset(),
                keys=(Pattern.compile(b"*"),) if not self.file_protected else (),
                channels=(Pattern.compile(b"*"),) if not self.file_protected else (),
            )
        }
        self.dummy = PasswordHash(secrets.token_bytes(16), secrets.token_bytes(32))
        if acl_file is not None:
            self._load(acl_file)

    def current(self, client: ClientState) -> User | None:
        user = self.users.get(client.username or b"")
        return user if user is not None and user.identity is client.identity else None

    def default(self) -> User | None:
        user = self.users[b"default"]
        return user if user.enabled and user.nopass else None

    @staticmethod
    def protected(users: dict[bytes, User]) -> bool:
        return not users[b"default"].enabled or not users[b"default"].nopass

    def validate(self, users: dict[bytes, User]) -> None:
        size = sum(
            len(user.name)
            + sum(len(p.source) for p in (*user.keys, *user.channels))
            + sum(map(len, user.commands))
            + 128 * len(user.passwords)
            for user in users.values()
        )
        if len(users) > MAX_USERS or size > MAX_POLICY_BYTES:
            raise ValueError("ACL policy exceeds configured limits")
        if self.file_protected or self.protected(users):
            if any(user.enabled and user.nopass for user in users.values()):
                raise ValueError("protected ACL users must require passwords")
            if not any(
                user.enabled
                and user.passwords
                and {b"ACL|SETUSER", b"ACL|DELUSER"} <= user.commands
                for user in users.values()
            ):
                raise ValueError("protected ACL policy requires a usable administrator")

    def _load(self, filename: str) -> None:
        try:
            path = Path(filename)
            flags = (
                os.O_RDONLY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
                | getattr(os, "O_BINARY", 0)
            )
            with os.fdopen(os.open(path, flags), "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ValueError("ACL file must be a regular file")
                if os.name == "posix" and os.fstat(stream.fileno()).st_mode & 0o077:
                    raise ValueError("ACL file must be private")
                raw = stream.read(MAX_POLICY_BYTES + 1)
            if len(raw) > MAX_POLICY_BYTES:
                raise ValueError("ACL file too large")
            document = json.loads(raw, object_pairs_hook=_unique_object)
            if (
                not isinstance(document, dict)
                or set(document) != {"version", "users"}
                or type(document["version"]) is not int
                or document["version"] != 1
                or not isinstance(document["users"], list)
                or not 1 <= len(document["users"]) <= MAX_USERS
            ):
                raise ValueError("invalid ACL schema")
            users = self.users.copy()
            seen: set[bytes] = set()
            for item in document["users"]:
                if (
                    not isinstance(item, dict)
                    or set(item) != {"name", "rules", "passwords"}
                    or not isinstance(item["name"], str)
                    or not isinstance(item["rules"], list)
                    or not all(isinstance(rule, str) for rule in item["rules"])
                    or not isinstance(item["passwords"], list)
                    or not all(isinstance(value, str) for value in item["passwords"])
                    or len(item["passwords"]) > MAX_PASSWORDS
                ):
                    raise ValueError("invalid ACL user")
                name = item["name"].encode("ascii")
                rules = tuple(rule.encode("utf-8") for rule in item["rules"])
                if (
                    not valid_username(name)
                    or name in seen
                    or any(rule.startswith((b">", b"<", b"#", b"!")) for rule in rules)
                ):
                    raise ValueError("invalid ACL user")
                seen.add(name)
                user = self.edit(User(name), rules)
                users[name] = replace(
                    user, passwords=tuple(PasswordHash.parse(value) for value in item["passwords"])
                )
            self.validate(users)
            self.users = users
        except (OSError, UnicodeError, ValueError, TypeError, KeyError, RecursionError):
            raise ValueError("invalid or unsafe ACL configuration") from None

    def permission_token(self, args: list[bytes]) -> bytes:
        name = args[0].upper()
        return name + b"|" + args[1].upper() if name == b"ACL" and len(args) > 1 else name

    def command_denied(self, user: User, token: bytes) -> Error:
        # Only validated usernames and known command names enter error text.
        command = token.lower() if token in self.all_commands else b"acl"
        return Error(
            b"NOPERM User "
            + user.name
            + b" has no permissions to run the '"
            + command
            + b"' command"
        )

    def check_user(self, user: User, args: list[bytes], *, verbose: bool = False) -> Error | None:
        name, token = args[0].upper(), self.permission_token(args)
        if name not in self.command_names:
            return None  # registry rejects unknown commands without echoing arguments
        if token not in user.commands:
            return self.command_denied(user, token)
        budget = MatchBudget()
        for key in command_keys(args):
            if not any(pattern.matches(key, budget) for pattern in user.keys):
                return (
                    Error(
                        b"NOPERM User "
                        + user.name
                        + b" has no permissions to access the '"
                        + key
                        + b"' key"
                    )
                    if verbose
                    else Error(b"NOPERM No permissions to access a key")
                )
        channels = (
            args[1:2]
            if name == b"PUBLISH"
            else args[1:]
            if name in (b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE", b"PUNSUBSCRIBE")
            else []
        )
        for channel in channels:
            pattern_command = name in (b"PSUBSCRIBE", b"PUNSUBSCRIBE")
            allowed = (any(pattern.source in (b"*", channel) for pattern in user.channels)
                       if pattern_command else
                       any(pattern.matches(channel, budget) for pattern in user.channels))
            if not allowed:
                return (
                    Error(
                        b"NOPERM User "
                        + user.name
                        + b" has no permissions to access the '"
                        + channel
                        + b"' channel"
                    )
                    if verbose
                    else Error(b"NOPERM No permissions to access a channel")
                )
        return None

    def authorize(self, args: list[bytes], client: ClientState) -> Error | None:
        if args and args[0].upper() in (b"AUTH", b"QUIT"):
            return None
        user = self.current(client)
        if user is None:
            return NOAUTH
        if args and args[0].upper() == b"PYREDIS.SYNC":
            return self.authorize_peer(client)
        return self.check_user(user, args) if args else None

    def authorize_peer(self, client: ClientState) -> Error | None:
        user = self.current(client)
        if user is None:
            return NOAUTH
        if self.secure_replication:
            if user.enabled and user.passwords and not user.nopass \
                    and b'PYREDIS.SYNC' in user.commands:
                return None
            return Error(b'NOPERM replication permission required')
        if (
            self.file_protected
            or self.protected(self.users)
            or user.name != b"default"
            or user.commands != self.all_commands
            or not any(p.source == b"*" for p in user.keys)
        ):
            return Error(b"NOPERM Protected replication requires E01B peer authentication")
        return None

    def authentication(self, args: list[bytes], user: User | None) -> Authentication:
        password = args[-1]
        if len(password) > MAX_PASSWORD_BYTES:
            raise ValueError("invalid authentication request")
        valid = bool(user is not None and user.enabled and user.nopass)
        hashes = user.passwords if user is not None and user.passwords else (self.dummy,)
        if not valid:
            matched = False
            for verifier in hashes:
                matched |= verifier.verify(password)
            valid = bool(user is not None and user.enabled and user.passwords and matched)
        return Authentication(user, valid, len(args) == 2)

    def edit(self, original: User, rules: tuple[bytes, ...]) -> User:
        if (
            len(rules) > MAX_RULES
            or sum(map(len, rules)) > 16384
            or sum(rule.startswith((b">", b"<")) for rule in rules) > 1
        ):
            raise ValueError("ACL rule limits exceeded")
        user = original
        patterns: tuple[Pattern, ...]
        for rule in rules:
            if not rule.startswith((b">", b"<", b"~", b"&")):
                rule = rule.lower()
            if rule == b"reset":
                user = User(original.name, identity=original.identity)
            elif rule in (b"on", b"off"):
                user = replace(user, enabled=rule == b"on")
            elif rule in (b"resetpass", b"nopass"):
                user = replace(user, passwords=(), nopass=rule == b"nopass")
            elif rule.startswith((b">", b"<")):
                password = rule[1:]
                if not 1 <= len(password) <= MAX_PASSWORD_BYTES:
                    raise ValueError("invalid password rule")
                retained, found = [], False
                for verifier in user.passwords:
                    matches = verifier.verify(password)
                    found |= matches
                    if not matches or rule.startswith(b">"):
                        retained.append(verifier)
                if rule.startswith(b">") and not found:
                    retained.append(PasswordHash.create(password))
                if rule.startswith(b"<") and not found:
                    raise ValueError("password is not configured")
                if len(retained) > MAX_PASSWORDS:
                    raise ValueError("password slots exceeded")
                user = replace(user, passwords=tuple(retained), nopass=False)
            elif rule in (b"allcommands", b"+@all", b"nocommands", b"-@all"):
                user = replace(
                    user,
                    commands=self.all_commands
                    if rule in (b"allcommands", b"+@all")
                    else frozenset(),
                )
            elif rule.startswith((b"+", b"-")):
                token = rule[1:].upper()
                additions = (
                    {b"ACL|" + sub for sub in ACL_SUBCOMMANDS} if token == b"ACL" else {token}
                )
                if not additions <= self.all_commands:
                    raise ValueError("unsupported ACL command or category")
                user = replace(
                    user,
                    commands=(
                        user.commands | additions if rule[:1] == b"+" else user.commands - additions
                    ),
                )
            elif rule in (b"allkeys", b"resetkeys", b"allchannels", b"resetchannels"):
                patterns = (Pattern.compile(b"*"),) if rule.startswith(b"all") else ()
                user = (
                    replace(user, keys=patterns)
                    if rule.endswith(b"keys")
                    else replace(user, channels=patterns)
                )
            elif rule.startswith((b"~", b"&")):
                patterns = user.keys if rule[:1] == b"~" else user.channels
                if rule[1:] not in {pattern.source for pattern in patterns}:
                    patterns = (*patterns, Pattern.compile(rule[1:]))
                if len(patterns) > MAX_PATTERNS:
                    raise ValueError("ACL pattern limit exceeded")
                user = (
                    replace(user, keys=patterns)
                    if rule[:1] == b"~"
                    else replace(user, channels=patterns)
                )
            else:
                raise ValueError("unsupported ACL rule")
        return user

    def prepare_update(
        self, name: bytes, rules: tuple[bytes, ...], previous: User | None
    ) -> PolicyUpdate:
        if not valid_username(name):
            raise ValueError("invalid ACL username")
        return PolicyUpdate(previous, self.edit(previous or User(name), rules))
