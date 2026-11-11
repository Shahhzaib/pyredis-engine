"""Security command controls; credential preparation never mutates executor state."""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING

from ..commands.registry import Command, CommandResult
from ..protocol import Array, Bulk, Error, Integer, Simple
from .permissions import MatchBudget
from .policy import UNSUPPORTED, WRONGPASS, AccessControl, Authentication, PolicyUpdate
from .worker import SecurityBusy, SecurityWorker

if TYPE_CHECKING:
    from ..engine.executor import Executor
    from ..engine.transactions import ClientState

COMMANDS = (Command(b"AUTH", 1, None, None), Command(b"ACL", 1, None, None))


class SecurityCommands:
    def __init__(self, owner: Executor, access: AccessControl, replication_enabled: bool) -> None:
        self.owner, self.access = owner, access
        self.replication_enabled = replication_enabled
        self.worker = SecurityWorker()

    async def prepare(self, args: list[bytes], client: ClientState) -> object:
        operation: Callable[[], object]
        if args[0].upper() == b"AUTH":
            if len(args) > 3:
                return Error(b"ERR syntax error")
            name = b"default" if len(args) == 2 else args[1]
            snapshot = self.access.users.get(name)
            operation = partial(self.access.authentication, args, snapshot)
        else:
            if self.replication_enabled:
                return Error(b"ERR ACL mutations with replication require E01B")
            snapshot = self.access.users.get(args[2])
            operation = partial(self.access.prepare_update, args[2], tuple(args[3:]), snapshot)
        try:
            return await self.worker.run(operation)
        except SecurityBusy:
            return Error(b"BUSY Authentication service is busy")
        except ValueError:
            return Error(b"ERR invalid or unsupported authentication/ACL request")

    def dispatch(self, args: list[bytes], client: ClientState) -> CommandResult:
        if isinstance(client.security_prepared, Error):
            return CommandResult(client.security_prepared)
        if args[0].upper() == b"AUTH":
            return self._auth(args, client)
        sub = args[1].upper()
        if sub == b"WHOAMI" and len(args) == 2:
            user = self.access.current(client)
            assert user is not None
            return CommandResult(Bulk(user.name))
        if sub == b"USERS" and len(args) == 2:
            return CommandResult(Array(tuple(Bulk(name) for name in sorted(self.access.users))))
        if sub == b"DRYRUN" and len(args) >= 4:
            return self._dryrun(args)
        if sub == b"SETUSER" and len(args) >= 3:
            return self._setuser(args, client)
        if sub == b"DELUSER" and len(args) >= 3:
            return self._deluser(args)
        return CommandResult(UNSUPPORTED)

    def _auth(self, args: list[bytes], client: ClientState) -> CommandResult:
        if len(args) > 3:
            return CommandResult(Error(b"ERR syntax error"))
        proof = client.security_prepared
        if not isinstance(proof, Authentication):
            name = b"default" if len(args) == 2 else args[1]
            user = self.access.users.get(name)
            if user is None or not user.nopass:
                return CommandResult(Error(b"ERR authentication service unavailable"))
            proof = Authentication(user, user.enabled, len(args) == 2)
        if proof.user is not None and proof.password_only and proof.user.nopass:
            return CommandResult(
                Error(
                    b"ERR AUTH <password> called without any password configured "
                    b"for the default user. "
                    b"Are you sure your configuration is correct?"
                )
            )
        current = self.access.users.get(proof.user.name) if proof.user is not None else None
        if not proof.valid or current is not proof.user:
            client.auth_failures += 1
            return CommandResult(WRONGPASS, close=client.auth_failures >= 5)
        assert current is not None
        self.owner.discard_transaction(client)
        client.username, client.identity = current.name, current.identity
        client.auth_failures = 0
        return CommandResult(Simple(b"OK"))

    def _setuser(self, args: list[bytes], client: ClientState) -> CommandResult:
        if self.replication_enabled:
            return CommandResult(Error(b"ERR ACL mutations with replication require E01B"))
        proof = client.security_prepared
        if not isinstance(proof, PolicyUpdate):
            if any(rule.startswith((b">", b"<")) for rule in args[3:]):
                return CommandResult(Error(b"ERR authentication service unavailable"))
            try:
                proof = self.access.prepare_update(
                    args[2], tuple(args[3:]), self.access.users.get(args[2])
                )
            except ValueError:
                return CommandResult(UNSUPPORTED)
        name = proof.replacement.name
        if self.access.users.get(name) is not proof.previous:
            return CommandResult(Error(b"ERR ACL policy changed; retry the request"))
        users = self.access.users | {name: proof.replacement}
        try:
            self.access.validate(users)
        except ValueError:
            return CommandResult(Error(b"ERR invalid or unsafe ACL policy"))
        self.access.users = users
        self.owner.refresh_acl({name})
        return CommandResult(Simple(b"OK"))

    def _deluser(self, args: list[bytes]) -> CommandResult:
        if self.replication_enabled:
            return CommandResult(Error(b"ERR ACL mutations with replication require E01B"))
        if b"default" in args[2:]:
            return CommandResult(Error(b"ERR The 'default' user cannot be removed"))
        removed = set(args[2:]) & self.access.users.keys()
        users = {name: user for name, user in self.access.users.items() if name not in removed}
        try:
            self.access.validate(users)
        except ValueError:
            return CommandResult(Error(b"ERR invalid or unsafe ACL policy"))
        self.access.users = users
        self.owner.refresh_acl(removed, deleted=True)
        return CommandResult(Integer(len(removed)))

    def _dryrun(self, args: list[bytes]) -> CommandResult:
        user = self.access.users.get(args[2])
        if user is None:
            return CommandResult(Error(b"ERR User does not exist"))
        command = args[3:]
        resolved = self.access.registry.resolve(command)
        if isinstance(resolved, Error):
            return CommandResult(resolved)
        denied = self.access.check_user(user, command, verbose=True)
        return CommandResult(
            Bulk(denied.payload.removeprefix(b"NOPERM ")) if denied else Simple(b"OK")
        )

    def channels_allowed(self, client: ClientState) -> bool:
        user = self.access.current(client)
        if user is None:
            return False
        assert client.subscriber is not None
        budget = MatchBudget()
        return all(
            any(pattern.matches(channel, budget) for pattern in user.channels)
            for channel in client.subscriber.channels
        ) and all(any(allowed.source in (b"*", pattern) for allowed in user.channels)
                  for pattern in client.subscriber.patterns)
