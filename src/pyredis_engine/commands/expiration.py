"""M03 expiration commands and option parsing; validation precedes mutation."""
from __future__ import annotations

from dataclasses import dataclass

from ..config import ServerConfig
from ..engine.store import Store
from ..protocol import INT64_MAX, INT64_MIN, Bulk, Integer, Reply, reply_size
from .numbers import integer
from .registry import Command, CommandError, Handler


def deadline(
    raw: bytes, *, seconds: bool, relative: bool, now: int, positive: bool, name: bytes,
) -> int:
    number = integer(raw)
    invalid = b"ERR invalid expire time in '" + name.lower() + b"' command"
    if positive and number <= 0:
        raise CommandError(invalid)
    milliseconds = number * (1000 if seconds else 1)
    if not INT64_MIN <= milliseconds <= INT64_MAX:
        raise CommandError(invalid)
    absolute = milliseconds + now if relative else milliseconds
    if not INT64_MIN <= absolute <= INT64_MAX:
        raise CommandError(invalid)
    return absolute


@dataclass
class SetOptions:
    expires_at_ms: int | None = None
    keep_ttl: bool = False
    nx: bool = False
    xx: bool = False


def set_options(arguments: tuple[bytes, ...], now: int) -> SetOptions:
    options = SetOptions()
    unit: bytes | None = None
    index = 0
    while index < len(arguments):
        token = arguments[index].upper()
        if token == b"NX" and not options.xx:
            options.nx = True
        elif token == b"XX" and not options.nx:
            options.xx = True
        elif token == b"KEEPTTL" and unit is None:
            options.keep_ttl = True
        elif token in (b"EX", b"PX") and not options.keep_ttl and unit in (None, token):
            if index + 1 == len(arguments):
                raise CommandError(b"ERR syntax error")
            index += 1
            unit = token
            options.expires_at_ms = deadline(
                arguments[index], seconds=token == b"EX", relative=True,
                now=now, positive=True, name=b"set")
        else:
            raise CommandError(b"ERR syntax error")
        index += 1
    return options


def expire_handler(name: bytes, *, seconds: bool, relative: bool) -> Handler:
    def expire(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
        absolute = deadline(args[1], seconds=seconds, relative=relative, now=store.now_ms,
                            positive=False, name=name)
        return Integer(int(store.set_deadline(args[0], absolute)))
    return expire


def pttl(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    if not store.contains(args[0]):
        return Integer(-2)
    absolute = store.deadline(args[0])
    return Integer(-1 if absolute is None else absolute - store.now_ms)


def ttl(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    remaining = pttl(store, args, config)
    assert isinstance(remaining, Integer)
    value = remaining.payload
    return Integer(value if value < 0 else (value + 500) // 1000)


def persist(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Integer(int(store.persist(args[0])))


def getex(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    absolute: int | None = None
    raw_timeout: bytes | None = None
    remove_ttl = False
    unit: bytes | None = None
    index = 1
    while index < len(args):
        token = args[index].upper()
        if token == b"PERSIST" and unit is None:
            remove_ttl = True
        elif (token in (b"EX", b"PX", b"EXAT", b"PXAT") and not remove_ttl
              and unit in (None, token) and index + 1 < len(args)):
            index += 1
            unit = token
            raw_timeout = args[index]
        else:
            raise CommandError(b"ERR syntax error")
        index += 1
    value = store.get_string(args[0])
    if value is not None and raw_timeout is not None:
        absolute = deadline(raw_timeout, seconds=unit in (b"EX", b"EXAT"),
                            relative=unit in (b"EX", b"PX"), now=store.now_ms,
                            positive=True, name=b"getex")
    reply = Bulk(value)
    if reply_size(reply) > config.max_reply_bytes:
        raise CommandError(b"ERR reply exceeds configured maximum size")
    if value is not None:
        if remove_ttl:
            store.persist(args[0])
        elif absolute is not None:
            store.set_deadline(args[0], absolute)
    return reply


COMMANDS = (
    Command(b"EXPIRE", 2, 2, expire_handler(b"expire", seconds=True, relative=True), writes=True),
    Command(b"PEXPIRE", 2, 2,
            expire_handler(b"pexpire", seconds=False, relative=True), writes=True),
    Command(b"EXPIREAT", 2, 2,
            expire_handler(b"expireat", seconds=True, relative=False), writes=True),
    Command(b"PEXPIREAT", 2, 2,
            expire_handler(b"pexpireat", seconds=False, relative=False), writes=True),
    Command(b"TTL", 1, 1, ttl),
    Command(b"PTTL", 1, 1, pttl),
    Command(b"PERSIST", 1, 1, persist, writes=True),
    Command(b"GETEX", 1, None, getex, writes=True),
)
