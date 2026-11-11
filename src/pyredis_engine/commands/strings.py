"""M02 string operations; all validation precedes observable writes."""
from __future__ import annotations

from ..config import ServerConfig
from ..engine.store import Store, StringValue
from ..ops.memory import preparing
from ..protocol import INT64_MAX, INT64_MIN, Array, Bulk, Integer, Reply, Simple
from .collection_helpers import checked_bulk
from .expiration import set_options
from .numbers import integer as _integer
from .registry import Command, CommandError


def _check_size(size: int, config: ServerConfig) -> None:
    if size > config.max_bulk_bytes:
        raise CommandError(b"ERR string exceeds configured maximum size")


def set_string(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    options = set_options(args[2:], store.now_ms)
    _check_size(len(args[1]), config)
    present = store.contains(args[0])
    if (options.nx and present) or (options.xx and not present):
        return Bulk(None)
    store.put(args[0], StringValue(args[1]), keep_ttl=options.keep_ttl,
              expires_at_ms=options.expires_at_ms)
    return Simple(b"OK")


def get_string(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Bulk(store.get_string(args[0]))


def getdel(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_string(args[0])
    if value is None:
        return Bulk(None)
    reply = checked_bulk(value, config)
    store.delete(args[0])
    return reply


def getrange(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    start, end = _integer(args[1]), _integer(args[2])
    value = store.get_string(args[0]) or b""
    length = len(value)
    start = max(0, start + length if start < 0 else start)
    end = min(length - 1, max(0, end + length) if end < 0 else end)
    return checked_bulk(value[start:end + 1] if start <= end else b"", config)


def setnx(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    _check_size(len(args[1]), config)
    if store.contains(args[0]):
        return Integer(0)
    store.put(args[0], StringValue(args[1]))
    return Integer(1)


def append(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    old = store.get_string(args[0]) or b""
    size = len(old) + len(args[1])
    _check_size(size, config)
    store.check_growth(args[0], b'string', size)
    with preparing():
        value = StringValue(old + args[1])
    store.put(args[0], value, keep_ttl=True)
    return Integer(size)


def strlen(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Integer(len(store.get_string(args[0]) or b""))


def _increment(store: Store, key: bytes, delta: int, config: ServerConfig) -> Reply:
    raw = store.get_string(key)
    current = 0 if raw is None else _integer(raw)
    result = current + delta
    if not INT64_MIN <= result <= INT64_MAX:
        raise CommandError(b"ERR increment or decrement would overflow")
    encoded = str(result).encode("ascii")
    _check_size(len(encoded), config)
    store.put(key, StringValue(encoded), keep_ttl=True)
    return Integer(result)


def incr(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return _increment(store, args[0], 1, config)


def decr(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return _increment(store, args[0], -1, config)


def incrby(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return _increment(store, args[0], _integer(args[1]), config)


def decrby(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    delta = _integer(args[1])
    if delta == INT64_MIN:
        raise CommandError(b"ERR decrement would overflow")
    return _increment(store, args[0], -delta, config)


def mget(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    values: list[Reply] = []
    for key in args:
        value = store.get(key)
        values.append(Bulk(value.data if isinstance(value, StringValue) else None))
    return Array(tuple(values))


def mset(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    for value in args[1::2]:
        _check_size(len(value), config)
    with preparing():
        pairs = [(args[offset], StringValue(args[offset + 1]))
                 for offset in range(0, len(args), 2)]
        final = dict(pairs)
    for key in final:
        store.get(key)  # expire before computing replacement credits
    with store.memory_batch(final):
        for key, stored in pairs:
            store.put(key, stored)
    return Simple(b"OK")


COMMANDS = (
    Command(b"SET", 2, None, set_string, writes=True),
    Command(b"GET", 1, 1, get_string),
    Command(b"GETDEL", 1, 1, getdel, writes=True),
    Command(b"GETRANGE", 3, 3, getrange),
    Command(b"SETNX", 2, 2, setnx, writes=True),
    Command(b"APPEND", 2, 2, append, writes=True),
    Command(b"STRLEN", 1, 1, strlen),
    Command(b"INCR", 1, 1, incr, writes=True),
    Command(b"DECR", 1, 1, decr, writes=True),
    Command(b"INCRBY", 2, 2, incrby, writes=True),
    Command(b"DECRBY", 2, 2, decrby, writes=True),
    Command(b"MGET", 1, None, mget),
    Command(b"MSET", 2, None, mset, step=2, writes=True),
)
