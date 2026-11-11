"""M04 hash commands; mutation validation precedes writes and preserves TTL."""
from __future__ import annotations

from ..config import ServerConfig
from ..engine.collections import HashValue
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import Array, Bulk, Integer, Reply, reply_size
from .collection_helpers import check_elements, check_growth, checked_array
from .registry import Command, CommandError


def hset(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    if value is None:
        value = HashValue()
    check_elements(args[1:], config)
    updates = dict(zip(args[1::2], args[2::2], strict=True))
    with preparing():
        added_fields = {field for field in updates if field not in value.fields}
    added = len(added_fields)
    size = value.byte_size
    for field, payload in updates.items():
        previous = value.fields.get(field)
        if previous is None:
            size += len(field) + len(payload)
        else:
            size += len(payload) - len(previous)
    check_growth(len(value.fields) + added, size, config)
    store.check_growth(args[0], b'hash', size, len(value.fields) + added)
    index = store.update_scan_index(value.scan_index, added_fields, set())
    with preparing():
        fields = value.fields.copy()
        fields.update(updates)
        value = HashValue(fields, size)
    store.put(args[0], value, keep_ttl=True, _prepared_scan_index=index)
    return Integer(added)


def hget(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    return Bulk(None if value is None else value.fields.get(args[1]))


def hmget(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    replies: list[Reply] = []
    size = len(str(len(args) - 1)) + 3
    for field in args[1:]:
        item = Bulk(None if value is None else value.fields.get(field))
        size += reply_size(item)
        if size > config.max_reply_bytes:
            raise CommandError(b"ERR reply exceeds configured maximum size")
        replies.append(item)
    return Array(tuple(replies))


def hstrlen(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    return Integer(0 if value is None else len(value.fields.get(args[1], b"")))


def hsetnx(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    if value is not None and args[1] in value.fields:
        return Integer(0)
    return hset(store, args, config)


def hdel(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    if value is None:
        return Integer(0)
    fields = {field for field in args[1:] if field in value.fields}
    if not fields:
        return Integer(0)
    if len(fields) == len(value.fields):
        store.delete(args[0])
        return Integer(len(fields))
    index = store.update_scan_index(value.scan_index, set(), fields)
    with preparing():
        value = HashValue(value.fields.copy(), value.byte_size)
        for field in fields:
            value.byte_size -= len(field) + len(value.fields.pop(field))
    store.put(args[0], value, keep_ttl=True, _prepared_scan_index=index)
    return Integer(len(fields))


def hexists(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    return Integer(int(value is not None and args[1] in value.fields))


def hgetall(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    if value is None:
        return Array(())
    return checked_array((part for pair in value.fields.items() for part in pair),
                         2 * len(value.fields), config)


def hlen(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    return Integer(0 if value is None else len(value.fields))


def hkeys(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    return Array(()) if value is None else checked_array(value.fields, len(value.fields), config)


def hvals(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_hash(args[0])
    return Array(()) if value is None else checked_array(
        value.fields.values(), len(value.fields), config)


COMMANDS = (
    Command(b"HSET", 3, None, hset, step=2, writes=True),
    Command(b"HGET", 2, 2, hget),
    Command(b"HMGET", 2, None, hmget),
    Command(b"HSTRLEN", 2, 2, hstrlen),
    Command(b"HSETNX", 3, 3, hsetnx, writes=True),
    Command(b"HDEL", 2, None, hdel, writes=True),
    Command(b"HEXISTS", 2, 2, hexists),
    Command(b"HGETALL", 1, 1, hgetall),
    Command(b"HLEN", 1, 1, hlen),
    Command(b"HKEYS", 1, 1, hkeys),
    Command(b"HVALS", 1, 1, hvals),
)
