"""Binary set membership with atomic growth checks and TTL-preserving writes."""
from __future__ import annotations

from ..config import ServerConfig
from ..engine.collections import SetValue
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import Array, Integer, Reply
from .collection_helpers import check_elements, check_growth, checked_array
from .registry import Command


def sadd(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_set(args[0])
    if value is None:
        value = SetValue()
    check_elements(args[1:], config)
    added = set(args[1:]).difference(value.members)
    size = value.byte_size + sum(map(len, added))
    check_growth(len(value.members) + len(added), size, config)
    if added:
        store.check_growth(args[0], b'set', size, len(value.members) + len(added))
        index = store.update_scan_index(value.scan_index, added, set())
        with preparing():
            value = SetValue(value.members.union(added), size)
        store.put(args[0], value, keep_ttl=True, _prepared_scan_index=index)
    return Integer(len(added))


def srem(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_set(args[0])
    if value is None:
        return Integer(0)
    removed = {member for member in args[1:] if member in value.members}
    if removed:
        if len(removed) == len(value.members):
            store.delete(args[0])
            return Integer(len(removed))
        index = store.update_scan_index(value.scan_index, set(), removed)
        with preparing():
            value = SetValue(value.members.difference(removed),
                             value.byte_size - sum(map(len, removed)))
        store.put(args[0], value, keep_ttl=True, _prepared_scan_index=index)
    return Integer(len(removed))


def sismember(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_set(args[0])
    return Integer(int(value is not None and args[1] in value.members))


def smismember(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_set(args[0])
    return Array(tuple(Integer(int(value is not None and member in value.members))
                       for member in args[1:]))


def smembers(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_set(args[0])
    if value is None:
        return Array(())
    return checked_array(sorted(value.members), len(value.members), config)


def scard(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_set(args[0])
    return Integer(0 if value is None else len(value.members))


COMMANDS = (
    Command(b"SADD", 2, None, sadd, writes=True),
    Command(b"SREM", 2, None, srem, writes=True),
    Command(b"SISMEMBER", 2, 2, sismember),
    Command(b"SMISMEMBER", 2, None, smismember),
    Command(b"SMEMBERS", 1, 1, smembers),
    Command(b"SCARD", 1, 1, scard),
)
