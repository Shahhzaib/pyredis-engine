"""M02 keyspace operations for the single in-memory database."""
from __future__ import annotations

from ..config import ServerConfig
from ..engine.store import Store
from ..protocol import Integer, Reply, Simple
from .registry import Command, CommandError


def delete(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Integer(sum(store.delete(key) for key in args))


def exists(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Integer(sum(store.contains(key) for key in args))


def key_type(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get(args[0])
    return Simple(value.kind if value is not None else b"none")


def dbsize(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Integer(len(store))


def flushdb(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    if args:
        raise CommandError(b"ERR syntax error")
    store.clear()
    return Simple(b"OK")


COMMANDS = (
    Command(b"SCAN", 1, None, None),
    Command(b"HSCAN", 2, None, None),
    Command(b"SSCAN", 2, None, None),
    Command(b"ZSCAN", 2, None, None),
    Command(b"DEL", 1, None, delete, writes=True),
    Command(b"EXISTS", 1, None, exists),
    Command(b"TYPE", 1, 1, key_type),
    Command(b"DBSIZE", 0, 0, dbsize),
    Command(b"FLUSHDB", 0, None, flushdb, writes=True),
)
