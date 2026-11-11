"""Existing connection commands routed through central command validation."""
from __future__ import annotations

from ..config import ServerConfig
from ..engine.store import Store
from ..protocol import Bulk, Reply, Simple
from .registry import Command


def ping(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Bulk(args[0]) if args else Simple(b"PONG")


def echo(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Bulk(args[0])


def quit_connection(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Simple(b"OK")


COMMANDS = (
    Command(b"PING", 0, 1, ping, queue_variadic=True),
    Command(b"ECHO", 1, 1, echo),
    Command(b"QUIT", 0, 0, quit_connection, closes=True),
)
