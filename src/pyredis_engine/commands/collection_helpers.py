"""Preflight bounds before collection writes or large reply construction."""
from __future__ import annotations

from collections.abc import Iterable

from ..config import ServerConfig
from ..protocol import Array, Bulk, Reply, reply_size
from .registry import CommandError


def check_elements(elements: Iterable[bytes], config: ServerConfig) -> None:
    for element in elements:
        if len(element) > config.max_bulk_bytes:
            raise CommandError(b"ERR collection element exceeds configured maximum size")


def check_growth(count: int, size: int, config: ServerConfig) -> None:
    if count > config.max_collection_items or size > config.max_collection_bytes:
        raise CommandError(b"ERR collection exceeds configured maximum size")


def checked_bulk(value: bytes, config: ServerConfig) -> Bulk:
    reply = Bulk(value)
    if reply_size(reply) > config.max_reply_bytes:
        raise CommandError(b"ERR reply exceeds configured maximum size")
    return reply


def checked_array(values: Iterable[bytes], count: int, config: ServerConfig) -> Array:
    size = len(str(count)) + 3
    items: list[Reply] = []
    for value in values:
        size += len(str(len(value))) + len(value) + 5
        if size > config.max_reply_bytes:
            raise CommandError(b"ERR reply exceeds configured maximum size")
        items.append(Bulk(value))
    return Array(tuple(items))
