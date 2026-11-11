"""Typed Pub/Sub reads over the existing raw TCP peer utility."""
from __future__ import annotations

import asyncio

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Reply, read_value

from .comparison import Peer


def ack(kind: bytes, channel: bytes | None, count: int) -> Array:
    return Array((Bulk(kind), Bulk(channel), Integer(count)))


def message(channel: bytes, payload: bytes) -> Array:
    return Array((Bulk(b'message'), Bulk(channel), Bulk(payload)))


async def read(peer: Peer, count: int = 1) -> list[Reply]:
    async with asyncio.timeout(5):
        replies = []
        for _ in range(count):
            reply = await read_value(peer.reader, ServerConfig(max_args=10000))
            assert reply is not None
            replies.append(reply)
        return replies
