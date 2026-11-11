"""Bounded, typed TCP comparison helpers; no datastore mutation or revision oracle."""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Reply, Simple, read_value, serialize_reply
from pyredis_engine.server import ManagedServer

from .acceptance_cases import Expected, TTLRange, assert_expected, normalize
from .redis_reference import close_writer, connect, exchange


class Peer:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader, self.writer = reader, writer

    async def call(self, *command: bytes) -> Reply:
        return await exchange(self.reader, self.writer, command)

    async def pipeline(self, commands: list[tuple[bytes, ...]]) -> list[Reply]:
        async with asyncio.timeout(5):
            self.writer.write(b"".join(serialize_reply(Array(tuple(Bulk(x) for x in command)))
                                       for command in commands))
            await self.writer.drain()
            replies = []
            for _ in commands:
                reply = await read_value(self.reader, ServerConfig(max_args=10_000))
                assert reply is not None
                replies.append(reply)
            return replies

@asynccontextmanager
async def peer(port: int) -> AsyncIterator[Peer]:
    reader, writer = await connect(port)
    try:
        yield Peer(reader, writer)
    finally:
        await close_writer(writer)

class Pair:
    def __init__(self, local: Peer, oracle: Peer, server: ManagedServer) -> None:
        self.local, self.oracle, self.server = local, oracle, server
        self.deadline_slack: dict[bytes, int] = {}

    async def check(self, command: tuple[bytes, ...], expected: Expected | None = None) -> Reply:
        started = time.monotonic_ns()
        actual = await self.local.call(*command)
        reference = await self.oracle.call(*command)
        elapsed = (time.monotonic_ns() - started + 999_999) // 1_000_000
        name = command[0]
        relative_changed = (
            name in (b"EXPIRE", b"PEXPIRE") and actual == Integer(1)
            or name == b"SET" and actual == Simple(b"OK")
            and any(token in (b"EX", b"PX") for token in command[3:])
            or name == b"GETEX" and isinstance(actual, Bulk) and actual.payload is not None
            and any(token in (b"EX", b"PX") for token in command[2:])
        )
        if relative_changed:
            self.deadline_slack[command[1]] = elapsed
        elif name in (b"EXPIREAT", b"PEXPIREAT") and actual == Integer(1):
            self.deadline_slack[command[1]] = 0
        if expected is not None:
            assert_expected(actual, expected, command)
            assert_expected(reference, expected, command)
        if isinstance(expected, TTLRange):
            assert isinstance(actual, Integer) and isinstance(reference, Integer)
            budget = elapsed + self.deadline_slack.get(command[1], 0) + 2
            tolerance = budget if command[0] == b"PTTL" else (budget + 999) // 1000 + 1
            assert abs(actual.payload - reference.payload) <= tolerance, command
        else:
            assert normalize(actual, command) == normalize(reference, command), command
        return actual
