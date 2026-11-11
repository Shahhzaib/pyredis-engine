from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import (
    Array,
    Bulk,
    Error,
    Integer,
    Reply,
    Simple,
    read_value,
    serialize_reply,
)
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def frame(*args: bytes) -> bytes:
    return serialize_reply(Array(tuple(Bulk(arg) for arg in args)))


@asynccontextmanager
async def database(config: ServerConfig | None = None) -> AsyncIterator[int]:
    server = await start_server(config or ServerConfig(port=0))
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


@asynccontextmanager
async def connection(port: int) -> AsyncIterator[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        yield reader, writer
    finally:
        writer.close()
        await writer.wait_closed()


async def response(reader: asyncio.StreamReader) -> Reply:
    reply = await asyncio.wait_for(read_value(reader, ServerConfig()), 10)
    assert reply is not None
    return reply


async def request(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *args: bytes,
) -> Reply:
    writer.write(frame(*args))
    await writer.drain()
    return await response(reader)


async def test_mutating_pipeline_binary_errors_and_order() -> None:
    commands = [
        (b"SET", b"\xff\x00", b"\r\n\x00\xff"), (b"GET", b"\xff\x00"),
        (b"INCR", b"counter"), (b"INCRBY", b"counter", b"4"),
        (b"MSET", b"a", b"1", b"b", b"2"), (b"MGET", b"b", b"missing", b"a"),
        (b"MSET", b"a", b"changed", b"dangling"), (b"GET", b"a"),
        (b"INCRBY", b"counter", b"invalid"), (b"GET", b"counter"), (b"QUIT",),
    ]
    expected = [Simple(b"OK"), Bulk(b"\r\n\x00\xff"), Integer(1), Integer(5), Simple(b"OK"),
                Array((Bulk(b"2"), Bulk(None), Bulk(b"1"))),
                Error(b"ERR wrong number of arguments for 'mset' command"), Bulk(b"1"),
                Error(b"ERR value is not an integer or out of range"), Bulk(b"5"), Simple(b"OK")]
    async with database() as port, connection(port) as (reader, writer):
        writer.write(b"".join(frame(*args) for args in commands))
        await writer.drain()
        wire = b"".join(serialize_reply(reply) for reply in expected)
        assert await asyncio.wait_for(reader.readexactly(len(wire)), 5) == wire
        assert await asyncio.wait_for(reader.read(), 2) == b""


async def test_fragmented_write_has_no_partial_effect() -> None:
    wire = frame(b"MSET", b"", b"\x00\r\n\xff", b"other", b"value")
    async with database() as port, connection(port) as (observer, observer_writer):
        for split in range(1, len(wire)):
            assert await request(observer, observer_writer, b"FLUSHDB") == Simple(b"OK")
            async with connection(port) as (reader, writer):
                writer.write(wire[:split])
                await writer.drain()
                assert await request(observer, observer_writer, b"DBSIZE") == Integer(0)
                writer.write(wire[split:] + frame(b"MGET", b"", b"other"))
                await writer.drain()
                assert await response(reader) == Simple(b"OK")
                assert await response(reader) == Array((Bulk(b"\x00\r\n\xff"), Bulk(b"value")))


async def test_100_clients_increment_one_key_exactly() -> None:
    clients, increments = 100, 20
    barrier = asyncio.Barrier(clients)
    async with database() as port:
        async def worker() -> list[int]:
            async with connection(port) as (reader, writer):
                await asyncio.wait_for(barrier.wait(), 10)
                writer.write(frame(b"INCR", b"shared") * increments + frame(b"QUIT"))
                await writer.drain()
                values: list[int] = []
                for _ in range(increments):
                    reply = await response(reader)
                    assert isinstance(reply, Integer)
                    values.append(reply.payload)
                assert values == sorted(values)
                assert await response(reader) == Simple(b"OK")
                assert await asyncio.wait_for(reader.read(), 2) == b""
                return values

        results = await asyncio.wait_for(asyncio.gather(*(worker() for _ in range(clients))), 20)
        assert sorted(value for values in results for value in values) == list(
            range(1, clients * increments + 1))
        async with connection(port) as (reader, writer):
            assert await request(reader, writer, b"GET", b"shared") == Bulk(b"2000")


async def test_mset_is_never_partially_observed() -> None:
    barrier = asyncio.Barrier(3)
    async with database() as port:
        async def write_pairs(value: bytes) -> None:
            async with connection(port) as (reader, writer):
                await barrier.wait()
                for _ in range(50):
                    assert await request(reader, writer, b"MSET", b"a", value, b"b", value) == (
                        Simple(b"OK"))

        async def observe() -> None:
            async with connection(port) as (reader, writer):
                await barrier.wait()
                for _ in range(100):
                    reply = await request(reader, writer, b"MGET", b"a", b"b")
                    assert isinstance(reply, Array) and reply.payload is not None
                    assert reply.payload[0] == reply.payload[1]

        await asyncio.wait_for(asyncio.gather(write_pairs(b"1"), write_pairs(b"2"), observe()), 10)


async def test_limits_and_malformed_client_preserve_database() -> None:
    config = ServerConfig(port=0, max_bulk_bytes=64, max_reply_bytes=128)
    async with database(config) as port:
        async with connection(port) as (reader, writer):
            assert await request(reader, writer, b"SET", b"big", b"x" * 64) == Simple(b"OK")
            assert await request(reader, writer, b"MGET", b"big", b"big") == Error(
                b"ERR reply exceeds configured maximum size")
            assert await request(reader, writer, b"APPEND", b"big", b"x") == Error(
                b"ERR string exceeds configured maximum size")
            assert await request(reader, writer, b"GET", b"big") == Bulk(b"x" * 64)
        async with connection(port) as (reader, writer):
            writer.write(b"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n$65\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == b"-ERR Protocol error\r\n"
        async with connection(port) as (reader, writer):
            assert await request(reader, writer, b"GET", b"big") == Bulk(b"x" * 64)


async def test_instances_are_isolated_and_reconnect_retains_server_state() -> None:
    async with database() as first, database() as second:
        async with connection(first) as (reader, writer):
            assert await request(reader, writer, b"SET", b"key", b"value") == Simple(b"OK")
        async with connection(second) as (reader, writer):
            assert await request(reader, writer, b"GET", b"key") == Bulk(None)
        async with connection(first) as (reader, writer):
            assert await request(reader, writer, b"GET", b"key") == Bulk(b"value")


async def test_truncated_mset_does_not_mutate_existing_values() -> None:
    async with database() as port:
        async with connection(port) as (reader, writer):
            assert await request(reader, writer, b"MSET", b"a", b"old", b"b", b"old") == (
                Simple(b"OK"))
        async with connection(port) as (reader, writer):
            writer.write(frame(b"MSET", b"a", b"new", b"b", b"new")[:-3])
            await writer.drain()
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), 2) == b"-ERR Protocol error\r\n"
        async with connection(port) as (reader, writer):
            assert await request(reader, writer, b"MGET", b"a", b"b") == Array(
                (Bulk(b"old"), Bulk(b"old")))
