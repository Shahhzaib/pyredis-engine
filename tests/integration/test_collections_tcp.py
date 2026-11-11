from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
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
from pyredis_engine.server import ManagedServer, start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def frame(*args: bytes) -> bytes:
    return serialize_reply(Array(tuple(Bulk(arg) for arg in args)))


@asynccontextmanager
async def database(config: ServerConfig | None = None) -> AsyncIterator[tuple[ManagedServer, int]]:
    server = await start_server(config or ServerConfig(port=0), clock=ManualClock(1000))
    async with server:
        yield server, server.sockets[0].getsockname()[1]


@asynccontextmanager
async def client(port: int) -> AsyncIterator[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        yield reader, writer
    finally:
        writer.close()
        await writer.wait_closed()


async def response(reader: asyncio.StreamReader) -> Reply:
    # Client reply arrays may exceed the server's inbound command-argument ceiling.
    reply = await asyncio.wait_for(read_value(reader, ServerConfig(max_args=10_000)), 5)
    assert reply is not None
    return reply


async def request(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *args: bytes,
) -> Reply:
    writer.write(frame(*args))
    await writer.drain()
    return await response(reader)


async def test_binary_hash_list_pipeline_order_and_errors() -> None:
    commands = [
        (b"HSET", b"\x00\xff", b"", b"\r\n\xff"), (b"HGET", b"\x00\xff", b""),
        (b"LPUSH", b"l", b"\x00", b"\xff"), (b"LRANGE", b"l", b"0", b"-1"),
        (b"GET", b"l"), (b"HSET", b"\x00\xff", b"missing"),
        (b"HLEN", b"\x00\xff"), (b"RPOP", b"l", b"2"), (b"TYPE", b"l"), (b"QUIT",),
    ]
    expected = [Integer(1), Bulk(b"\r\n\xff"), Integer(2), Array((Bulk(b"\xff"), Bulk(b"\x00"))),
                Error(b"WRONGTYPE Operation against a key holding the wrong kind of value"),
                Error(b"ERR wrong number of arguments for 'hset' command"), Integer(1),
                Array((Bulk(b"\x00"), Bulk(b"\xff"))), Simple(b"none"), Simple(b"OK")]
    async with database() as (_, port), client(port) as (reader, writer):
        writer.write(b"".join(frame(*command) for command in commands))
        await writer.drain()
        wire = b"".join(serialize_reply(reply) for reply in expected)
        assert await asyncio.wait_for(reader.readexactly(len(wire)), 5) == wire
        assert await asyncio.wait_for(reader.read(), 2) == b""


@pytest.mark.parametrize("command", [
    (b"HSET", b"key", b"\x00\xff", b"\r\n", b"", b""),
    (b"RPUSH", b"key", b"\x00\r\n\xff", b""),
])
async def test_every_fragment_boundary_is_atomic(command: tuple[bytes, ...]) -> None:
    wire = frame(*command)
    async with database() as (_, port), client(port) as (observer, observer_writer):
        for split in range(1, len(wire)):
            assert await request(observer, observer_writer, b"FLUSHDB") == Simple(b"OK")
            async with client(port) as (reader, writer):
                writer.write(wire[:split])
                await writer.drain()
                assert await request(observer, observer_writer, b"DBSIZE") == Integer(0)
                writer.write(wire[split:])
                await writer.drain()
                assert await response(reader) == Integer(2)
                assert await request(observer, observer_writer, b"DBSIZE") == Integer(1)


async def test_truncation_and_oversized_frames_preserve_existing_collections() -> None:
    async with database(ServerConfig(port=0, max_bulk_bytes=16)) as (_, port):
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"HSET", b"h", b"a", b"old") == Integer(1)
        for malformed in [frame(b"HSET", b"h", b"a", b"new", b"b", b"new")[:-3],
                          b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$17\r\n"]:
            async with client(port) as (reader, writer):
                writer.write(malformed)
                await writer.drain()
                writer.write_eof()
                assert await asyncio.wait_for(reader.read(), 2) == b"-ERR Protocol error\r\n"
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"HGETALL", b"h") == Array(
                (Bulk(b"a"), Bulk(b"old")))


async def test_100_clients_push_and_set_fields_without_lost_updates() -> None:
    clients, items = 100, 20
    barrier = asyncio.Barrier(clients)
    async with database() as (_, port):
        async def worker(number: int) -> int:
            payloads = [f"{number}:{index}".encode() for index in range(items)]
            fields = [part for payload in payloads for part in (payload, payload)]
            async with client(port) as (reader, writer):
                await asyncio.wait_for(barrier.wait(), 10)
                writer.write(frame(b"HSET", b"h", *fields) + frame(b"RPUSH", b"l", *payloads))
                await writer.drain()
                assert await response(reader) == Integer(items)
                length = await response(reader)
                assert isinstance(length, Integer)
                return length.payload

        lengths = await asyncio.wait_for(asyncio.gather(*(worker(n) for n in range(clients))), 20)
        assert sorted(lengths) == list(range(items, clients * items + 1, items))
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"HLEN", b"h") == Integer(2000)
            listing = await request(reader, writer, b"LRANGE", b"l", b"0", b"-1")
            assert isinstance(listing, Array) and listing.payload is not None
            expected = {Bulk(f"{number}:{index}".encode())
                        for number in range(clients) for index in range(items)}
            assert len(listing.payload) == 2000 and set(listing.payload) == expected
            hashed = await request(reader, writer, b"HGETALL", b"h")
            assert isinstance(hashed, Array) and hashed.payload is not None
            assert len(hashed.payload) == 4000
            assert set(hashed.payload[::2]) == expected
            assert hashed.payload[::2] == hashed.payload[1::2]
            for offset in range(0, 2000, items):
                block = listing.payload[offset:offset + items]
                first = block[0]
                assert isinstance(first, Bulk) and first.payload is not None
                number = first.payload.split(b":")[0]
                assert block == tuple(Bulk(number + b":" + str(i).encode()) for i in range(items))


async def test_100_concurrent_pops_remove_each_element_once() -> None:
    barrier = asyncio.Barrier(100)
    async with database() as (server, port):
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"RPUSH", b"l",
                                 *(str(n).encode() for n in range(100))) == Integer(100)
            assert await request(reader, writer, b"PEXPIRE", b"l", b"10") == Integer(1)

        async def pop() -> Reply:
            async with client(port) as (reader, writer):
                await asyncio.wait_for(barrier.wait(), 10)
                return await request(reader, writer, b"LPOP", b"l")

        replies = await asyncio.wait_for(asyncio.gather(*(pop() for _ in range(100))), 20)
        assert set(replies) == {Bulk(str(n).encode()) for n in range(100)}
        assert server.executor.store.physical_count == server.executor.store.expiration_count == 0


async def test_collection_expiry_and_reply_limit_recovery() -> None:
    async with database(ServerConfig(port=0, max_reply_bytes=128)) as (server, port):
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"RPUSH", b"l", b"x" * 128) == Integer(1)
            assert await request(reader, writer, b"PEXPIRE", b"l", b"10") == Integer(1)
            assert await request(reader, writer, b"LPOP", b"l") == Error(
                b"ERR reply exceeds configured maximum size")
            assert await request(reader, writer, b"LLEN", b"l") == Integer(1)
            assert await request(reader, writer, b"PTTL", b"l") == Integer(10)
            clock = server.executor.store.clock
            assert isinstance(clock, ManualClock)
            clock.advance(11)
            assert await request(reader, writer, b"HSET", b"l", b"f", b"new") == Integer(1)
            assert await request(reader, writer, b"TYPE", b"l") == Simple(b"hash")
            assert await request(reader, writer, b"PTTL", b"l") == Integer(-1)


async def test_concurrent_hash_pairs_are_not_partially_observed() -> None:
    barrier = asyncio.Barrier(3)
    async with database() as (_, port):
        async def change(value: bytes) -> None:
            async with client(port) as (reader, writer):
                await barrier.wait()
                for _ in range(30):
                    assert isinstance(await request(reader, writer, b"HSET", b"h",
                                                    b"a", value, b"b", value), Integer)

        async def observe() -> None:
            async with client(port) as (reader, writer):
                await barrier.wait()
                for _ in range(60):
                    reply = await request(reader, writer, b"HGETALL", b"h")
                    assert isinstance(reply, Array) and reply.payload is not None
                    if reply.payload:
                        assert len(reply.payload) == 4 and reply.payload[1] == reply.payload[3]

        await asyncio.wait_for(asyncio.gather(change(b"1"), change(b"2"), observe()), 10)
