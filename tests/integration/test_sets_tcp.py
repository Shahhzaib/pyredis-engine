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
    reply = await asyncio.wait_for(read_value(reader, ServerConfig(max_args=10_000)), 5)
    assert reply is not None
    return reply


async def request(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *args: bytes,
) -> Reply:
    writer.write(frame(*args))
    await writer.drain()
    return await response(reader)


async def test_binary_pipeline_and_errors_preserve_exact_wire_order() -> None:
    commands = [
        (b"SADD", b"\x00\xff", b"", b"\r\n\xff", b""),
        (b"SMEMBERS", b"\x00\xff"), (b"SCARD", b"\x00\xff"),
        (b"ZADD", b"z", b"1", b"\xff", b"-inf", b"\x00", b"1", b""),
        (b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"),
        (b"ZADD", b"z", b"2", b"", b"nan", b"bad"),
        (b"ZSCORE", b"z", b""), (b"SADD", b"z", b"bad"),
        (b"ZREM", b"z", b"", b"\xff", b"\x00"), (b"TYPE", b"z"), (b"QUIT",),
    ]
    expected = [
        Integer(2), Array((Bulk(b""), Bulk(b"\r\n\xff"))), Integer(2), Integer(3),
        Array(tuple(Bulk(x) for x in (b"\x00", b"-inf", b"", b"1", b"\xff", b"1"))),
        Error(b"ERR value is not a valid float"), Bulk(b"1"),
        Error(b"WRONGTYPE Operation against a key holding the wrong kind of value"),
        Integer(3), Simple(b"none"), Simple(b"OK"),
    ]
    async with database() as (_, port), client(port) as (reader, writer):
        writer.write(b"".join(frame(*command) for command in commands))
        await writer.drain()
        wire = b"".join(serialize_reply(reply) for reply in expected)
        assert await asyncio.wait_for(reader.readexactly(len(wire)), 5) == wire
        assert await asyncio.wait_for(reader.read(), 2) == b""


async def test_compatibility_corrections_preserve_wire_order_and_post_error_state() -> None:
    commands = [
        (b"LPOP", b"missing"), (b"RPOP", b"missing", b"0"),
        (b"RPUSH", b"list", b"\x00\xff"), (b"PEXPIRE", b"list", b"100"),
        (b"LPOP", b"list", b"bad"), (b"LRANGE", b"list", b"0", b"-1"),
        (b"PTTL", b"list"), (b"HSET", b"h", b"f", b"v"),
        (b"GETEX", b"h", b"PX", b"0"), (b"GETEX", b"missing", b"PX", b"bad"),
        (b"ZADD", b"z", b"0x1p2", b"\x00\xff", b"5e-324", b"tiny"),
        (b"ZADD", b"z", b"2", b"\x00\xff", b"0x1p-1075", b"bad"),
        (b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"), (b"QUIT",),
    ]
    expected = [
        Bulk(None), Array(None), Integer(1), Integer(1),
        Error(b"ERR value is out of range, must be positive"),
        Array((Bulk(b"\x00\xff"),)), Integer(100), Integer(1),
        Error(b"WRONGTYPE Operation against a key holding the wrong kind of value"),
        Bulk(None), Integer(2), Error(b"ERR value is not a valid float"),
        Array(tuple(Bulk(part) for part in (b"tiny", b"5e-324", b"\x00\xff", b"4"))),
        Simple(b"OK"),
    ]
    async with database() as (_, port), client(port) as (reader, writer):
        writer.write(b"".join(frame(*command) for command in commands))
        await writer.drain()
        wire = b"".join(serialize_reply(reply) for reply in expected)
        assert await asyncio.wait_for(reader.readexactly(len(wire)), 5) == wire
        assert await asyncio.wait_for(reader.read(), 2) == b""


@pytest.mark.parametrize("command", [
    (b"SADD", b"key", b"\x00\r\n\xff", b""),
    (b"ZADD", b"key", b"-1.5", b"\x00\r\n\xff", b"+inf", b""),
])
async def test_every_fragment_boundary_and_truncation_are_atomic(
    command: tuple[bytes, ...],
) -> None:
    wire = frame(*command)
    async with database() as (_, port), client(port) as (observer, output):
        for split in range(1, len(wire)):
            assert await request(observer, output, b"FLUSHDB") == Simple(b"OK")
            async with client(port) as (reader, writer):
                writer.write(wire[:split])
                await writer.drain()
                assert await request(observer, output, b"DBSIZE") == Integer(0)
                writer.write(wire[split:])
                await writer.drain()
                assert await response(reader) == Integer(2)
        assert await request(observer, output, b"FLUSHDB") == Simple(b"OK")
        async with client(port) as (reader, writer):
            writer.write(wire[:-3])
            await writer.drain()
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), 2) == b"-ERR Protocol error\r\n"
        assert await request(observer, output, b"DBSIZE") == Integer(0)
        assert await request(observer, output, b"PING") == Simple(b"PONG")


async def test_100_clients_add_duplicate_and_unique_members_without_lost_updates() -> None:
    clients, count = 100, 20
    barrier = asyncio.Barrier(clients)
    async with database() as (_, port):
        async def worker(number: int) -> tuple[int, int]:
            members = [f"{number}:{n}".encode() for n in range(count)]
            pairs = [part for member in members for part in (b"1", member)]
            async with client(port) as (reader, writer):
                await asyncio.wait_for(barrier.wait(), 10)
                writer.write(frame(b"SADD", b"s", b"shared", *members, *members)
                             + frame(b"ZADD", b"z", b"1", b"shared", *pairs, *pairs))
                await writer.drain()
                first, second = await response(reader), await response(reader)
                assert isinstance(first, Integer) and isinstance(second, Integer)
                return first.payload, second.payload

        results = await asyncio.wait_for(asyncio.gather(*(worker(n) for n in range(clients))), 20)
        assert sum(result[0] for result in results) == sum(result[1] for result in results) == 2001
        assert sorted(result[0] for result in results) == [20] * 99 + [21]
        assert sorted(result[1] for result in results) == [20] * 99 + [21]
        expected = sorted({b"shared"} | {f"{n}:{i}".encode()
                                        for n in range(clients) for i in range(count)})
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"SCARD", b"s") == Integer(2001)
            assert await request(reader, writer, b"ZCARD", b"z") == Integer(2001)
            listing = Array(tuple(Bulk(member) for member in expected))
            assert await request(reader, writer, b"SMEMBERS", b"s") == listing
            assert await request(reader, writer, b"ZRANGE", b"z", b"0", b"-1") == listing
            scored = await request(reader, writer, b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES")
            assert scored == Array(tuple(part for member in expected
                                         for part in (Bulk(member), Bulk(b"1"))))


async def test_100_clients_remove_same_members_exactly_once() -> None:
    barrier = asyncio.Barrier(100)
    async with database() as (server, port):
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"SADD", b"s", b"a", b"b") == Integer(2)
            assert await request(reader, writer, b"ZADD", b"z", b"1", b"a", b"2", b"b") == (
                Integer(2))
            for key in (b"s", b"z"):
                assert await request(reader, writer, b"PEXPIRE", key, b"100") == Integer(1)

        async def remove() -> tuple[Reply, Reply]:
            async with client(port) as (reader, writer):
                await asyncio.wait_for(barrier.wait(), 10)
                writer.write(frame(b"SREM", b"s", b"a", b"a", b"b")
                             + frame(b"ZREM", b"z", b"a", b"a", b"b"))
                await writer.drain()
                return await response(reader), await response(reader)

        results = await asyncio.wait_for(asyncio.gather(*(remove() for _ in range(100))), 20)
        for column in (0, 1):
            replies = [result[column] for result in results]
            assert replies.count(Integer(2)) == 1
            assert replies.count(Integer(0)) == 99
        assert server.executor.store.physical_count == server.executor.store.expiration_count == 0
        assert server.executor.store.revision_count == 0


async def test_score_pair_updates_are_never_partially_observed() -> None:
    barrier = asyncio.Barrier(3)
    async with database() as (_, port):
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"ZADD", b"z", b"0", b"a", b"0", b"b") == (
                Integer(2))

        async def change(score: bytes) -> None:
            async with client(port) as (reader, writer):
                await barrier.wait()
                for _ in range(30):
                    assert isinstance(await request(reader, writer, b"ZADD", b"z",
                                                    score, b"a", score, b"b"), Integer)

        async def observe() -> None:
            async with client(port) as (reader, writer):
                await barrier.wait()
                for _ in range(60):
                    reply = await request(reader, writer, b"ZRANGE", b"z", b"0", b"-1",
                                          b"WITHSCORES")
                    assert isinstance(reply, Array) and reply.payload is not None
                    assert reply.payload[::2] == (Bulk(b"a"), Bulk(b"b"))
                    assert reply.payload[1] == reply.payload[3]

        await asyncio.wait_for(asyncio.gather(change(b"-1"), change(b"2"), observe()), 10)


async def test_concurrent_expired_keys_recreate_without_inherited_ttl() -> None:
    barrier = asyncio.Barrier(100)
    async with database() as (server, port):
        async with client(port) as (reader, writer):
            for key in (b"s", b"z"):
                assert await request(reader, writer, b"SET", key, b"old", b"PX", b"1") == (
                    Simple(b"OK"))
        clock = server.executor.store.clock
        assert isinstance(clock, ManualClock)
        clock.advance(2)

        async def recreate(number: int) -> None:
            async with client(port) as (reader, writer):
                await asyncio.wait_for(barrier.wait(), 10)
                member = str(number).encode()
                assert await request(reader, writer, b"SADD", b"s", member) == Integer(1)
                assert await request(reader, writer, b"ZADD", b"z", b"1", member) == Integer(1)

        await asyncio.wait_for(asyncio.gather(*(recreate(n) for n in range(100))), 20)
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"SCARD", b"s") == Integer(100)
            assert await request(reader, writer, b"ZCARD", b"z") == Integer(100)
            for key in (b"s", b"z"):
                assert await request(reader, writer, b"PTTL", key) == Integer(-1)
        assert server.executor.store.expiration_count == 0
    assert server.expiration_task.done() and server.client_task_count == 0


async def test_resource_errors_and_malformed_frame_allow_recovery() -> None:
    async with database(ServerConfig(port=0, max_reply_bytes=128,
                                     max_collection_items=2, max_bulk_bytes=128)) as (_, port):
        async with client(port) as (reader, writer):
            for name, key, args in [(b"SADD", b"s", (b"a" * 80, b"b" * 80)),
                                    (b"ZADD", b"z", (b"1", b"a" * 80, b"2", b"b" * 80))]:
                assert await request(reader, writer, name, key, *args) == Integer(2)
                extra = (b"3", b"extra") if name == b"ZADD" else (b"extra",)
                assert await request(reader, writer, name, key, *extra) == Error(
                    b"ERR collection exceeds configured maximum size")
            assert await request(reader, writer, b"SMEMBERS", b"s") == Error(
                b"ERR reply exceeds configured maximum size")
            assert await request(reader, writer, b"ZRANGE", b"z", b"0", b"-1") == Error(
                b"ERR reply exceeds configured maximum size")
            assert await request(reader, writer, b"SCARD", b"s") == Integer(2)
            assert await request(reader, writer, b"ZCARD", b"z") == Integer(2)
        async with client(port) as (reader, writer):
            writer.write(b"*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n$129\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == b"-ERR Protocol error\r\n"
        async with client(port) as (reader, writer):
            assert await request(reader, writer, b"SCARD", b"s") == Integer(2)
            assert await request(reader, writer, b"ZCARD", b"z") == Integer(2)


async def test_active_expiry_without_access_and_shutdown_cleanup() -> None:
    wake, completed = asyncio.Event(), asyncio.Event()

    async def controlled_wait(interval: float) -> None:
        await wake.wait()
        wake.clear()

    clock = ManualClock(1000)
    server = await start_server(ServerConfig(port=0, expiry_batch_size=3), clock=clock,
                                expiry_wait=controlled_wait)
    original = server.executor.expire_batch

    def observed_batch() -> int:
        result = original()
        completed.set()
        return result

    server.executor.expire_batch = observed_batch  # type: ignore[method-assign]
    port = server.sockets[0].getsockname()[1]
    async with server, client(port) as (reader, writer):
        for number in range(10):
            key = str(number).encode()
            create = ((b"SADD", key, b"m") if number % 2
                      else (b"ZADD", key, b"1", b"m"))
            assert await request(reader, writer, *create) == Integer(1)
            assert await request(reader, writer, b"PEXPIRE", key, b"1") == Integer(1)
        clock.advance(2)
        assert server.executor.store.physical_count == 10
        for expected in [7, 4, 1, 0]:
            completed.clear()
            wake.set()
            await asyncio.wait_for(completed.wait(), 2)
            assert server.executor.store.physical_count == expected
        assert server.executor.store.expiration_count == server.executor.store.revision_count == 0
    assert server.expiration_task.done() and server.client_task_count == 0
