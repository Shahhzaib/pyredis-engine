from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer
from _support.transaction_cases import OK, QUEUED

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_128_transactions_have_no_interleaved_increment_pairs() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        barrier = asyncio.Barrier(128)

        async def worker() -> list[int]:
            async with peer(port) as connection:
                await asyncio.wait_for(barrier.wait(), 10)
                observed = []
                for _ in range(10):
                    replies = await connection.pipeline([
                        (b"MULTI",), (b"INCR", b"count"), (b"INCR", b"count"), (b"EXEC",)])
                    assert replies[:3] == [OK, QUEUED, QUEUED]
                    batch = replies[3]
                    assert isinstance(batch, Array) and batch.payload is not None
                    first, last = batch.payload
                    assert isinstance(first, Integer) and isinstance(last, Integer)
                    assert last.payload == first.payload + 1
                    observed.extend([first.payload, last.payload])
                return observed

        results = await asyncio.wait_for(asyncio.gather(*(worker() for _ in range(128))), 30)
        assert sorted(value for result in results for value in result) == list(range(1, 2561))
        async with peer(port) as observer:
            assert await observer.call(b"GET", b"count") == Bulk(b"2560")


async def test_128_watchers_have_exactly_one_winning_exec() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        barrier = asyncio.Barrier(128)

        async def worker() -> Array:
            async with peer(port) as connection:
                assert await connection.call(b"WATCH", b"k") == OK
                assert await connection.call(b"MULTI") == OK
                assert await connection.call(b"INCR", b"k") == QUEUED
                await asyncio.wait_for(barrier.wait(), 10)
                reply = await connection.call(b"EXEC")
                assert isinstance(reply, Array)
                return reply

        results = await asyncio.wait_for(asyncio.gather(*(worker() for _ in range(128))), 20)
        assert results.count(Array((Integer(1),))) == 1
        assert results.count(Array(None)) == 127
        assert server.executor.watched_key_count == 0


async def test_disconnect_and_shutdown_release_queues_and_watchers() -> None:
    server = await start_server(ServerConfig(port=0))
    port = server.sockets[0].getsockname()[1]
    async with server:
        async with peer(port) as connection:
            assert await connection.call(b"WATCH", b"\x00\xff") == OK
            assert await connection.call(b"MULTI") == OK
            assert await connection.call(b"SET", b"k", b"uncommitted") == QUEUED
        async with asyncio.timeout(5):
            while server.executor.watched_key_count:
                await asyncio.sleep(0)
        async with peer(port) as observer:
            assert await observer.call(b"GET", b"k") == Bulk(None)
            assert await observer.call(b"WATCH", b"pending") == OK
            server.close()
    assert server.executor.watched_key_count == server.client_task_count == 0
    assert server.expiration_task.done()


async def test_fragmented_exec_and_malformed_frame_do_not_execute_queued_writes() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        wire = serialize_reply(Array((Bulk(b"EXEC"),)))
        async with peer(port) as observer:
            for split in range(1, len(wire)):
                async with peer(port) as connection:
                    assert await connection.call(b"MULTI") == OK
                    assert await connection.call(b"SET", b"k", b"\x00\r\n\xff") == QUEUED
                    connection.writer.write(wire[:split])
                    await connection.writer.drain()
                    assert await observer.call(b"GET", b"k") == Bulk(None)
                    connection.writer.write(wire[split:])
                    await connection.writer.drain()
                    assert await connection.reader.readexactly(9) == b"*1\r\n+OK\r\n"
                    assert await observer.call(b"DEL", b"k") == Integer(1)
            async with peer(port) as connection:
                await connection.call(b"MULTI")
                await connection.call(b"SET", b"k", b"uncommitted")
                connection.writer.write(b"*1\r\n$4\r\nEX")
                await connection.writer.drain()
                connection.writer.write_eof()
                assert await connection.reader.read() == b"-ERR Protocol error\r\n"
            assert await observer.call(b"GET", b"k") == Bulk(None)
