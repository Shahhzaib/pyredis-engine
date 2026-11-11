from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer
from _support.transaction_cases import OK, QUEUED, TRACES, WATCH_CASES

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize("name", TRACES)
async def test_transaction_trace(reference_port: int, name: str) -> None:
    server = await start_server(ServerConfig(port=0))
    port = server.sockets[0].getsockname()[1]
    async with server, peer(port) as local, peer(reference_port) as ref:
        assert await ref.call(b"FLUSHDB") == OK
        for command, expected in TRACES[name]:
            assert await local.call(*command) == await ref.call(*command) == expected


@pytest.mark.parametrize("setup,mutation,conflict", WATCH_CASES)
async def test_two_client_watch(
    reference_port: int, setup: tuple[bytes, ...], mutation: tuple[bytes, ...], conflict: bool,
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as observer, peer(port) as writer:
                assert await writer.call(b"FLUSHDB") == OK
                await writer.call(*setup)
                assert await observer.call(b"WATCH", b"k") == OK
                await writer.call(*mutation)
                assert await observer.call(b"MULTI") == OK
                assert await observer.call(b"EXEC") == Array(None if conflict else ())


async def test_watch_expiration_and_cleanup(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as watcher, peer(port) as writer:
                await writer.call(b"FLUSHDB")
                assert await writer.call(b"SET", b"k", b"v", b"PX", b"100") == OK
                assert await watcher.call(b"WATCH", b"k") == OK
                # A bounded state predicate, not a TTL-equals-after-sleep assertion.
                async with asyncio.timeout(5):
                    while await writer.call(b"PTTL", b"k") != Integer(-2):
                        await asyncio.sleep(0.001)
                assert await watcher.call(b"MULTI") == OK
                assert await watcher.call(b"SET", b"other", b"v") == QUEUED
                assert await watcher.call(b"EXEC") == Array(None)
                assert await writer.call(b"GET", b"other") == Bulk(None)
                assert await watcher.call(b"MULTI") == OK
                assert await watcher.call(b"EXEC") == Array(())


async def test_concurrent_exec_isolation_on_both_servers(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as setup:
                assert await setup.call(b"FLUSHDB") == Simple(b"OK")
            barrier = asyncio.Barrier(100)

            async def change(port: int, barrier: asyncio.Barrier) -> int:
                async with peer(port) as connection:
                    await asyncio.wait_for(barrier.wait(), 10)
                    replies = await connection.pipeline([
                        (b"MULTI",), (b"INCR", b"count"), (b"INCR", b"count"), (b"EXEC",)])
                    assert replies[:3] == [OK, QUEUED, QUEUED]
                    result = replies[3]
                    assert isinstance(result, Array) and result.payload is not None
                    first, last = result.payload
                    assert isinstance(first, Integer) and isinstance(last, Integer)
                    assert last.payload == first.payload + 1
                    return first.payload

            results = await asyncio.wait_for(
                asyncio.gather(*(change(port, barrier) for _ in range(100))), 20)
            assert sorted(results) == list(range(1, 200, 2))
            async with peer(port) as observer:
                assert await observer.call(b"GET", b"count") == Bulk(b"200")


async def test_unknown_queue_error_aborts_and_releases_watches(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as connection:
                await connection.call(b"FLUSHDB")
                assert await connection.call(b"WATCH", b"k") == OK
                assert await connection.call(b"MULTI") == OK
                assert await connection.call(b"SET", b"k", b"v") == QUEUED
                error = await connection.call(b"M06_UNKNOWN")
                # Existing documented difference: PyRedis omits Redis's unknown
                # command context. Assert the error class/prefix, then exact abort.
                assert isinstance(error, Error) and error.payload.startswith(b"ERR unknown command")
                assert await connection.call(b"EXEC") == Error(
                    b"EXECABORT Transaction discarded because of previous errors.")
                assert await connection.call(b"GET", b"k") == Bulk(None)
                assert await connection.call(b"MULTI") == OK
                assert await connection.call(b"EXEC") == Array(())


async def test_unchanged_deadline_invalidates_watch(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as connection:
                await connection.call(b"FLUSHDB")
                await connection.call(b"SET", b"k", b"v")
                await connection.call(b"PEXPIREAT", b"k", b"4000000000000")
                assert await connection.call(b"WATCH", b"k") == OK
                assert await connection.call(b"PEXPIREAT", b"k", b"4000000000000") == Integer(1)
                assert await connection.call(b"MULTI") == OK
                assert await connection.call(b"EXEC") == Array(None)
