from __future__ import annotations

import asyncio

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Integer, Reply, Simple, read_value, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def frame(*args: bytes) -> bytes:
    return serialize_reply(Array(tuple(Bulk(arg) for arg in args)))


async def request(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *args: bytes,
) -> Reply:
    writer.write(frame(*args))
    await writer.drain()
    result = await asyncio.wait_for(read_value(reader, ServerConfig()), 5)
    assert result is not None
    return result


async def test_active_expiration_without_any_key_access_and_worker_cleanup() -> None:
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
    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
    try:
        for n in range(10):
            assert await request(reader, writer, b"SET", str(n).encode(), b"v", b"PX", b"1") == (
                Simple(b"OK"))
        clock.advance(2)
        assert server.executor.store.physical_count == 10
        for expected in [7, 4, 1, 0]:
            completed.clear()
            wake.set()
            await asyncio.wait_for(completed.wait(), 2)
            # Inspect physical state; a GET/DBSIZE must not hide a broken worker.
            assert server.executor.store.physical_count == expected
        assert server.executor.store.expiration_count == server.executor.store.revision_count == 0
    finally:
        server.close()
        await server.wait_closed()
        writer.close()
        await writer.wait_closed()
    assert server.expiration_task.done()
    assert server.client_task_count == 0
    # Closing again is safe and cannot restart a worker.
    server.close()
    await server.wait_closed()


async def test_fragmented_expiration_pipeline_and_clock_jump() -> None:
    clock = ManualClock(1000)
    server = await start_server(ServerConfig(port=0), clock=clock)
    async with server:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1])
        try:
            raw = frame(b"SET", b"\x00\xff", b"\r\n\x00", b"PX", b"10")
            for byte in raw:
                writer.write(bytes([byte]))
                await writer.drain()
            assert await asyncio.wait_for(reader.readline(), 2) == b"+OK\r\n"
            assert await request(reader, writer, b"PTTL", b"\x00\xff") == Integer(10)
            clock.advance(11)
            writer.write(frame(b"GET", b"\x00\xff") + frame(b"TTL", b"\x00\xff")
                         + frame(b"INCR", b"\x00\xff") + frame(b"PTTL", b"\x00\xff"))
            await writer.drain()
            expected = b"$-1\r\n:-2\r\n:1\r\n:-1\r\n"
            assert await asyncio.wait_for(reader.readexactly(len(expected)), 2) == expected
        finally:
            writer.close()
            await writer.wait_closed()


async def test_100_clients_conditionally_create_expired_key_once() -> None:
    clock = ManualClock(1000)
    server = await start_server(ServerConfig(port=0), clock=clock)
    async with server:
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        assert await request(reader, writer, b"SET", b"k", b"old", b"PX", b"1") == Simple(b"OK")
        clock.advance(2)
        barrier = asyncio.Barrier(100)

        async def contender(number: int) -> Reply:
            connection, output = await asyncio.open_connection("127.0.0.1", port)
            try:
                await asyncio.wait_for(barrier.wait(), 10)
                return await request(connection, output, b"SET", b"k", str(number).encode(),
                                     b"NX", b"PX", b"10")
            finally:
                output.close()
                await output.wait_closed()

        try:
            replies = await asyncio.wait_for(
                asyncio.gather(*(contender(n) for n in range(100))), 20)
            assert replies.count(Simple(b"OK")) == 1
            assert replies.count(Bulk(None)) == 99
            assert await request(reader, writer, b"PTTL", b"k") == Integer(10)
            assert server.executor.store.expiration_count == 1
        finally:
            writer.close()
            await writer.wait_closed()


async def test_shutdown_joins_idle_connections_and_serve_forever_cancellation() -> None:
    server = await start_server(ServerConfig(port=0))
    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
    assert await request(reader, writer, b"PING") == Simple(b"PONG")
    serving = asyncio.create_task(server.serve_forever())
    await asyncio.sleep(0)
    serving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await serving
    await server.wait_closed()
    assert await asyncio.wait_for(reader.read(), 2) == b""
    assert server.expiration_task.done()
    assert server.client_task_count == 0
    writer.close()
    await writer.wait_closed()


async def test_expiration_worker_failure_is_reported_and_closes_server() -> None:
    wake = asyncio.Event()

    async def controlled_wait(interval: float) -> None:
        await wake.wait()

    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    reported = asyncio.Event()
    messages: list[str] = []

    def record_error(current: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        messages.append(str(context["message"]))
        reported.set()

    loop.set_exception_handler(record_error)
    server = await start_server(ServerConfig(port=0), expiry_wait=controlled_wait)

    def broken_batch() -> int:
        raise RuntimeError("injected worker failure")

    server.executor.expire_batch = broken_batch  # type: ignore[method-assign]
    try:
        wake.set()
        await asyncio.wait_for(reported.wait(), 2)
        assert not server.is_serving()
        with pytest.raises(RuntimeError, match="injected worker failure"):
            await server.wait_closed()
        assert messages == ["expiration worker failed; closing server"]
        assert server.expiration_task.done()
    finally:
        server.close()
        loop.set_exception_handler(previous)
