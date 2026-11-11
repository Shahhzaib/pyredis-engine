from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from pyredis_engine import server as server_module
from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]
PING = b"*1\r\n$4\r\nPING\r\n"
QUIT = b"*1\r\n$4\r\nQUIT\r\n"
PROTOCOL_ERROR = b"-ERR Protocol error\r\n"


def command(*args: bytes) -> bytes:
    return b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in args)


@asynccontextmanager
async def running(config: ServerConfig | None = None) -> AsyncIterator[int]:
    server = await start_server(config or ServerConfig(port=0))
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


@asynccontextmanager
async def client(port: int) -> AsyncIterator[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        yield reader, writer
    finally:
        writer.close()
        await writer.wait_closed()


async def receive(reader: asyncio.StreamReader, size: int) -> bytes:
    return await asyncio.wait_for(reader.readexactly(size), 2)


async def assert_healthy(port: int) -> None:
    async with client(port) as (reader, writer):
        writer.write(PING + QUIT)
        await writer.drain()
        assert await receive(reader, 12) == b"+PONG\r\n+OK\r\n"
        assert await asyncio.wait_for(reader.read(), 2) == b""


async def test_every_tcp_split_and_mixed_pipeline() -> None:
    payload = b"\x00\r\n\xff\xe2\x98\x83"
    wire = command(b"ECHO", payload)
    expected = b"$%d\r\n" % len(payload) + payload + b"\r\n+PONG\r\n+OK\r\n"
    async with running() as port:
        for split in range(1, len(wire)):
            async with client(port) as (reader, writer):
                writer.write(wire[:split])
                await writer.drain()
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(reader.read(1), 0.01)
                writer.write(wire[split:] + PING + QUIT)
                await writer.drain()
                assert await receive(reader, len(expected)) == expected


async def test_pipeline_order_and_command_errors() -> None:
    requests = [command(b"ECHO", str(n).encode()) for n in range(100)]
    expected = b"".join(b"$%d\r\n" % len(str(n)) + str(n).encode() + b"\r\n"
                        for n in range(100))
    async with running() as port:
        async with client(port) as (reader, writer):
            writer.write(b"".join(requests) + command(b"", b"") + command(b"echo")
                         + command(b"quit", b"x") + command(b"ping", b"1", b"2")
                         + command(b"eChO", b"") + command(b"pInG", b"\xff") + QUIT + PING)
            await writer.drain()
            expected += (b"-ERR unknown command\r\n"
                         b"-ERR wrong number of arguments for 'echo' command\r\n"
                         b"-ERR wrong number of arguments for 'quit' command\r\n"
                         b"-ERR wrong number of arguments for 'ping' command\r\n"
                         b"$0\r\n\r\n$1\r\n\xff\r\n+OK\r\n")
            assert await receive(reader, len(expected)) == expected
            assert await asyncio.wait_for(reader.read(), 2) == b""


@pytest.mark.parametrize("bad", [
    b"?\r\n", b"*1\n", b"*0\r\n", b"*1025\r\n", b"*1\r\n$-2\r\n",
    b"*1\r\n$99999999999999999999\r\n", b"*1\r\n$1\r\nxXX",
    b"*1\r\n$-1\r\n", b"*1\r\n:9223372036854775808\r\n",
    b"*1\r\n*1\r\n*0\r\n", b"*" + b"1" * 40 + b"\r\n",
])
async def test_malformed_closes_only_offender_and_releases_slot(bad: bytes) -> None:
    config = ServerConfig(port=0, max_clients=1, max_depth=2, max_header_bytes=32)
    async with running(config) as port:
        for _ in range(3):
            async with client(port) as (reader, writer):
                writer.write(bad + PING)
                await writer.drain()
                assert await asyncio.wait_for(reader.read(), 2) == PROTOCOL_ERROR
            await assert_healthy(port)


@pytest.mark.parametrize("bad", [
    b"*2\r\n$4\r\nECHO\r\n$33\r\n",
    b"*" + b"9" * 128,
])
async def test_oversized_bulk_and_input_buffer(bad: bytes) -> None:
    config = ServerConfig(port=0, max_bulk_bytes=32, max_frame_bytes=64,
                          max_input_bytes=64, max_header_bytes=32)
    async with running(config) as port:
        async with client(port) as (reader, writer):
            writer.write(bad)
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == PROTOCOL_ERROR
        await assert_healthy(port)


async def test_oversized_frame() -> None:
    config = ServerConfig(port=0, max_bulk_bytes=16, max_frame_bytes=24)
    async with running(config) as port:
        async with client(port) as (reader, writer):
            writer.write(command(b"ECHO", b"x" * 16))
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == PROTOCOL_ERROR
        await assert_healthy(port)


async def test_valid_then_malformed_pipeline_preserves_prior_reply() -> None:
    async with running() as port:
        async with client(port) as (reader, writer):
            writer.write(PING + b"?\r\n" + PING)
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 2) == b"+PONG\r\n" + PROTOCOL_ERROR
        await assert_healthy(port)


@pytest.mark.parametrize("partial", [b"*", b"*2\r\n$4\r\nECHO\r\n", b"*1\r\n$4\r\nPI"])
async def test_eof_mid_frame(partial: bytes) -> None:
    async with running() as port:
        async with client(port) as (reader, writer):
            writer.write(partial)
            await writer.drain()
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), 2) == PROTOCOL_ERROR
        await assert_healthy(port)


async def test_clean_eof_and_disconnect_after_request() -> None:
    async with running() as port:
        async with client(port) as (reader, writer):
            writer.write_eof()
            assert await asyncio.wait_for(reader.read(), 2) == b""
        for _ in range(10):
            _, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(PING * 10)
            writer.transport.abort()
        await assert_healthy(port)


async def test_concurrent_clients_and_max_clients() -> None:
    async with running() as port:
        await asyncio.gather(*(assert_healthy(port) for _ in range(32)))
    async with running(ServerConfig(port=0, max_clients=1)) as port:
        async with client(port) as (reader, writer):
            writer.write(PING)
            await writer.drain()
            assert await receive(reader, 7) == b"+PONG\r\n"
            async with client(port) as (denied, _):
                assert await asyncio.wait_for(denied.read(), 2) == (
                    b"-ERR max number of clients reached\r\n")
            writer.write(QUIT)
            await writer.drain()
            assert await receive(reader, 5) == b"+OK\r\n"
            assert await asyncio.wait_for(reader.read(), 2) == b""
        await assert_healthy(port)


@pytest.mark.parametrize("drip", [False, True])
async def test_idle_and_total_command_deadline(drip: bool) -> None:
    config = ServerConfig(port=0, read_timeout=0.15)
    async with running(config) as port:
        async with client(port) as (reader, writer):
            if drip:
                for piece in [b"*", b"1", b"\r"]:
                    writer.write(piece)
                    await writer.drain()
                    await asyncio.sleep(0.04)
            assert await asyncio.wait_for(reader.read(), 1) == b""
        await assert_healthy(port)


async def test_slow_reader_write_deadline_and_slot_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timed_out = asyncio.Event()
    original_send = server_module._safe_send

    async def observe_send(
        writer: asyncio.StreamWriter, payload: bytes, config: ServerConfig,
    ) -> None:
        try:
            await original_send(writer, payload, config)
        except TimeoutError:
            timed_out.set()
            raise

    monkeypatch.setattr(server_module, "_safe_send", observe_send)
    config = ServerConfig(port=0, max_clients=1, write_timeout=0.1, read_timeout=5)
    async with running(config) as port:
        async with client(port) as (_, writer):
            writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            writer.transport.pause_reading()
            writer.write(command(b"ECHO", b"x" * config.max_bulk_bytes))
            await asyncio.wait_for(writer.drain(), 5)
            await asyncio.wait_for(timed_out.wait(), 5)
            await assert_healthy(port)
