from __future__ import annotations

import asyncio

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server


@pytest.mark.asyncio
@pytest.mark.integration
async def test_redis_style_ping_echo_quit() -> None:
    server = await start_server(ServerConfig(port=0))
    try:
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(b"*1\r\n$4\r\nPING\r\n")
            writer.write(b"*2\r\n$4\r\nECHO\r\n$4\r\nabc\x00\r\n")
            writer.write(b"*1\r\n$4\r\nQUIT\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.readexactly(7), 2) == b"+PONG\r\n"
            assert await asyncio.wait_for(reader.readexactly(10), 2) == b"$4\r\nabc\x00\r\n"
            assert await asyncio.wait_for(reader.readexactly(5), 2) == b"+OK\r\n"
            assert await asyncio.wait_for(reader.read(), 2) == b""
        finally:
            writer.close()
            await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_fragmented_input_and_unsupported_command() -> None:
    server = await start_server(ServerConfig(port=0))
    try:
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            for byte in b"*1\r\n$4\r\nPING\r\n":
                writer.write(bytes([byte]))
                await writer.drain()
            assert await asyncio.wait_for(reader.readexactly(7), 2) == b"+PONG\r\n"
            writer.write(b"*1\r\n$9\r\nSUBSCRIBE\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.readline(), 2) == (
                b"-ERR wrong number of arguments for 'subscribe' command\r\n")
            writer.write(b"*1\r\n$10\r\nPSUBSCRIBE\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.readline(), 2) == (
                b"-ERR wrong number of arguments for 'psubscribe' command\r\n")
            writer.write(b"*1\r\n$10\r\nPSUBSCRIBX\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.readline(), 2) == b"-ERR unknown command\r\n"
        finally:
            writer.close()
            await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
