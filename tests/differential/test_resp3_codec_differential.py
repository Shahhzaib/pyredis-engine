"""Compare isolated encoder values with official Redis wire replies.

The live-server check covers only the selected command subset.
"""
from __future__ import annotations

import asyncio

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, serialize_reply
from pyredis_engine.resp3 import Double, Integer, Map, Null, Push, Resp3Value, SetReply, encode
from pyredis_engine.server import start_server

pytestmark = pytest.mark.differential


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    header = await asyncio.wait_for(reader.readline(), timeout=2)
    if not header.endswith(b"\r\n"):
        raise AssertionError("truncated reference response")
    if header[:1] == b"$":
        return header + await asyncio.wait_for(reader.readexactly(int(header[1:-2]) + 2), 2)
    if header[:1] in (b"%", b"~", b">", b"*"):
        count = int(header[1:-2]) * (2 if header[:1] == b"%" else 1)
        for _ in range(count):
            header += await read_frame(reader)
    return header


@pytest.mark.asyncio
async def test_resp3_wire_types_against_redis(reference_port: int) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", reference_port)

    async def command(*args: bytes) -> bytes:
        writer.write(serialize_reply(Array(tuple(Bulk(arg) for arg in args))))
        await writer.drain()
        return await read_frame(reader)

    def wire(value: Resp3Value) -> bytes:
        return encode(value, max_bytes=1024, max_depth=8)

    try:
        assert (await command(b"HELLO", b"3")).startswith(b"%")
        assert await command(b"GET", b"absent") == wire(Null())
        assert await command(b"HSET", b"h", b"\xff", b"\x00") == b":1\r\n"
        assert await command(b"HGETALL", b"h") == wire(Map(((Bulk(b"\xff"), Bulk(b"\x00")),)))
        assert await command(b"SADD", b"s", b"\xff") == b":1\r\n"
        assert await command(b"SMEMBERS", b"s") == wire(SetReply((Bulk(b"\xff"),)))
        assert await command(b"ZADD", b"z", b"1.25", b"x") == b":1\r\n"
        assert await command(b"ZSCORE", b"z", b"x") == wire(Double(1.25))
        assert await command(b"SUBSCRIBE", b"c") == wire(
            Push((Bulk(b"subscribe"), Bulk(b"c"), Integer(1))))
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_resp3_server_wire_against_redis(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        ports = (reference_port, server.sockets[0].getsockname()[1])
        peers = [await asyncio.open_connection('127.0.0.1', port) for port in ports]
        try:
            async def command(*args: bytes) -> tuple[bytes, bytes]:
                request = serialize_reply(Array(tuple(Bulk(arg) for arg in args)))
                for _, writer in peers:
                    writer.write(request)
                    await writer.drain()
                return tuple([await read_frame(reader) for reader, _ in peers])  # type: ignore[return-value]

            hello = await command(b'HELLO', b'3')
            assert all(value.startswith(b'%') for value in hello)
            assert await command(b'FLUSHDB') == (b'+OK\r\n', b'+OK\r\n')
            for args in (
                (b'SET', b'k', b'\xff'), (b'GET', b'k'), (b'GET', b'missing'),
                (b'HSET', b'h', b'f', b'v'), (b'HGETALL', b'h'),
                (b'SADD', b's', b'm'), (b'SMEMBERS', b's'), (b'SISMEMBER', b's', b'm'),
                (b'SMISMEMBER', b's', b'm', b'x'), (b'ZADD', b'z', b'1.25', b'a'),
                (b'ZSCORE', b'z', b'a'), (b'ZMSCORE', b'z', b'a', b'none'),
                (b'ZRANGE', b'z', b'0', b'-1', b'WITHSCORES'),
                (b'XADD', b'stream', b'1-0', b'f', b'v'),
                (b'XREAD', b'STREAMS', b'stream', b'0'),
                (b'XGROUP', b'CREATE', b'stream', b'g', b'0'),
                (b'XREADGROUP', b'GROUP', b'g', b'c', b'STREAMS', b'stream', b'>'),
                (b'SUBSCRIBE', b'channel'), (b'PSUBSCRIBE', b'chan*'),
                (b'GET', b'k'), (b'PING',),
            ):
                actual = await command(*args)
                assert actual[0] == actual[1], (args[0], actual)
        finally:
            for _, writer in peers:
                writer.close()
                await writer.wait_closed()
