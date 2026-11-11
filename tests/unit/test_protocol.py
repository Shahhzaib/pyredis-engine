from __future__ import annotations

import asyncio

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import (
    Bulk,
    Error,
    ProtocolError,
    Simple,
    read_command,
    serialize_reply,
)


@pytest.mark.asyncio
async def test_command_framing_retains_raw_bytes() -> None:
    raw = b"*2\r\n$4\r\nECHO\r\n$7\r\n\x00\r\n\xffabc\r\n"
    reader = asyncio.StreamReader(limit=8192)
    for byte in raw:
        reader.feed_data(bytes([byte]))
    reader.feed_eof()
    assert await read_command(reader, ServerConfig()) == [b"ECHO", b"\x00\r\n\xffabc"]
    assert await read_command(reader, ServerConfig()) is None


@pytest.mark.asyncio
async def test_pipelined_frames() -> None:
    reader = asyncio.StreamReader(limit=8192)
    reader.feed_data(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
    reader.feed_eof()
    assert await read_command(reader, ServerConfig()) == [b"PING"]
    assert await read_command(reader, ServerConfig()) == [b"PING"]


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    b"*1\r\n$-1\r\n", b"*0\r\n", b"*1\r\n$3\r\nabc\r",
    b"*1\n", b"*999999999999\r\n", b"x\r\n",
    b"*1\r\n$1\r\nAxx", b"*1\r\n$1\r\na",
])
async def test_invalid_frames_fail_closed(raw: bytes) -> None:
    reader = asyncio.StreamReader(limit=8192)
    reader.feed_data(raw)
    reader.feed_eof()
    with pytest.raises(ProtocolError):
        await read_command(reader, ServerConfig())


def test_reply_serialization() -> None:
    assert serialize_reply(Simple(b"PONG")) == b"+PONG\r\n"
    assert serialize_reply(Error(b"ERR bad")) == b"-ERR bad\r\n"
    assert serialize_reply(Bulk(None)) == b"$-1\r\n"
    assert serialize_reply(Bulk(b"\x00\r\n")) == b"$3\r\n\x00\r\n\r\n"
