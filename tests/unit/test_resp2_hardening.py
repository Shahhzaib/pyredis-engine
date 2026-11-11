from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import (
    Array,
    Bulk,
    Error,
    Integer,
    ProtocolError,
    Reply,
    Simple,
    read_command,
    read_value,
    serialize_reply,
)


async def decode(raw: bytes, config: ServerConfig | None = None) -> Reply | None:
    reader = asyncio.StreamReader(limit=32)
    reader.feed_data(raw)
    reader.feed_eof()
    return await read_value(reader, config or ServerConfig())


@pytest.mark.parametrize(("raw", "value"), [
    (b"+OK\r\n", Simple(b"OK")),
    (b"+\r\n", Simple(b"")),
    (b"-ERR bad\r\n", Error(b"ERR bad")),
    (b":0\r\n", Integer(0)),
    (b":-9223372036854775808\r\n", Integer(-(2**63))),
    (b":9223372036854775807\r\n", Integer(2**63 - 1)),
    (b"$0\r\n\r\n", Bulk(b"")),
    (b"$-1\r\n", Bulk(None)),
    (b"$7\r\n\x00\r\n\xffabc\r\n", Bulk(b"\x00\r\n\xffabc")),
    (b"*0\r\n", Array(())),
    (b"*-1\r\n", Array(None)),
    (b"*3\r\n:1\r\n$-1\r\n*1\r\n+hi\r\n",
     Array((Integer(1), Bulk(None), Array((Simple(b"hi"),))))),
])
async def test_all_resp2_types(raw: bytes, value: Reply) -> None:
    assert await decode(raw) == value
    assert serialize_reply(value) == raw


async def test_integer_optional_sign() -> None:
    assert await decode(b":+12\r\n") == Integer(12)
    assert await decode(b":-0\r\n") == Integer(0)
    assert await decode(b"") is None


@pytest.mark.parametrize("raw", [
    b"?\r\n", b"_\r\n", b"+a\rb\r\n", b"+a\nb\r\n", b"+OK\n",
    b":\r\n", b":+\r\n", b": 1\r\n", b":1_0\r\n", b":\xff\r\n",
    b":9223372036854775808\r\n", b":-9223372036854775809\r\n",
    b"$-2\r\n", b"$+1\r\n", b"$ 1\r\n", b"$1_0\r\n", b"$\r\n",
    b"$999999999999999999999999999999\r\n", b"$1\r\naXX",
    b"*-2\r\n", b"*+1\r\n", b"*\r\n", b"*1025\r\n", b"*\xff\r\n",
    b"+" + b"x" * 64 + b"\r\n",
])
async def test_malformed_values(raw: bytes) -> None:
    with pytest.raises(ProtocolError):
        await decode(raw)


async def test_every_truncated_prefix_fails() -> None:
    raw = b"*2\r\n$4\r\nECHO\r\n$5\r\na\x00\r\nb\r\n"
    for end in range(1, len(raw)):
        with pytest.raises(ProtocolError):
            await decode(raw[:end])


async def test_every_fragment_boundary_waits_for_completion() -> None:
    raw = b"*2\r\n$4\r\nECHO\r\n$5\r\na\x00\r\nb\r\n"
    for split in range(1, len(raw)):
        reader = asyncio.StreamReader(limit=32)
        pending = asyncio.create_task(read_command(reader, ServerConfig()))
        reader.feed_data(raw[:split])
        await asyncio.sleep(0)
        assert not pending.done()
        reader.feed_data(raw[split:])
        assert await asyncio.wait_for(pending, 1) == [b"ECHO", b"a\x00\r\nb"]


async def test_bulk_frame_array_and_depth_limits() -> None:
    config = ServerConfig(max_bulk_bytes=8, max_frame_bytes=31, max_args=2, max_depth=2)
    assert await decode(b"$8\r\n12345678\r\n", config) == Bulk(b"12345678")
    assert await decode(b"*1\r\n*0\r\n", config) == Array((Array(()),))
    for raw in [b"$9\r\n", b"*3\r\n", b"*1\r\n*1\r\n*0\r\n",
                b"*2\r\n$8\r\n12345678\r\n$8\r\n12345678\r\n"]:
        with pytest.raises(ProtocolError):
            await decode(raw, config)
    exact = b"*2\r\n$8\r\n12345678\r\n$8\r\n12345678\r\n"
    assert len(exact) == 32
    assert await decode(exact, replace(config, max_frame_bytes=32)) == Array(
        (Bulk(b"12345678"), Bulk(b"12345678")))


async def test_frame_budget_counts_headers_and_empty_arrays() -> None:
    config = ServerConfig(max_bulk_bytes=1, max_frame_bytes=16)
    with pytest.raises(ProtocolError):
        await decode(b"*4\r\n*0\r\n*0\r\n*0\r\n*0\r\n", config)
    with pytest.raises(ProtocolError):
        await decode(b"+" + b"x" * 14 + b"\r\n", config)


async def test_exact_array_header_and_recursion_boundaries() -> None:
    config = ServerConfig(max_args=2, max_header_bytes=32, max_depth=128)
    assert await decode(b"*2\r\n+\r\n+\r\n", config) == Array((Simple(b""), Simple(b"")))
    assert await decode(b"+" + b"x" * 29 + b"\r\n", config) == Simple(b"x" * 29)
    with pytest.raises(ProtocolError):
        await decode(b"+" + b"x" * 30 + b"\r\n", config)
    assert isinstance(await decode(b"*1\r\n" * 127 + b"*0\r\n", config), Array)
    with pytest.raises(ProtocolError):
        await decode(b"*1\r\n" * 128 + b"*0\r\n", config)


@pytest.mark.parametrize("raw", [b"+PING\r\n", b"*-1\r\n", b"*0\r\n",
                                 b"*1\r\n:1\r\n", b"*1\r\n*0\r\n",
                                 b"*1\r\n$-1\r\n"])
async def test_commands_remain_nonempty_bulk_arrays(raw: bytes) -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(raw)
    reader.feed_eof()
    with pytest.raises(ProtocolError):
        await read_command(reader, ServerConfig())


@pytest.mark.parametrize("value", [Simple(b"a\r"), Error(b"a\n"),
                                    Integer(2**63), Integer(-(2**63) - 1)])
def test_serializer_rejects_invalid_values(value: Reply) -> None:
    with pytest.raises(ValueError):
        serialize_reply(value)


@pytest.mark.parametrize("options", [
    {"max_depth": 0}, {"max_depth": 129}, {"max_input_bytes": 0},
    {"max_output_bytes": 0}, {"read_timeout": float("nan")},
    {"write_timeout": float("inf")},
])
def test_invalid_limits(options: dict[str, int | float]) -> None:
    with pytest.raises(ValueError):
        ServerConfig(**options)  # type: ignore[arg-type]
