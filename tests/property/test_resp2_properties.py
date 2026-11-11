"""Reproducible generated values, arbitrary bytes and fragmentation."""
from __future__ import annotations

import asyncio

from hypothesis import given, settings
from hypothesis import strategies as st

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

safe_line = st.binary(max_size=40).filter(lambda raw: b"\r" not in raw and b"\n" not in raw)
scalars = st.one_of(
    safe_line.map(Simple), safe_line.map(Error),
    st.integers(-(2**63), 2**63 - 1).map(Integer),
    st.one_of(st.none(), st.binary(max_size=100)).map(Bulk), st.just(Array(None)),
)
values = st.recursive(scalars, lambda child: st.lists(child, max_size=5).map(
    lambda items: Array(tuple(items))), max_leaves=25)
deterministic = settings(max_examples=150, derandomize=True, database=None, deadline=None)


@deterministic
@given(values)
def test_generated_resp2_roundtrip(value: Reply) -> None:
    async def check() -> None:
        reader = asyncio.StreamReader(limit=128)
        reader.feed_data(serialize_reply(value))
        reader.feed_eof()
        assert await read_value(reader, ServerConfig()) == value
        assert await read_value(reader, ServerConfig()) is None
    asyncio.run(check())


@deterministic
@given(st.lists(st.binary(max_size=100), min_size=1, max_size=10),
       st.lists(st.integers(1, 17), min_size=1, max_size=20))
def test_generated_fragmented_command_pipeline(args: list[bytes], sizes: list[int]) -> None:
    async def check() -> None:
        reader = asyncio.StreamReader(limit=128)
        wire = serialize_reply(Array(tuple(Bulk(arg) for arg in args)))
        wire += b"*1\r\n$4\r\nPING\r\n"

        async def parse() -> None:
            assert await read_command(reader, ServerConfig()) == args
            assert await read_command(reader, ServerConfig()) == [b"PING"]
            assert await read_command(reader, ServerConfig()) is None

        task = asyncio.create_task(parse())
        offset = 0
        for size in sizes:
            reader.feed_data(wire[offset:offset + size])
            offset += size
            await asyncio.sleep(0)
        reader.feed_data(wire[offset:])
        reader.feed_eof()
        await asyncio.wait_for(task, 1)
    asyncio.run(check())


@deterministic
@given(st.binary(max_size=256))
def test_arbitrary_wire_never_escapes_protocol_errors(raw: bytes) -> None:
    async def check() -> None:
        reader = asyncio.StreamReader(limit=32)
        reader.feed_data(raw)
        reader.feed_eof()
        config = ServerConfig(max_bulk_bytes=128, max_frame_bytes=256,
                              max_args=8, max_depth=4, max_header_bytes=32)
        try:
            while await read_value(reader, config) is not None:
                pass
        except ProtocolError:
            pass
    asyncio.run(check())
