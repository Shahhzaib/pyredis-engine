from __future__ import annotations

import asyncio
from typing import cast

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import ProtocolError
from pyredis_engine.server import _BoundedReader, _safe_send


async def test_input_accounting_and_hard_ceiling() -> None:
    reader = _BoundedReader(ServerConfig(max_bulk_bytes=16, max_input_bytes=64,
                                         max_header_bytes=32))
    reader.feed_data(b"$3\r\na\x00b\r\n")
    assert reader.buffered_bytes == 9
    assert await reader.readline() == b"$3\r\n"
    assert reader.buffered_bytes == 5
    assert await reader.readexactly(5) == b"a\x00b\r\n"
    assert reader.buffered_bytes == 0
    reader.feed_data(b"x" * 64)
    assert reader.buffered_bytes == 64
    reader.feed_data(b"x")
    assert reader.failed
    assert reader.buffered_bytes == 64
    reader.feed_data(b"x" * 1024)
    assert reader.buffered_bytes == 64
    with pytest.raises(ProtocolError):
        await reader.readline()


async def test_partial_eof_accounting() -> None:
    for line in (False, True):
        reader = _BoundedReader(ServerConfig())
        reader.feed_data(b"abc")
        reader.feed_eof()
        if line:
            assert await reader.readline() == b"abc"
        else:
            with pytest.raises(asyncio.IncompleteReadError):
                await reader.readexactly(4)
        assert reader.buffered_bytes == 0


class ControlledWriter:
    def __init__(self, *, stall: bool = False) -> None:
        self.chunks: list[bytes] = []
        self.drains = 0
        self.stall = stall

    def write(self, data: bytes) -> None:
        # Each write must follow the previous drain.
        assert len(self.chunks) == self.drains
        self.chunks.append(bytes(data))

    async def drain(self) -> None:
        if self.stall:
            await asyncio.Event().wait()
        self.drains += 1


async def test_output_chunks_are_bounded_and_drained_in_order() -> None:
    writer = ControlledWriter()
    config = ServerConfig(max_output_bytes=7)
    await _safe_send(cast(asyncio.StreamWriter, writer), b"x" * 32, config)
    assert b"".join(writer.chunks) == b"x" * 32
    assert max(map(len, writer.chunks)) <= 7
    assert writer.drains == 5


async def test_blocked_output_times_out_without_queuing_more() -> None:
    writer = ControlledWriter(stall=True)
    config = ServerConfig(max_output_bytes=7, write_timeout=0.01)
    with pytest.raises(TimeoutError):
        await _safe_send(cast(asyncio.StreamWriter, writer), b"x" * 32, config)
    assert writer.chunks == [b"x" * 7]
