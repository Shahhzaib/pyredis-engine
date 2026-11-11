"""Shared bounded byte streams for clients and private replication peers."""
from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import SupportsIndex

from .config import ServerConfig
from .protocol import ProtocolError


def set_write_limits(writer: asyncio.StreamWriter) -> None:
    # SSLProtocol pauses at size >= high, so high=0 never resumes on some Python
    # versions. One byte retains near-zero backpressure without that deadlock.
    high = 1 if writer.get_extra_info('ssl_object') is not None else 0
    writer.transport.set_write_buffer_limits(high=high, low=0)


class BoundedReader(asyncio.StreamReader):
    """Hard input cap in addition to asyncio's soft transport watermark.

    Only readline and readexactly are used by the decoder.
    Count their consumption without accessing StreamReader's private buffer.
    A transport delivery exceeding the cap is discarded, then the connection
    sends a protocol error and closes. The failed reader accepts no more data.
    """

    def __init__(self, config: ServerConfig) -> None:
        super().__init__(limit=config.max_header_bytes)
        self.buffered_bytes = 0
        self.max_input_bytes = config.max_input_bytes
        self.failed = False
        self.input_ready = asyncio.Event()
        self.closed_event = asyncio.Event()

    def feed_data(self, data: Iterable[SupportsIndex]) -> None:
        if self.failed:
            return
        # Transports deliver bytes. Retain StreamReader's more general input
        # signature for substitutability; bytes(bytes) does not copy the data.
        chunk = bytes(data)
        if len(chunk) > self.max_input_bytes - self.buffered_bytes:
            self.failed = True
            self.set_exception(ProtocolError("input buffer too large"))
            return
        self.buffered_bytes += len(chunk)
        super().feed_data(chunk)
        self.input_ready.set()

    def feed_eof(self) -> None:
        super().feed_eof()
        self.input_ready.set()
        self.closed_event.set()

    def set_exception(self, exc: Exception) -> None:
        super().set_exception(exc)
        self.input_ready.set()
        self.closed_event.set()

    async def wait_input(self) -> None:
        """Permit subscriber idle waiting without removing partial-frame deadlines."""
        while (not self.buffered_bytes and not self.at_eof() and not self.failed
               and self.exception() is None):
            self.input_ready.clear()
            await self.input_ready.wait()

    async def readline(self) -> bytes:
        try:
            data = await super().readline()
        except ValueError:
            # StreamReader discards an overlong line. This is fatal framing,
            # so no further transport deliveries should be retained.
            self.buffered_bytes = 0
            self.failed = True
            raise
        self.buffered_bytes -= len(data)
        return data

    async def readexactly(self, n: int) -> bytes:
        try:
            data = await super().readexactly(n)
        except asyncio.IncompleteReadError as exc:
            self.buffered_bytes -= len(exc.partial)
            raise
        self.buffered_bytes -= len(data)
        return data


async def send(
    writer: asyncio.StreamWriter, payload: bytes, config: ServerConfig,
) -> None:
    # A zero (one for TLS) high watermark drains each bounded chunk.
    # No second reply is queued while this one waits for a slow reader.
    async with asyncio.timeout(config.write_timeout):
        view = memoryview(payload)
        for start in range(0, len(view), config.max_output_bytes):
            writer.write(view[start:start + config.max_output_bytes])
            await writer.drain()


async def close(writer: asyncio.StreamWriter, config: ServerConfig) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=config.write_timeout)
    except (TimeoutError, OSError):
        writer.transport.abort()
