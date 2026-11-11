"""Binary-safe RESP2 values, strict command framing and reply serialization.

Lengths count wire bytes. The decoder never allocates from a declared count before
checking limits and never decodes payloads as text. It consumes exactly one frame.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TypeAlias

from .config import ServerConfig

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class ProtocolError(Exception):
    """Malformed or resource-exhausting frame; close after a bounded error."""


@dataclass(frozen=True)
class Simple:
    payload: bytes


@dataclass(frozen=True)
class Error:
    payload: bytes


@dataclass(frozen=True)
class Bulk:
    payload: bytes | None


@dataclass(frozen=True)
class Integer:
    payload: int


@dataclass(frozen=True)
class Array:
    payload: tuple[Reply, ...] | None


Reply: TypeAlias = Simple | Error | Bulk | Integer | Array


@dataclass
class _FrameBudget:
    remaining: int

    def consume(self, size: int) -> None:
        if size > self.remaining:
            raise ProtocolError("frame too large")
        self.remaining -= size


async def _read_line(
    reader: asyncio.StreamReader, config: ServerConfig, budget: _FrameBudget,
    *, allow_eof: bool = False,
) -> bytes | None:
    try:
        line = await reader.readline()
    except ValueError as exc:
        raise ProtocolError("header too long") from exc
    if not line:
        if allow_eof:
            return None
        raise ProtocolError("unexpected end of stream")
    budget.consume(len(line))
    if len(line) > config.max_header_bytes or not line.endswith(b"\r\n"):
        raise ProtocolError("invalid CRLF-terminated header")
    line = line[:-2]
    if not line or b"\r" in line or b"\n" in line:
        raise ProtocolError("invalid header")
    return line


def _decimal(text: bytes, *, signed: bool = False) -> int:
    digits = text[1:] if signed and text[:1] in (b"+", b"-") else text
    if not digits or any(ch < 48 or ch > 57 for ch in digits):
        raise ProtocolError("invalid decimal")
    # Prevent Python's integer digit limit from being reached, even with a
    # large header configuration. Leading zeroes do not affect magnitude.
    significant = digits.lstrip(b"0") or b"0"
    if len(significant) > 19:
        raise ProtocolError("numeric value too large")
    number = int(significant)
    if text.startswith(b"-"):
        number = -number
    if not INT64_MIN <= number <= INT64_MAX:
        raise ProtocolError("integer out of range")
    return number


async def _read_value(
    reader: asyncio.StreamReader,
    config: ServerConfig,
    budget: _FrameBudget,
    depth: int,
    line: bytes,
    *,
    command: bool = False,
) -> Reply:
    prefix, payload = line[:1], line[1:]
    if command and prefix != b"*":
        raise ProtocolError("expected command array")
    if prefix == b"+":
        return Simple(payload)
    if prefix == b"-":
        return Error(payload)
    if prefix == b":":
        return Integer(_decimal(payload, signed=True))
    if prefix == b"$":
        if payload == b"-1":
            return Bulk(None)
        size = _decimal(payload)
        if size > config.max_bulk_bytes:
            raise ProtocolError("bulk string too large")
        budget.consume(size + 2)
        try:
            body = await reader.readexactly(size + 2)
        except asyncio.IncompleteReadError as exc:
            raise ProtocolError("truncated bulk string") from exc
        if not body.endswith(b"\r\n"):
            raise ProtocolError("invalid bulk terminator")
        return Bulk(body[:-2])
    if prefix == b"*":
        if depth >= config.max_depth:
            raise ProtocolError("array nesting too deep")
        if payload == b"-1" and not command:
            return Array(None)
        count = _decimal(payload)
        if count > config.max_args or (command and count == 0):
            raise ProtocolError("invalid array length")
        # Even an empty simple string needs three bytes. Never preallocate
        # from a declared array count or wait for an impossible frame.
        if count > budget.remaining // 3:
            raise ProtocolError("array exceeds frame budget")
        values: list[Reply] = []
        for _ in range(count):
            header = await _read_line(reader, config, budget)
            assert header is not None
            # Reject invalid command arguments before reading their bodies.
            if command and (not header.startswith(b"$") or header == b"$-1"):
                raise ProtocolError("expected non-null bulk argument")
            values.append(await _read_value(reader, config, budget, depth + 1, header))
        return Array(tuple(values))
    raise ProtocolError("invalid RESP2 prefix")


async def _read_top(
    reader: asyncio.StreamReader, config: ServerConfig, *, command: bool = False,
) -> Reply | None:
    budget = _FrameBudget(config.max_frame_bytes)
    header = await _read_line(reader, config, budget, allow_eof=True)
    if header is None:
        return None
    return await _read_value(reader, config, budget, 0, header, command=command)


async def read_value(reader: asyncio.StreamReader, config: ServerConfig) -> Reply | None:
    """Read one value; None denotes clean EOF, distinct from either RESP null.

    The caller supplies a reader with a bounded header limit and applies one
    deadline to the whole operation. The server enforces both requirements.
    """
    return await _read_top(reader, config)


async def read_command(
    reader: asyncio.StreamReader, config: ServerConfig,
) -> list[bytes] | None:
    """Read a nonempty array of non-null bulk arguments; None on clean EOF."""
    value = await _read_top(reader, config, command=True)
    if value is None:
        return None
    assert isinstance(value, Array) and value.payload is not None
    args: list[bytes] = []
    for item in value.payload:
        assert isinstance(item, Bulk) and item.payload is not None
        args.append(item.payload)
    return args


def reply_size(reply: Reply) -> int:
    """Compute wire size without allocating or duplicating bulk payloads."""
    if isinstance(reply, (Simple, Error)):
        return len(reply.payload) + 3
    if isinstance(reply, Integer):
        return len(str(reply.payload)) + 3
    if isinstance(reply, Bulk):
        return 5 if reply.payload is None else len(str(len(reply.payload))) + len(reply.payload) + 5
    if isinstance(reply, Array):
        return 5 if reply.payload is None else len(str(len(reply.payload))) + 3 + sum(
            reply_size(item) for item in reply.payload)
    raise TypeError("unsupported RESP2 reply")


def serialize_reply(reply: Reply) -> bytes:
    """Encode RESP2 values; reject invalid locally constructed replies."""
    if isinstance(reply, (Simple, Error)):
        if b"\r" in reply.payload or b"\n" in reply.payload:
            raise ValueError("invalid simple reply")
        return (b"+" if isinstance(reply, Simple) else b"-") + reply.payload + b"\r\n"
    if isinstance(reply, Bulk):
        if reply.payload is None:
            return b"$-1\r\n"
        return b"$%d\r\n" % len(reply.payload) + reply.payload + b"\r\n"
    if isinstance(reply, Integer):
        if not INT64_MIN <= reply.payload <= INT64_MAX:
            raise ValueError("integer out of range")
        return b":%d\r\n" % reply.payload
    if isinstance(reply, Array):
        if reply.payload is None:
            return b"*-1\r\n"
        return b"*%d\r\n" % len(reply.payload) + b"".join(
            serialize_reply(item) for item in reply.payload)
    raise TypeError("unsupported RESP2 reply")
