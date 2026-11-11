"""Explicit, bounded RESP3 reply encoding; not yet wired to client connections.

This codec is deliberately separate from the RESP2 and peer-replication codecs.
The caller must choose reply types explicitly and preflight before committing writes.
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TypeAlias

from .protocol import INT64_MAX, INT64_MIN, Array, Bulk, Error, Integer, Reply, Simple


@dataclass(frozen=True)
class Null:
    pass


@dataclass(frozen=True)
class Boolean:
    payload: bool


@dataclass(frozen=True)
class Double:
    payload: float


@dataclass(frozen=True)
class ArrayReply:
    payload: tuple[Resp3Value, ...]


@dataclass(frozen=True)
class Map:
    payload: tuple[tuple[Resp3Value, Resp3Value], ...]


@dataclass(frozen=True)
class SetReply:
    payload: tuple[Resp3Value, ...]


@dataclass(frozen=True)
class Push:
    payload: tuple[Resp3Value, ...]


Resp3Value: TypeAlias = (
    Simple | Error | Integer | Bulk | Null | Boolean | Double | ArrayReply | Map | SetReply | Push
)


def from_resp2(value: Reply) -> Resp3Value:
    """Convert protocol-neutral command replies to explicit RESP3 values."""
    if isinstance(value, Bulk):
        return Null() if value.payload is None else value
    if isinstance(value, Array):
        return (Null() if value.payload is None else
                ArrayReply(tuple(from_resp2(item) for item in value.payload)))
    return value


def for_command(args: tuple[bytes, ...] | list[bytes], value: Reply) -> Resp3Value:
    """Choose command-specific RESP3 representations before admitting effects."""
    name = args[0].upper()
    if name == b'GEOPOS' and isinstance(value, Array) and value.payload is not None:
        return ArrayReply(tuple(_geo_coordinate(item) for item in value.payload))
    if name == b'GEOSEARCH' and isinstance(value, Array) and value.payload is not None:
        position = 4 if len(args) > 2 and args[2].upper() == b'FROMMEMBER' else 5
        shape = args[position].upper() if position < len(args) else b''
        position += 3 if shape == b'BYRADIUS' else 4
        options = {arg.upper() for arg in args[position:]}
        if options.intersection((b'WITHDIST', b'WITHCOORD', b'WITHHASH')):
            converted: list[Resp3Value] = []
            for item in value.payload:
                assert isinstance(item, Array) and item.payload is not None
                fields: list[Resp3Value] = [from_resp2(item.payload[0])]
                index = 1
                if b'WITHDIST' in options:
                    dist = item.payload[index]
                    assert isinstance(dist, Bulk) and dist.payload is not None
                    fields.append(from_resp2(dist))
                    index += 1
                if b'WITHHASH' in options:
                    fields.append(from_resp2(item.payload[index]))
                    index += 1
                if b'WITHCOORD' in options:
                    fields.append(_geo_coordinate(item.payload[index]))
                converted.append(ArrayReply(tuple(fields)))
            return ArrayReply(tuple(converted))
    if name in (b'SUBSCRIBE', b'UNSUBSCRIBE', b'PSUBSCRIBE', b'PUNSUBSCRIBE'):
        if isinstance(value, Array) and value.payload is not None:
            return Push(tuple(from_resp2(item) for item in value.payload))
    if name == b'ZRANGE' and len(args) == 5 and args[4].upper() == b'WITHSCORES':
        if isinstance(value, Array) and value.payload is not None:
            pairs: list[Resp3Value] = []
            for member, score in zip(value.payload[::2], value.payload[1::2], strict=True):
                assert isinstance(score, Bulk) and score.payload is not None
                pairs.append(ArrayReply((from_resp2(member), Double(float(score.payload)))))
            return ArrayReply(tuple(pairs))
    if name in (b'HGETALL', b'HELLO') and isinstance(value, Array) and value.payload is not None:
        return Map(tuple((from_resp2(key), from_resp2(item)) for key, item in
                         zip(value.payload[::2], value.payload[1::2], strict=True)))
    if name == b'SMEMBERS' and isinstance(value, Array) and value.payload is not None:
        return SetReply(tuple(from_resp2(item) for item in value.payload))
    if name == b'ZSCORE' and isinstance(value, Bulk) and value.payload is not None:
        return Double(float(value.payload))
    if name == b'ZMSCORE' and isinstance(value, Array) and value.payload is not None:
        return ArrayReply(tuple(Double(float(item.payload))
                                if isinstance(item, Bulk) and item.payload is not None
                                else from_resp2(item) for item in value.payload))
    if name in (b'XREAD', b'XREADGROUP') and isinstance(value, Array) and value.payload is not None:
        parts: list[tuple[Resp3Value, Resp3Value]] = []
        for item in value.payload:
            assert isinstance(item, Array) and item.payload is not None and len(item.payload) == 2
            parts.append((from_resp2(item.payload[0]), from_resp2(item.payload[1])))
        return Map(tuple(parts))
    return from_resp2(value)


def _geo_coordinate(value: Reply) -> Resp3Value:
    if isinstance(value, Array) and value.payload is not None:
        result: list[Resp3Value] = []
        for item in value.payload:
            assert isinstance(item, Bulk) and item.payload is not None
            result.append(Double(float(item.payload)))
        return ArrayReply(tuple(result))
    return from_resp2(value)


def _tokens(value: Resp3Value, depth: int, max_depth: int) -> Iterator[bytes]:
    if isinstance(value, Null):
        yield b"_\r\n"
    elif isinstance(value, (Simple, Error)):
        if b"\r" in value.payload or b"\n" in value.payload:
            raise ValueError("invalid simple reply")
        yield b"+" if isinstance(value, Simple) else b"-"
        yield value.payload
        yield b"\r\n"
    elif isinstance(value, Integer):
        if not INT64_MIN <= value.payload <= INT64_MAX:
            raise ValueError("integer out of range")
        yield b":%d\r\n" % value.payload
    elif isinstance(value, Bulk):
        if value.payload is None:
            raise ValueError("use an explicit RESP3 null")
        yield b"$%d\r\n" % len(value.payload)
        yield value.payload
        yield b"\r\n"
    elif isinstance(value, Boolean):
        if not isinstance(value.payload, bool):
            raise TypeError("boolean payload must be bool")
        yield b"#t\r\n" if value.payload else b"#f\r\n"
    elif isinstance(value, Double):
        if not isinstance(value.payload, float):
            raise ValueError("invalid double")
        number = repr(value.payload)
        if number.endswith(".0"):
            number = number[:-2]
        yield b"," + number.encode("ascii") + b"\r\n"
    elif isinstance(value, (ArrayReply, Map, SetReply, Push)):
        if depth >= max_depth:
            raise ValueError("reply exceeds maximum depth")
        prefix = (b"*" if isinstance(value, ArrayReply) else b"%" if isinstance(value, Map)
                  else b"~" if isinstance(value, SetReply) else b">")
        yield prefix + str(len(value.payload)).encode("ascii") + b"\r\n"
        if isinstance(value, Map):
            for key, item in value.payload:
                yield from _tokens(key, depth + 1, max_depth)
                yield from _tokens(item, depth + 1, max_depth)
        else:
            for item in value.payload:
                yield from _tokens(item, depth + 1, max_depth)
    else:
        raise TypeError("unsupported RESP3 reply")


def encoded_size(value: Resp3Value, *, max_bytes: int, max_depth: int) -> int:
    """Validate and count bytes before a response is allocated or committed."""
    if max_bytes < 0 or not 1 <= max_depth <= 128:
        raise ValueError("invalid size or depth limit")
    size = 0
    for token in _tokens(value, 0, max_depth):
        size += len(token)
        if size > max_bytes:
            raise ValueError("reply exceeds maximum size")
    return size


def encode(value: Resp3Value, *, max_bytes: int, max_depth: int) -> bytes:
    """Serialize a validated reply without exceeding the caller's wire limit."""
    encoded_size(value, max_bytes=max_bytes, max_depth=max_depth)
    return b"".join(_tokens(value, 0, max_depth))
