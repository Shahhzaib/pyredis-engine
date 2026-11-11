"""Always-on M04 command contracts and explicitly pending official-Redis comparisons."""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from _support.redis_reference import connect
from _support.redis_reference import exchange as bounded_exchange

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import (
    Array,
    Bulk,
    Error,
    Integer,
    Reply,
    Simple,
)
from pyredis_engine.server import start_server

Step = tuple[tuple[bytes, ...], Reply]
WRONGTYPE = Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")


def arity(name: bytes) -> Error:
    return Error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


CASES: dict[str, tuple[Step, ...]] = {
    "HSET": (
        ((b"HSET", b"k"), arity(b"HSET")),
        ((b"HSET", b"k", b"", b"\x00\xff", b"f", b"1", b"f", b"2"), Integer(2)),
        ((b"HGET", b"k", b"f"), Bulk(b"2")), ((b"HSET", b"k", b"f", b""), Integer(0)),
        ((b"HSET", b"k", b"f", b"new", b"odd"), arity(b"HSET")),
        ((b"HGET", b"k", b"f"), Bulk(b"")),
    ),
    "HGET": (
        ((b"HGET", b"k"), arity(b"HGET")), ((b"HGET", b"k", b"f"), Bulk(None)),
        ((b"HSET", b"k", b"\xff\x00", b"\r\n"), Integer(1)),
        ((b"HGET", b"k", b"\xff\x00"), Bulk(b"\r\n")), ((b"HGET", b"k", b""), Bulk(None)),
    ),
    "HDEL": (
        ((b"HDEL", b"k"), arity(b"HDEL")), ((b"HDEL", b"k", b"missing"), Integer(0)),
        ((b"HSET", b"k", b"a", b"1", b"b", b"2"), Integer(2)),
        ((b"HDEL", b"k", b"a", b"a", b"missing"), Integer(1)),
        ((b"HLEN", b"k"), Integer(1)), ((b"HDEL", b"k", b"b"), Integer(1)),
        ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "HEXISTS": (
        ((b"HEXISTS",), arity(b"HEXISTS")), ((b"HEXISTS", b"k", b"f"), Integer(0)),
        ((b"HSET", b"k", b"f", b""), Integer(1)),
        ((b"HEXISTS", b"k", b"f"), Integer(1)), ((b"HEXISTS", b"k", b"F"), Integer(0)),
    ),
    "HGETALL": (
        ((b"HGETALL",), arity(b"HGETALL")), ((b"HGETALL", b"k"), Array(())),
        ((b"HSET", b"k", b"\xff", b"\r\n", b"", b""), Integer(2)),
        ((b"HGETALL", b"k"), Array((Bulk(b"\xff"), Bulk(b"\r\n"), Bulk(b""), Bulk(b"")))),
    ),
    "HLEN": (
        ((b"HLEN",), arity(b"HLEN")), ((b"HLEN", b"k"), Integer(0)),
        ((b"HSET", b"k", b"f", b"1", b"f", b"2"), Integer(1)),
        ((b"HLEN", b"k"), Integer(1)),
    ),
    "LPUSH": (
        ((b"LPUSH", b"k"), arity(b"LPUSH")),
        ((b"LPUSH", b"k", b"a", b"b", b"c"), Integer(3)),
        ((b"LRANGE", b"k", b"0", b"-1"), Array((Bulk(b"c"), Bulk(b"b"), Bulk(b"a")))),
        ((b"LPUSH", b"k", b"\x00\xff"), Integer(4)), ((b"LLEN", b"k"), Integer(4)),
    ),
    "RPUSH": (
        ((b"RPUSH", b"k"), arity(b"RPUSH")),
        ((b"RPUSH", b"k", b"a", b"b", b"c"), Integer(3)),
        ((b"LRANGE", b"k", b"0", b"-1"), Array((Bulk(b"a"), Bulk(b"b"), Bulk(b"c")))),
        ((b"RPUSH", b"k", b""), Integer(4)), ((b"RPOP", b"k"), Bulk(b"")),
    ),
    "LPOP": (
        ((b"LPOP",), arity(b"LPOP")), ((b"LPOP", b"k"), Bulk(None)),
        ((b"RPUSH", b"k", b"\xff", b"\x00"), Integer(2)),
        ((b"LPOP", b"k"), Bulk(b"\xff")), ((b"LPOP", b"k"), Bulk(b"\x00")),
        ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "RPOP": (
        ((b"RPOP",), arity(b"RPOP")), ((b"RPOP", b"k"), Bulk(None)),
        ((b"RPUSH", b"k", b"a", b"b"), Integer(2)),
        ((b"RPOP", b"k"), Bulk(b"b")), ((b"RPOP", b"k"), Bulk(b"a")),
        ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "LPOP_COUNT": (
        ((b"LPOP", b"k", b"0"), Array(None)),
        ((b"RPUSH", b"k", b"a", b"b", b"c"), Integer(3)),
        ((b"LPOP", b"k", b"0"), Array(())),
        ((b"LPOP", b"k", b"-1"), Error(b"ERR value is out of range, must be positive")),
        ((b"LPOP", b"k", b"bad"), Error(b"ERR value is out of range, must be positive")),
        ((b"LPOP", b"k", b"2"), Array((Bulk(b"a"), Bulk(b"b")))),
        ((b"LPOP", b"k", b"9223372036854775807"), Array((Bulk(b"c"),))),
        ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "RPOP_COUNT": (
        ((b"RPOP", b"k", b"1"), Array(None)),
        ((b"RPUSH", b"k", b"a", b"b", b"c"), Integer(3)),
        ((b"RPOP", b"k", b"0"), Array(())),
        ((b"RPOP", b"k", b"2"), Array((Bulk(b"c"), Bulk(b"b")))),
        ((b"RPOP", b"k", b"10"), Array((Bulk(b"a"),))),
    ),
    "LRANGE": (
        ((b"LRANGE", b"k", b"0"), arity(b"LRANGE")),
        ((b"LRANGE", b"k", b"0", b"-1"), Array(())),
        ((b"RPUSH", b"k", b"a", b"b", b"c"), Integer(3)),
        ((b"LRANGE", b"k", b"-2", b"-1"), Array((Bulk(b"b"), Bulk(b"c")))),
        ((b"LRANGE", b"k", b"0", b"-5"), Array(())),
        ((b"LRANGE", b"k", b"3", b"100"), Array(())),
        ((b"LRANGE", b"k", b"0", b"1"), Array((Bulk(b"a"), Bulk(b"b")))),
        ((b"LRANGE", b"k", b"01", b"1"), Error(b"ERR value is not an integer or out of range")),
    ),
    "LLEN": (
        ((b"LLEN",), arity(b"LLEN")), ((b"LLEN", b"k"), Integer(0)),
        ((b"RPUSH", b"k", b""), Integer(1)), ((b"LLEN", b"k"), Integer(1)),
        ((b"LPOP", b"k"), Bulk(b"")), ((b"LLEN", b"k"), Integer(0)),
    ),
}

WRONG_TYPE_COMMANDS = {
    "HSET": (b"HSET", b"bad", b"f", b"v"), "HGET": (b"HGET", b"bad", b"f"),
    "HDEL": (b"HDEL", b"bad", b"f"), "HEXISTS": (b"HEXISTS", b"bad", b"f"),
    "HGETALL": (b"HGETALL", b"bad"), "HLEN": (b"HLEN", b"bad"),
    "LPUSH": (b"LPUSH", b"bad", b"v"), "RPUSH": (b"RPUSH", b"bad", b"v"),
    "LPOP": (b"LPOP", b"bad"), "RPOP": (b"RPOP", b"bad"),
    "LPOP_COUNT": (b"LPOP", b"bad", b"1"), "RPOP_COUNT": (b"RPOP", b"bad", b"1"),
    "LRANGE": (b"LRANGE", b"bad", b"0", b"-1"), "LLEN": (b"LLEN", b"bad"),
}


async def exchange(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: tuple[bytes, ...],
) -> Reply:
    return await bounded_exchange(reader, writer, command)


def comparable(reply: Reply, command: tuple[bytes, ...]) -> object:
    if command[0] != b"HGETALL" or not isinstance(reply, Array):
        return reply
    assert reply.payload is not None and len(reply.payload) % 2 == 0
    pairs: list[tuple[bytes, bytes]] = []
    for index in range(0, len(reply.payload), 2):
        field, value = reply.payload[index:index + 2]
        assert isinstance(field, Bulk) and isinstance(value, Bulk)
        assert field.payload is not None and value.payload is not None
        pairs.append((field.payload, value.payload))
    assert len({field for field, _ in pairs}) == len(pairs)
    return sorted(pairs)


@pytest_asyncio.fixture
async def local_port() -> AsyncIterator[int]:
    server = await start_server(ServerConfig(port=0))
    async with server:
        yield server.sockets[0].getsockname()[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", CASES)
async def test_m04_command_contract(local_port: int, name: str) -> None:
    reader, writer = await connect(local_port)
    try:
        for command, expected in CASES[name]:
            assert await exchange(reader, writer, command) == expected, command
        assert await exchange(reader, writer, (b"SET", b"bad", b"old")) == Simple(b"OK")
        assert await exchange(reader, writer, WRONG_TYPE_COMMANDS[name]) == WRONGTYPE
        assert await exchange(reader, writer, (b"GET", b"bad")) == Bulk(b"old")
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
@pytest.mark.differential
@pytest.mark.parametrize("name", CASES)
async def test_m04_matches_real_redis(local_port: int, reference_port: int, name: str) -> None:
    local, output = await connect(local_port)
    oracle, reference = await connect(reference_port)
    try:
        assert await exchange(oracle, reference, (b"FLUSHDB",)) == Simple(b"OK")
        for command, expected in CASES[name]:
            actual = await exchange(local, output, command)
            comparison = await exchange(oracle, reference, command)
            assert comparable(actual, command) == comparable(comparison, command)
            assert comparable(comparison, command) == comparable(expected, command)
        for command, expected in [((b"SET", b"bad", b"old"), Simple(b"OK")),
                                  (WRONG_TYPE_COMMANDS[name], WRONGTYPE),
                                  ((b"GET", b"bad"), Bulk(b"old"))]:
            assert await exchange(local, output, command) == expected
            assert await exchange(oracle, reference, command) == expected
    finally:
        output.close()
        reference.close()
        await output.wait_closed()
        await reference.wait_closed()


@pytest.mark.asyncio
@pytest.mark.differential
@pytest.mark.parametrize("kind", ["HASH_TTL", "LIST_TTL"])
async def test_m04_expiry_matches_real_redis(
    local_port: int, reference_port: int, kind: str,
) -> None:
    local, output = await connect(local_port)
    oracle, reference = await connect(reference_port)
    try:
        await exchange(oracle, reference, (b"FLUSHDB",))

        async def compare(command: tuple[bytes, ...]) -> Reply:
            result = await exchange(local, output, command)
            assert result == await exchange(oracle, reference, command)
            return result

        creation = ((b"HSET", b"k", b"f", b"v") if kind == "HASH_TTL"
                    else (b"RPUSH", b"k", b"a", b"b"))
        update = ((b"HSET", b"k", b"g", b"new") if kind == "HASH_TTL"
                  else (b"LPOP", b"k"))
        remove = ((b"HDEL", b"k", b"f", b"g") if kind == "HASH_TTL"
                  else (b"RPOP", b"k"))
        await compare(creation)
        absolute = str(time.time_ns() // 1_000_000 + 60_000).encode()
        await compare((b"PEXPIREAT", b"k", absolute))
        started = time.monotonic_ns()
        await compare(update)
        actual = await exchange(local, output, (b"PTTL", b"k"))
        expected = await exchange(oracle, reference, (b"PTTL", b"k"))
        assert isinstance(actual, Integer) and isinstance(expected, Integer)
        elapsed = (time.monotonic_ns() - started) // 1_000_000
        assert actual.payload > 0 and expected.payload > 0
        assert abs(actual.payload - expected.payload) <= elapsed + 2
        await compare(remove)
        assert await compare((b"PTTL", b"k")) == Integer(-2)
        assert await compare((b"TYPE", b"k")) == Simple(b"none")
    finally:
        output.close()
        reference.close()
        await output.wait_closed()
        await reference.wait_closed()
