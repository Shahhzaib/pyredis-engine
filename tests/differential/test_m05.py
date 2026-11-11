"""M05 local acceptance traces and required, explicitly pending Redis comparisons."""
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
INVALID = Error(b"ERR value is not a valid float")
WRONGTYPE = Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")


def array(*members: bytes) -> Array:
    return Array(tuple(Bulk(member) for member in members))


def arity(name: bytes) -> Error:
    return Error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


CASES: dict[str, tuple[Step, ...]] = {
    "SADD": (
        ((b"SADD", b"k"), arity(b"SADD")),
        ((b"SADD", b"k", b"", b"\x00\r\n\xff", b""), Integer(2)),
        ((b"SADD", b"k", b""), Integer(0)), ((b"SCARD", b"k"), Integer(2)),
        ((b"SMEMBERS", b"k"), array(b"", b"\x00\r\n\xff")),
    ),
    "SREM": (
        ((b"SREM", b"k"), arity(b"SREM")), ((b"SREM", b"k", b"x"), Integer(0)),
        ((b"SADD", b"k", b"a", b"b"), Integer(2)),
        ((b"SREM", b"k", b"a", b"a", b"missing"), Integer(1)),
        ((b"SCARD", b"k"), Integer(1)), ((b"SREM", b"k", b"b"), Integer(1)),
        ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "SISMEMBER": (
        ((b"SISMEMBER", b"k"), arity(b"SISMEMBER")),
        ((b"SISMEMBER", b"k", b""), Integer(0)),
        ((b"SADD", b"k", b"", b"A"), Integer(2)),
        ((b"SISMEMBER", b"k", b""), Integer(1)), ((b"SISMEMBER", b"k", b"a"), Integer(0)),
    ),
    "SMEMBERS": (
        ((b"SMEMBERS",), arity(b"SMEMBERS")), ((b"SMEMBERS", b"k"), array()),
        ((b"SADD", b"k", b"\xff", b"", b"\x00", b"\xff"), Integer(3)),
        ((b"SMEMBERS", b"k"), array(b"", b"\x00", b"\xff")),
    ),
    "SCARD": (
        ((b"SCARD",), arity(b"SCARD")), ((b"SCARD", b"k"), Integer(0)),
        ((b"SADD", b"k", b"a", b"a"), Integer(1)), ((b"SCARD", b"k"), Integer(1)),
    ),
    "ZADD": (
        ((b"ZADD", b"k", b"1"), arity(b"ZADD")),
        ((b"ZADD", b"k", b"1", b"a", b"2", b"a", b"2", b"b"), Integer(2)),
        ((b"ZSCORE", b"k", b"a"), Bulk(b"2")),
        ((b"ZADD", b"k", b"-1", b"a"), Integer(0)),
        ((b"ZRANGE", b"k", b"0", b"-1"), array(b"a", b"b")),
        ((b"ZADD", b"k", b"3", b"a", b"nan", b"bad"), INVALID),
        ((b"ZSCORE", b"k", b"a"), Bulk(b"-1")), ((b"ZCARD", b"k"), Integer(2)),
    ),
    "ZREM": (
        ((b"ZREM", b"k"), arity(b"ZREM")), ((b"ZREM", b"k", b"x"), Integer(0)),
        ((b"ZADD", b"k", b"1", b"a", b"2", b"b"), Integer(2)),
        ((b"ZREM", b"k", b"a", b"a", b"missing"), Integer(1)),
        ((b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"), array(b"b", b"2")),
        ((b"ZREM", b"k", b"b"), Integer(1)), ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "ZSCORE": (
        ((b"ZSCORE", b"k"), arity(b"ZSCORE")), ((b"ZSCORE", b"k", b"m"), Bulk(None)),
        ((b"ZADD", b"k", b"1.5", b"\x00\xff", b"-0", b""), Integer(2)),
        ((b"ZSCORE", b"k", b"\x00\xff"), Bulk(b"1.5")),
        ((b"ZSCORE", b"k", b""), Bulk(b"0")), ((b"ZSCORE", b"k", b"m"), Bulk(None)),
    ),
    "ZRANGE": (
        ((b"ZRANGE", b"k", b"0"), arity(b"ZRANGE")),
        ((b"ZRANGE", b"k", b"0", b"-1"), array()),
        ((b"ZADD", b"k", b"1", b"\xff", b"1", b"\x00", b"0", b""), Integer(3)),
        ((b"ZRANGE", b"k", b"0", b"-1"), array(b"", b"\x00", b"\xff")),
        ((b"ZRANGE", b"k", b"-2", b"-1"), array(b"\x00", b"\xff")),
        ((b"ZRANGE", b"k", b"0", b"-5"), array()),
        ((b"ZRANGE", b"k", b"3", b"9"), array()),
        ((b"ZRANGE", b"k", b"2", b"1"), array()),
        ((b"ZRANGE", b"k", b"-9223372036854775808", b"9223372036854775807"),
         array(b"", b"\x00", b"\xff")),
        ((b"ZRANGE", b"k", b"01", b"1"), Error(b"ERR value is not an integer or out of range")),
    ),
    "ZCARD": (
        ((b"ZCARD",), arity(b"ZCARD")), ((b"ZCARD", b"k"), Integer(0)),
        ((b"ZADD", b"k", b"1", b"a", b"2", b"a"), Integer(1)),
        ((b"ZCARD", b"k"), Integer(1)),
    ),
    "ZRANGE_WITHSCORES": (
        ((b"ZADD", b"k", b"1", b"\xff", b"1", b"\x00", b"-1.5", b""), Integer(3)),
        ((b"ZRANGE", b"k", b"0", b"-1", b"withscores"),
         array(b"", b"-1.5", b"\x00", b"1", b"\xff", b"1")),
        ((b"ZRANGE", b"k", b"-2", b"-1", b"WITHSCORES"), array(b"\x00", b"1", b"\xff", b"1")),
    ),
    "FLOAT_PRECISION": (
        ((b"ZADD", b"k", b"0.1", b"decimal", b"9007199254740993", b"large"), Integer(2)),
        ((b"ZSCORE", b"k", b"decimal"), Bulk(b"0.1")),
        ((b"ZSCORE", b"k", b"large"), Bulk(b"9007199254740992")),
        ((b"ZADD", b"k", b"5e-324", b"small"), Integer(1)),
        ((b"ZSCORE", b"k", b"small"), Bulk(b"5e-324")),
    ),
    "INFINITY_TIES": (
        ((b"ZADD", b"k", b"+inf", b"\xff", b"-inf", b"a", b"inf", b"b", b"-0", b"z"),
         Integer(4)),
        ((b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"),
         array(b"a", b"-inf", b"z", b"0", b"b", b"inf", b"\xff", b"inf")),
    ),
    "INVALID_SCORES": tuple(
        [((b"ZADD", b"k", b"1", b"old"), Integer(1))]
        + [((b"ZADD", b"k", b"2", b"old", raw, b"bad"), INVALID)
           for raw in (b"nan", b"NaN", b"1e309", b"1e-999", b" 1", b"1 ",
                       b"1_0", b"1\x00", b"\xff")]
        + [((b"ZSCORE", b"k", b"old"), Bulk(b"1")), ((b"ZCARD", b"k"), Integer(1))]
    ),
}

WRONG_TYPE_COMMANDS = {
    "SADD": (b"SADD", b"bad", b"m"), "SREM": (b"SREM", b"bad", b"m"),
    "SISMEMBER": (b"SISMEMBER", b"bad", b"m"), "SMEMBERS": (b"SMEMBERS", b"bad"),
    "SCARD": (b"SCARD", b"bad"), "ZADD": (b"ZADD", b"bad", b"1", b"m"),
    "ZREM": (b"ZREM", b"bad", b"m"), "ZSCORE": (b"ZSCORE", b"bad", b"m"),
    "ZRANGE": (b"ZRANGE", b"bad", b"0", b"-1"), "ZCARD": (b"ZCARD", b"bad"),
    "ZRANGE_WITHSCORES": (b"ZRANGE", b"bad", b"0", b"-1", b"WITHSCORES"),
    "FLOAT_PRECISION": (b"ZADD", b"bad", b"1", b"m"),
    "INFINITY_TIES": (b"ZADD", b"bad", b"1", b"m"),
    "INVALID_SCORES": (b"ZADD", b"bad", b"1", b"m"),
}


async def exchange(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: tuple[bytes, ...],
) -> Reply:
    return await bounded_exchange(reader, writer, command)


def comparable(reply: Reply, command: tuple[bytes, ...]) -> object:
    if command[0].upper() != b"SMEMBERS" or not isinstance(reply, Array):
        return reply
    assert reply.payload is not None
    members: list[bytes] = []
    for part in reply.payload:
        assert isinstance(part, Bulk) and part.payload is not None
        members.append(part.payload)
    assert len(members) == len(set(members))
    return sorted(members)


@pytest_asyncio.fixture
async def local_port() -> AsyncIterator[int]:
    server = await start_server(ServerConfig(port=0))
    async with server:
        yield server.sockets[0].getsockname()[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", CASES)
async def test_m05_command_contract(local_port: int, name: str) -> None:
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
async def test_m05_matches_real_redis(local_port: int, reference_port: int, name: str) -> None:
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
@pytest.mark.parametrize("kind", ["SET_TTL", "ZSET_TTL"])
async def test_m05_expiry_matches_real_redis(
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

        creation = ((b"SADD", b"k", b"a", b"b") if kind == "SET_TTL"
                    else (b"ZADD", b"k", b"1", b"a", b"2", b"b"))
        update = ((b"SADD", b"k", b"c") if kind == "SET_TTL"
                  else (b"ZADD", b"k", b"-1", b"a", b"3", b"c"))
        remove = (b"SREM" if kind == "SET_TTL" else b"ZREM", b"k", b"a", b"b", b"c")
        await compare(creation)
        await compare((b"PEXPIREAT", b"k", str(time.time_ns() // 1_000_000 + 60_000).encode()))
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
