"""M02 command contracts and optional comparisons with a private official Redis.

Never connect to an existing Redis service. Configure PYREDIS_REDIS_SERVER with an
installed binary, or put redis-server on PATH. No downloads or installs occur here.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from _support.redis_reference import connect
from _support.redis_reference import exchange as bounded_exchange

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.collections import HashValue
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import Store
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
OK = Simple(b"OK")
INVALID = Error(b"ERR value is not an integer or out of range")
OVERFLOW = Error(b"ERR increment or decrement would overflow")


def arity(name: bytes) -> Error:
    return Error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


CASES: dict[str, tuple[Step, ...]] = {
    "SET": (
        ((b"SET",), arity(b"SET")), ((b"SET", b"", b"\x00\r\n\xff"), OK),
        ((b"GET", b""), Bulk(b"\x00\r\n\xff")), ((b"set", b"", b""), OK),
        ((b"GET", b""), Bulk(b"")),
        ((b"SET", b"", b"new", b"bad-option"), Error(b"ERR syntax error")),
        ((b"GET", b""), Bulk(b"")),
    ),
    "GET": (
        ((b"GET",), arity(b"GET")), ((b"GET", b"missing"), Bulk(None)),
        ((b"SET", b"Case", b"\xff\x00"), OK), ((b"GET", b"Case"), Bulk(b"\xff\x00")),
        ((b"GET", b"case"), Bulk(None)), ((b"GET", b"a", b"b"), arity(b"GET")),
    ),
    "DEL": (
        ((b"DEL",), arity(b"DEL")), ((b"DEL", b"missing"), Integer(0)),
        ((b"MSET", b"a", b"1", b"b", b"2"), OK),
        ((b"DEL", b"a", b"a", b"missing", b"b"), Integer(2)), ((b"DBSIZE",), Integer(0)),
    ),
    "EXISTS": (
        ((b"EXISTS",), arity(b"EXISTS")), ((b"EXISTS", b"missing"), Integer(0)),
        ((b"SET", b"\x00\xff", b""), OK),
        ((b"EXISTS", b"\x00\xff", b"\x00\xff", b"missing"), Integer(2)),
    ),
    "TYPE": (
        ((b"TYPE",), arity(b"TYPE")), ((b"TYPE", b"missing"), Simple(b"none")),
        ((b"SET", b"k", b"1"), OK), ((b"TYPE", b"k"), Simple(b"string")),
    ),
    "DBSIZE": (
        ((b"DBSIZE", b"x"), arity(b"DBSIZE")), ((b"DBSIZE",), Integer(0)),
        ((b"MSET", b"a", b"1", b"b", b"2", b"a", b"3"), OK),
        ((b"DBSIZE",), Integer(2)),
    ),
    "FLUSHDB": (
        ((b"FLUSHDB",), OK), ((b"SET", b"a", b"v"), OK),
        ((b"FLUSHDB", b"bad-option"), Error(b"ERR syntax error")),
        ((b"DBSIZE",), Integer(1)), ((b"FLUSHDB",), OK), ((b"DBSIZE",), Integer(0)),
    ),
    "INCR": (
        ((b"INCR",), arity(b"INCR")), ((b"INCR", b"n"), Integer(1)),
        ((b"SET", b"n", b"01"), OK), ((b"INCR", b"n"), INVALID),
        ((b"GET", b"n"), Bulk(b"01")), ((b"SET", b"n", b"9223372036854775807"), OK),
        ((b"INCR", b"n"), OVERFLOW), ((b"GET", b"n"), Bulk(b"9223372036854775807")),
    ),
    "DECR": (
        ((b"DECR",), arity(b"DECR")), ((b"DECR", b"n"), Integer(-1)),
        ((b"SET", b"n", b"-9223372036854775808"), OK), ((b"DECR", b"n"), OVERFLOW),
        ((b"GET", b"n"), Bulk(b"-9223372036854775808")),
    ),
    "INCRBY": (
        ((b"INCRBY", b"n"), arity(b"INCRBY")),
        ((b"INCRBY", b"n", b"+1"), INVALID), ((b"EXISTS", b"n"), Integer(0)),
        ((b"INCRBY", b"n", b"-9223372036854775808"), Integer(-(2**63))),
        ((b"INCRBY", b"n", b"-1"), OVERFLOW),
        ((b"INCRBY", b"n", b"9223372036854775807"), Integer(-1)),
        ((b"GET", b"n"), Bulk(b"-1")),
    ),
    "APPEND": (
        ((b"APPEND", b"n"), arity(b"APPEND")), ((b"APPEND", b"n", b""), Integer(0)),
        ((b"EXISTS", b"n"), Integer(1)), ((b"APPEND", b"n", b"\x00\r\n\xff"), Integer(4)),
        ((b"APPEND", b"n", b"abc"), Integer(7)), ((b"GET", b"n"), Bulk(b"\x00\r\n\xffabc")),
    ),
    "STRLEN": (
        ((b"STRLEN",), arity(b"STRLEN")), ((b"STRLEN", b"missing"), Integer(0)),
        ((b"SET", b"k", b"\xe2\x98\x83\x00"), OK), ((b"STRLEN", b"k"), Integer(4)),
    ),
    "MGET": (
        ((b"MGET",), arity(b"MGET")), ((b"MSET", b"a", b"1", b"b", b""), OK),
        ((b"MGET", b"b", b"missing", b"a", b"a"),
         Array((Bulk(b""), Bulk(None), Bulk(b"1"), Bulk(b"1")))),
    ),
    "MSET": (
        ((b"MSET",), arity(b"MSET")), ((b"MSET", b"a", b"1", b"dangling"), arity(b"MSET")),
        ((b"DBSIZE",), Integer(0)), ((b"MSET", b"a", b"1", b"a", b"2", b"", b"\xff"), OK),
        ((b"MGET", b"a", b""), Array((Bulk(b"2"), Bulk(b"\xff")))),
    ),
}


async def exchange(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: tuple[bytes, ...],
) -> Reply:
    return await bounded_exchange(reader, writer, command)


@pytest_asyncio.fixture
async def local_port() -> AsyncIterator[int]:
    server = await start_server(ServerConfig(port=0))
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("name", CASES)
async def test_m02_command_contract(local_port: int, name: str) -> None:
    reader, writer = await connect(local_port)
    try:
        for command, expected in CASES[name]:
            assert await exchange(reader, writer, command) == expected, command
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
@pytest.mark.differential
@pytest.mark.parametrize("name", CASES)
async def test_m02_matches_real_redis(local_port: int, reference_port: int, name: str) -> None:
    local_reader, local_writer = await connect(local_port)
    reader, writer = await connect(reference_port)
    try:
        assert await exchange(reader, writer, (b"FLUSHDB",)) == OK
        for command, expected in CASES[name]:
            reference = await exchange(reader, writer, command)
            assert reference == expected, command
            assert await exchange(local_reader, local_writer, command) == reference, command
    finally:
        local_writer.close()
        writer.close()
        await local_writer.wait_closed()
        await writer.wait_closed()


@pytest.mark.asyncio
@pytest.mark.differential
@pytest.mark.parametrize("command", [
    (b"GET", b"k"), (b"STRLEN", b"k"), (b"APPEND", b"k", b"x"),
    (b"INCR", b"k"), (b"DECR", b"k"), (b"INCRBY", b"k", b"1"),
    (b"MGET", b"k", b"missing"), (b"SET", b"k", b"new"),
    (b"MSET", b"k", b"new", b"other", b"v"),
])
async def test_wrong_type_matches_real_redis(
    reference_port: int, command: tuple[bytes, ...],
) -> None:
    store = Store()
    store.put(b"k", HashValue({b"field": b"value"}, 10))
    engine = Executor(ServerConfig(), store=store)
    reader, writer = await connect(reference_port)
    try:
        assert await exchange(reader, writer, (b"FLUSHDB",)) == OK
        assert await exchange(reader, writer, (b"HSET", b"k", b"field", b"value")) == Integer(1)
        assert engine.execute(list(command)).reply == await exchange(reader, writer, command)
        post_commands = [(b"TYPE", b"k"), (b"PTTL", b"k"), (b"GET", b"other")]
        post_commands.append((b"GET", b"k") if command[0] in (b"SET", b"MSET")
                             else (b"HGETALL", b"k"))
        for post in post_commands:
            assert engine.execute(list(post)).reply == await exchange(reader, writer, post)
    finally:
        writer.close()
        await writer.wait_closed()
