from __future__ import annotations

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize(
    "commands",
    [
        [
            (b"SETNX", b"k", b"\x00\xff"),
            (b"SETNX", b"k", b"no"),
            (b"GETDEL", b"k"),
            (b"GETDEL", b"k"),
            (b"EXISTS", b"k"),
        ],
        [
            (b"DECRBY", b"n", b"3"),
            (b"DECRBY", b"n", b"-2"),
            (b"DECRBY", b"n", b"-9223372036854775808"),
            (b"GET", b"n"),
        ],
        [
            (b"HSETNX", b"h", b"f", b"a"),
            (b"HSETNX", b"h", b"f", b"b"),
            (b"HMGET", b"h", b"f", b"none"),
            (b"HSTRLEN", b"h", b"f"),
            (b"GETDEL", b"h"),
            (b"HGET", b"h", b"f"),
        ],
        [
            (b"RPUSH", b"l", b"a", b"b"),
            (b"LINDEX", b"l", b"-1"),
            (b"LINDEX", b"l", b"999"),
            (b"LINDEX", b"l", b"bad"),
        ],
        [
            (b"SADD", b"s", b"a", b"\x00\xff"),
            (b"SMISMEMBER", b"s", b"\x00\xff", b"none", b"a"),
            (b"SMISMEMBER", b"missing", b"a"),
        ],
        [
            (b"ZADD", b"z", b"1.5", b"a", b"-inf", b"b"),
            (b"ZMSCORE", b"z", b"a", b"none", b"b"),
            (b"ZMSCORE", b"missing", b"a"),
        ],
        [
            (b"SET", b"k", b"string"),
            (b"HMGET", b"k", b"f"),
            (b"HSTRLEN", b"k", b"f"),
            (b"HSETNX", b"k", b"f", b"v"),
            (b"LINDEX", b"k", b"0"),
            (b"SMISMEMBER", b"k", b"a"),
            (b"ZMSCORE", b"k", b"a"),
            (b"GET", b"k"),
        ],
        [
            (b"CLIENT", b"GETNAME"),
            (b"CLIENT", b"SETNAME", b"named"),
            (b"CLIENT", b"GETNAME"),
            (b"CLIENT", b"SETINFO", b"LIB-NAME", b"test"),
            (b"CLIENT", b"SETINFO", b"LIB-VER", b"1.2"),
            (b"SELECT", b"0"),
            (b"HELLO", b"9"),
            (b"HELLO", b"bad"),
        ],
        [
            (b"SET", b"s", b"\x00abc\xff"),
            (b"GETRANGE", b"s", b"-3", b"-1"),
            (b"GETRANGE", b"s", b"10", b"20"),
            (b"GETRANGE", b"s", b"0", b"-99"),
            (b"GETRANGE", b"missing", b"0", b"-1"),
        ],
        [
            (b"HSET", b"h", b"f", b"v"),
            (b"HKEYS", b"h"),
            (b"HVALS", b"h"),
            (b"HKEYS", b"missing"),
            (b"HVALS", b"missing"),
        ],
        [
            (b"ZADD", b"z", b"1", b"b", b"1", b"a", b"2", b"c"),
            (b"ZRANK", b"z", b"a"),
            (b"ZRANK", b"z", b"b"),
            (b"ZRANK", b"z", b"none"),
            (b"ZRANK", b"missing", b"a"),
        ],
    ],
)
async def test_core_and_handshake_against_redis(
    reference_port: int, commands: list[tuple[bytes, ...]]
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        assert await ref.call(b"FLUSHDB") == await local.call(b"FLUSHDB")
        for command in commands:
            assert await local.call(*command) == await ref.call(*command), command


async def test_hello_resp2_shape(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for connection in (local, ref):
            response = await connection.call(b"HELLO", b"2")
            assert isinstance(response, Array) and response.payload is not None
            fields = dict(zip(response.payload[::2], response.payload[1::2], strict=True))
            assert fields[Bulk(b"proto")] == Integer(2)
            assert fields[Bulk(b"mode")] == Bulk(b"standalone")
            assert fields[Bulk(b"role")] == Bulk(b"master")
