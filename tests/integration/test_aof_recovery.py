from __future__ import annotations

from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.persistence]


async def test_all_types_transactions_and_sequential_restarts(tmp_path: Path) -> None:
    path = tmp_path / "engine.aof"
    clock = ManualClock(1000)
    config = ServerConfig(port=0, aof_path=str(path), aof_fsync="always")
    commands = [
        (b"SET", b"\xff\x00", b"\r\n\x00", b"PX", b"500"),
        (b"HSET", b"h", b"\xff", b"v"), (b"RPUSH", b"l", b"\x00", b"", b"\xff"),
        (b"SADD", b"s", b"\xff", b"\x00"), (b"ZADD", b"z", b"0.1", b"a", b"5e-324", b"b"),
    ]
    server = await start_server(config, clock=clock)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        for command in commands:
            assert not isinstance(await connection.call(*command), Error)
        replies = await connection.pipeline([
            (b"MULTI",), (b"INCR", b"count"), (b"INCR", b"\xff\x00"),
            (b"INCR", b"count"), (b"EXEC",)])
        assert replies[-1] == Array((Integer(1), Error(
            b"ERR value is not an integer or out of range"), Integer(2)))
        assert await connection.call(b"SET", b"gone", b"v") == Simple(b"OK")
        assert await connection.call(b"DEL", b"gone") == Integer(1)
    for _ in range(3):
        server = await start_server(config, clock=clock)
        async with server, peer(server.sockets[0].getsockname()[1]) as connection:
            assert await connection.call(b"GET", b"\xff\x00") == Bulk(b"\r\n\x00")
            assert await connection.call(b"PTTL", b"\xff\x00") == Integer(500)
            assert await connection.call(b"HGET", b"h", b"\xff") == Bulk(b"v")
            assert await connection.call(b"LRANGE", b"l", b"0", b"-1") == Array(
                (Bulk(b"\x00"), Bulk(b""), Bulk(b"\xff")))
            assert await connection.call(b"SCARD", b"s") == Integer(2)
            assert await connection.call(b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES") == Array(
                tuple(Bulk(part) for part in (b"b", b"5e-324", b"a", b"0.1")))
            assert await connection.call(b"GET", b"count") == Bulk(b"2")
            assert await connection.call(b"GET", b"gone") == Bulk(None)
    clock.advance(501)
    server = await start_server(config, clock=clock)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"\xff\x00") == Bulk(None)
        assert await connection.call(b"PTTL", b"\xff\x00") == Integer(-2)


async def test_flushdb_is_an_ordered_effect_inside_exec(tmp_path: Path) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "engine.aof"))
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        await connection.call(b"SET", b"old", b"v")
        replies = await connection.pipeline([
            (b"MULTI",), (b"SET", b"before", b"v"), (b"FLUSHDB",),
            (b"SET", b"after", b"new"), (b"EXEC",)])
        assert replies[-1] == Array((Simple(b"OK"), Simple(b"OK"), Simple(b"OK")))
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"DBSIZE") == Integer(1)
        assert await connection.call(b"GET", b"after") == Bulk(b"new")
