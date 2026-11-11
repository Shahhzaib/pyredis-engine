from __future__ import annotations

from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.persistence]


async def test_compaction_and_repeated_restart_preserve_all_types(tmp_path: Path) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "engine.aof"),
                          aof_fsync="always", snapshot_enabled=True)
    clock = ManualClock(1000)
    server = await start_server(config, clock=clock)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        commands = [(b"SET", b"\xff", b"\x00\r\n", b"PX", b"500"),
                    (b"HSET", b"h", b"f", b"v"), (b"RPUSH", b"l", b"a", b"b"),
                    (b"SADD", b"s", b"a", b"\xff"), (b"ZADD", b"z", b"0.1", b"a")]
        for command in commands:
            await client.call(*command)
        assert server.persistence is not None
        result = await server.persistence.compact()
        assert result.activated, result.error
        await client.call(b"INCR", b"after")
    for _ in range(3):
        server = await start_server(config, clock=clock)
        async with server, peer(server.sockets[0].getsockname()[1]) as client:
            assert await client.call(b"GET", b"\xff") == Bulk(b"\x00\r\n")
            assert await client.call(b"PTTL", b"\xff") == Integer(500)
            assert await client.call(b"HGET", b"h", b"f") == Bulk(b"v")
            assert await client.call(b"LRANGE", b"l", b"0", b"-1") == Array(
                (Bulk(b"a"), Bulk(b"b")))
            assert await client.call(b"SCARD", b"s") == Integer(2)
            assert await client.call(b"ZSCORE", b"z", b"a") == Bulk(b"0.1")
            assert await client.call(b"GET", b"after") == Bulk(b"1")
            assert server.persistence is not None
            assert (await server.persistence.compact()).activated
    clock.advance(501)
    server = await start_server(config, clock=clock)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"GET", b"\xff") == Bulk(None)
        assert await client.call(b"PING") == Simple(b"PONG")


async def test_rejected_exec_reply_overflow_has_no_effects_after_compaction(tmp_path: Path) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True, max_reply_bytes=128)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'SET', b'large', b'x' * 100)
        replies = await client.pipeline([(b'MULTI',), (b'GET', b'large'),
            (b'GET', b'large'), (b'INCR', b'counter'), (b'EXEC',)])
        assert replies[-1] == Error(
            b'EXECABORT Transaction discarded because reply exceeds configured maximum size')
        assert server.persistence is not None
        assert (await server.persistence.compact()).activated
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'counter') == Bulk(None)
        assert await client.call(b'GET', b'large') == Bulk(b'x' * 100)
