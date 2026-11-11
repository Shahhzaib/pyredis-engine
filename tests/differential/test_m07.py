"""Reference regressions through the persistence path and after local restart."""
from __future__ import annotations

import time
from pathlib import Path

import pytest
from _support.acceptance_cases import TTLRange
from _support.comparison import Pair, peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential, pytest.mark.persistence]

CASES = {
    "string": ([(b"SET", b"k", b"\xff"), (b"APPEND", b"k", b"\x00\r\n")], (b"GET", b"k")),
    "hash": ([(b"HSET", b"k", b"a", b"v", b"b", b""), (b"HDEL", b"k", b"a"),
              (b"HSET", b"k", b"b", b"\xff")], (b"HGETALL", b"k")),
    "list": ([(b"RPUSH", b"k", b"a", b"\x00\xff", b"c"), (b"LPOP", b"k", b"1"),
              (b"RPOP", b"k")], (b"LRANGE", b"k", b"0", b"-1")),
    "set": ([(b"SADD", b"k", b"a", b"b", b"a", b"\xff"), (b"SREM", b"k", b"b")],
            (b"SMEMBERS", b"k")),
    "zset": ([(b"ZADD", b"k", b"1", b"a", b"0x1p2", b"b", b"5e-324", b"tiny"),
              (b"ZADD", b"k", b"0.1", b"a"), (b"ZREM", b"k", b"tiny")],
             (b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES")),
}


@pytest.mark.parametrize("kind", CASES)
async def test_persisted_type_and_deadline_match_redis(
    reference_port: int, tmp_path: Path, kind: str,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    commands, snapshot = CASES[kind]
    async with peer(reference_port) as oracle:
        await oracle.call(b"FLUSHDB")
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            for command in commands:
                await pair.check(command)
            deadline = time.time_ns() // 1_000_000 + 60_000
            await pair.check((b"PEXPIREAT", b"k", str(deadline).encode()), Integer(1))
            before = await pair.check(snapshot)
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            await pair.check(snapshot, before)
            await pair.check((b"PTTL", b"k"), TTLRange(60_000))
            await pair.check((b"SET", b"k", b"replacement"), Simple(b"OK"))
            await pair.check((b"PTTL", b"k"), Integer(-1))
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            await Pair(local, oracle, server).check((b"GET", b"k"), Bulk(b"replacement"))


async def test_exec_effect_group_and_watch_after_restart(
    reference_port: int, tmp_path: Path,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    async with peer(reference_port) as oracle:
        await oracle.call(b"FLUSHDB")
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            for command in [(b"SET", b"k", b"1"), (b"MULTI",), (b"INCR", b"k"),
                            (b"LPUSH", b"k", b"bad"), (b"INCR", b"k"), (b"EXEC",)]:
                await pair.check(command)
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            await pair.check((b"GET", b"k"), Bulk(b"3"))
            for command in [(b"WATCH", b"k"), (b"SET", b"k", b"3"), (b"MULTI",),
                            (b"INCR", b"k")]:
                await pair.check(command)
            await pair.check((b"EXEC",), Array(None))
