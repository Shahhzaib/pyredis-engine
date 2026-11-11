"""Official Redis regressions across snapshot publication and repeated recovery."""
from __future__ import annotations

import time
from pathlib import Path

import pytest
from _support.acceptance_cases import TTLRange
from _support.comparison import Pair, peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential, pytest.mark.persistence]

CASES = {
    'string': ([(b'SET', b'k', b'\x00\xff'), (b'APPEND', b'k', b'\r\n')], (b'GET', b'k')),
    'hash': ([(b'HSET', b'k', b'\xff', b'\x00', b'a', b'v')], (b'HGETALL', b'k')),
    'list': ([(b'RPUSH', b'k', b'a', b'\xff', b'\x00'), (b'LPOP', b'k', b'1')],
             (b'LRANGE', b'k', b'0', b'-1')),
    'set': ([(b'SADD', b'k', b'a', b'\xff', b'a', b'\x00')], (b'SMEMBERS', b'k')),
    'zset': ([(b'ZADD', b'k', b'0.1', b'a', b'5e-324', b'tiny', b'-0', b'zero',
               b'inf', b'\xff', b'0x1p2', b'hex')],
             (b'ZRANGE', b'k', b'0', b'-1', b'WITHSCORES')),
}


@pytest.mark.parametrize('kind', CASES)
async def test_snapshot_types_absolute_ttl_and_restart(
    reference_port: int, tmp_path: Path, kind: str,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True)
    commands, read = CASES[kind]
    async with peer(reference_port) as oracle:
        await oracle.call(b'FLUSHDB')
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            for command in commands:
                await pair.check(command)
            deadline = time.time_ns() // 1_000_000 + 60_000
            await pair.check((b'PEXPIREAT', b'k', str(deadline).encode()), Integer(1))
            await pair.check(read)
            assert server.persistence is not None
            assert (await server.persistence.compact()).activated
            await pair.check((b'INCR', b'after'), Integer(1))
        for _ in range(2):
            server = await start_server(config)
            async with server, peer(server.sockets[0].getsockname()[1]) as local:
                pair = Pair(local, oracle, server)
                await pair.check(read)
                await pair.check((b'PTTL', b'k'), TTLRange(60_000))
                await pair.check((b'GET', b'after'), Bulk(b'1'))
                assert server.persistence is not None
                assert (await server.persistence.compact()).activated


async def test_snapshot_then_atomic_exec_tail_and_clear(
    reference_port: int, tmp_path: Path,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True)
    async with peer(reference_port) as oracle:
        await oracle.call(b'FLUSHDB')
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            await pair.check((b'SET', b'k', b'1'))
            assert server.persistence is not None
            assert (await server.persistence.compact()).activated
            for command in [(b'MULTI',), (b'INCR', b'k'), (b'LPUSH', b'k', b'bad'),
                            (b'INCR', b'k'), (b'EXEC',)]:
                await pair.check(command)
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            await pair.check((b'GET', b'k'), Bulk(b'3'))
            assert server.persistence is not None
            assert (await server.persistence.compact()).activated
            await pair.check((b'FLUSHDB',))
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            pair = Pair(local, oracle, server)
            await pair.check((b'DBSIZE',), Integer(0))
            await pair.check((b'MGET', b'k', b'absent'), Array((Bulk(None), Bulk(None))))
