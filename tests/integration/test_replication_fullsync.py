from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.replication.wire import ReplicationError
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.replication]


def config(path: Path, **options: object) -> ServerConfig:
    return ServerConfig(port=0, aof_path=str(path), snapshot_enabled=True,
                        aof_fsync='always', replication_enabled=True, **options)


async def test_real_snapshot_and_live_effects_are_ordered_and_read_only(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        for command in [(b'SET', b'\xff', b'\x00\r\n'), (b'HSET', b'h', b'f', b'v'),
                        (b'RPUSH', b'l', b'a', b'b'), (b'SADD', b's', b'a', b'\xff'),
                        (b'ZADD', b'z', b'5e-324', b'tiny', b'0.1', b'a')]:
            await writer.call(*command)
        replica = await start_server(config(tmp_path / 'replica.aof',
            replica_host='127.0.0.1', replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            assert replica.replication is not None and primary.replication is not None
            await replica.replication.wait_ready()
            assert await reader.call(b'GET', b'\xff') == Bulk(b'\x00\r\n')
            for command in [(b'HGETALL', b'h'), (b'LRANGE', b'l', b'0', b'-1'),
                            (b'SCARD', b's'), (b'ZRANGE', b'z', b'0', b'-1', b'WITHSCORES')]:
                assert await reader.call(*command) == await writer.call(*command)
            replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'counter'),
                (b'INCR', b'paired'), (b'EXEC',)])
            assert replies[-1] == Array((Integer(1), Integer(1)))
            await replica.replication.wait_offset(primary.replication.offset)
            assert await reader.call(b'MGET', b'counter', b'paired') == Array(
                (Bulk(b'1'), Bulk(b'1')))
            denied = await reader.call(b'SET', b'counter', b'bad')
            assert isinstance(denied, Error) and denied.payload.startswith(b'READONLY ')
            assert await reader.call(b'PING') == Simple(b'PONG')
            assert replica.replication.offset == primary.replication.offset
            assert replica.replication.replication_id == primary.replication.replication_id
    # Local durable state is still recoverable without a network connection.
    recovered = await start_server(replace(config(tmp_path / 'replica.aof'),
                                           replication_enabled=False))
    async with recovered, peer(recovered.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'GET', b'counter') == Bulk(b'1')


@pytest.mark.parametrize('control', ['version', 'multi', 'watch'])
async def test_invalid_fullsync_requests_close_only_the_requesting_connection(
    tmp_path: Path, control: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary:
        port = primary.sockets[0].getsockname()[1]
        async with peer(port) as client:
            if control == 'multi':
                await client.call(b'MULTI')
                await client.call(b'INCR', b'uncommitted')
            elif control == 'watch':
                await client.call(b'WATCH', b'k')
            reply = await client.call(b'PYREDIS.SYNC', b'2' if control == 'version' else b'1')
            assert isinstance(reply, Error) and reply.payload.startswith(b'ERR ')
            assert await asyncio.wait_for(client.reader.read(1), 5) == b''
        async with peer(port) as client:
            assert await client.call(b'PING') == Simple(b'PONG')
            assert await client.call(b'GET', b'uncommitted') == Bulk(None)
        assert primary.replication is not None and not primary.replication.sessions


async def test_replica_admission_cap_preserves_existing_replica(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof', max_replicas=1))
    async with primary:
        upstream = dict(replica_host='127.0.0.1', replica_port=primary.sockets[0].getsockname()[1])
        first = await start_server(config(tmp_path / 'first.aof', **upstream))
        async with first:
            assert first.replication is not None and primary.replication is not None
            await first.replication.wait_ready()
            second = await start_server(config(tmp_path / 'second.aof', **upstream))
            async with second:
                assert second.replication is not None
                with pytest.raises(ReplicationError):
                    await second.replication.wait_ready()
                async with peer(primary.sockets[0].getsockname()[1]) as writer:
                    await writer.call(b'INCR', b'k')
                await first.replication.wait_offset(primary.replication.offset)
                async with peer(first.sockets[0].getsockname()[1]) as reader:
                    assert await reader.call(b'GET', b'k') == Bulk(b'1')
                assert len(primary.replication.sessions) == 1
async def test_read_only_queues_and_replicated_writes_invalidate_watch(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'k', b'v')
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            assert replica.replication is not None and primary.replication is not None
            await replica.replication.wait_ready()
            offset = replica.replication.offset
            for command in [(b'SET', b'k', b'bad'), (b'DEL', b'k'), (b'INCR', b'counter'),
                (b'EXPIRE', b'k', b'1'), (b'GETEX', b'k'), (b'HSET', b'h', b'f', b'v'),
                (b'LPUSH', b'l', b'v'), (b'SADD', b's', b'v'), (b'ZADD', b'z', b'1', b'v'),
                (b'MSET', b'k', b'bad'), (b'FLUSHDB',)]:
                reply = await reader.call(*command)
                assert isinstance(reply, Error) and reply.payload.startswith(b'READONLY ')
            replies = await reader.pipeline([(b'MULTI',), (b'GET', b'k'),
                (b'SET', b'k', b'bad'), (b'EXEC',)])
            assert isinstance(replies[2], Error) and replies[2].payload.startswith(b'READONLY ')
            assert isinstance(replies[3], Error) and replies[3].payload.startswith(b'EXECABORT ')
            assert await reader.call(b'GET', b'k') == Bulk(b'v')
            assert replica.replication.offset == offset
            await reader.call(b'WATCH', b'k')
            await reader.call(b'MULTI')
            await reader.call(b'GET', b'k')
            await writer.call(b'SET', b'k', b'v')
            await replica.replication.wait_offset(primary.replication.offset)
            assert await reader.call(b'EXEC') == Array(None)
            info = await reader.call(b'INFO', b'replication')
            assert isinstance(info, Bulk) and info.payload is not None
            assert b'role:slave\r\n' in info.payload and b'master_link_status:up' in info.payload


async def test_primary_restart_and_explicit_replica_restart_require_fresh_full_sync(
    tmp_path: Path,
) -> None:
    primary_config = config(tmp_path / 'primary.aof')
    primary = await start_server(primary_config)
    replica_config = config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
                            replica_port=primary.sockets[0].getsockname()[1])
    replica = await start_server(replica_config)
    async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
        assert primary.replication is not None and replica.replication is not None
        async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
            await replica.replication.wait_ready()
            await writer.call(b'INCR', b'k')
            await replica.replication.wait_offset(primary.replication.offset)
            old_id = primary.replication.replication_id
        async with asyncio.timeout(5):
            while replica.replication.phase != 'down':
                replica.replication.changed.clear()
                await replica.replication.changed.wait()
        error = await reader.call(b'GET', b'k')
        assert isinstance(error, Error) and error.payload.startswith(b'MASTERDOWN ')
        assert await reader.call(b'PING') == Simple(b'PONG')
        assert replica.replication.task is not None and replica.replication.task.done()
    primary = await start_server(primary_config)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert primary.replication is not None and primary.replication.replication_id != old_id
        await writer.call(b'INCR', b'k')
        replica = await start_server(replace(replica_config,
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            assert replica.replication is not None
            await replica.replication.wait_ready()
            assert await reader.call(b'GET', b'k') == Bulk(b'2')
            assert replica.replication.replication_id == primary.replication.replication_id
