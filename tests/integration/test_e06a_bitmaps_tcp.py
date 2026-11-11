from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import Partition, config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_bitmaps_compaction_and_replica_partial_sync(tmp_path: Path) -> None:
    primary_config = config(tmp_path / 'primary')
    primary = await start_server(primary_config)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b'SETBIT', b'k', b'0', b'1') == Integer(0)
        assert await writer.call(b'PEXPIRE', b'k', b'60000') == Integer(1)
        assert await writer.call(b'SET', b'd', b'old') == Simple(b'OK')
        assert await writer.call(b'PEXPIRE', b'd', b'60000') == Integer(1)
        replica = await start_server(config(tmp_path / 'replica',
            replica_host='127.0.0.1', replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            assert state.full_syncs == 1
            async with Partition(state):
                assert await writer.call(b'BITFIELD', b'k', b'SET', b'u8', b'8', b'255') == (
                    Array((Integer(0),)))
                assert await writer.call(b'BITOP', b'NOT', b'd', b'k') == Integer(2)
            await state.wait_offset(node(primary).offset)
            assert state.partial_syncs == 1
            assert await reader.call(b'GET', b'k') == Bulk(b'\x80\xff')
            assert await reader.call(b'GET', b'd') == Bulk(b'\x7f\x00')
            ttl = await writer.call(b'PTTL', b'k')
            assert isinstance(ttl, Integer) and ttl.payload > 0
            assert await writer.call(b'PTTL', b'd') == Integer(-1)
            assert primary.persistence is not None
            assert (await primary.persistence.compact()).activated
    reopened = await start_server(primary_config)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'GET', b'k') == Bulk(b'\x80\xff')
        assert await reader.call(b'GET', b'd') == Bulk(b'\x7f\x00')


async def test_persistence_preflight_rejects_bitmap_growth(tmp_path: Path) -> None:
    settings = ServerConfig(port=0, aof_path=str(tmp_path / 'journal'), aof_fsync='always',
                            aof_max_record_bytes=1024)
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'SET', b'k', b'old') == Simple(b'OK')
        assert server.persistence is not None
        before = server.persistence.file.sequence
        result = await client.call(b'SETBIT', b'k', b'16384', b'1')
        assert isinstance(result, Error) and b'persistence' in result.payload
        assert await client.call(b'GET', b'k') == Bulk(b'old')
        assert server.persistence.file.sequence == before
        result = await client.call(b'BITFIELD', b'k', b'SET', b'u8', b'16384', b'1')
        assert isinstance(result, Error) and b'persistence' in result.payload
        replies = await client.pipeline([
            (b'MULTI',), (b'SETBIT', b'k', b'0', b'1'),
            (b'BITFIELD', b'k', b'SET', b'u8', b'16384', b'1'), (b'EXEC',),
        ])
        assert isinstance(replies[-1], Error) and replies[-1].payload.startswith(b'EXECABORT')
        assert await client.call(b'GET', b'k') == Bulk(b'old')
        assert server.persistence.file.sequence == before


async def test_acl_checks_every_bitop_source_and_destination(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / 'acl.json',
                    ['on', '+bitop', '+get', '+multi', '+exec', '~allowed:*'])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (server, peer(server.sockets[0].getsockname()[1]) as admin,
                peer(server.sockets[0].getsockname()[1]) as app):
        assert await admin.call(b'AUTH', b'admin', PASSWORD) == Simple(b'OK')
        assert await app.call(b'AUTH', b'app', PASSWORD) == Simple(b'OK')
        assert await admin.call(b'SET', b'private', b'private') == Simple(b'OK')
        assert await admin.call(b'SET', b'allowed:src', b'\xff') == Simple(b'OK')
        for args in ((b'BITOP', b'OR', b'allowed:dest', b'private'),
                     (b'BITOP', b'OR', b'private', b'allowed:src')):
            reply = await app.call(*args)
            assert isinstance(reply, Error) and reply.payload.startswith(b'NOPERM')
        assert await admin.call(b'GET', b'private') == Bulk(b'private')
        assert await admin.call(b'GET', b'allowed:dest') == Bulk(None)
        assert await app.call(b'MULTI') == Simple(b'OK')
        assert await app.call(b'BITOP', b'NOT', b'allowed:dest', b'allowed:src') == (
            Simple(b'QUEUED'))
        assert await admin.call(b'ACL', b'SETUSER', b'app', b'-bitop') == Simple(b'OK')
        result = await app.call(b'EXEC')
        assert isinstance(result, Array) and result.payload is not None
        assert (isinstance(result.payload[0], Error)
                and result.payload[0].payload.startswith(b'NOPERM'))
        assert await admin.call(b'GET', b'allowed:dest') == Bulk(None)


@pytest.mark.parametrize('protocol', [2, 3])
async def test_real_redispy_bitfield_and_pipeline(protocol: int, tmp_path: Path) -> None:
    import redis
    import redis.asyncio

    acl = bootstrap(tmp_path / 'acl.json', ['on', '+@all', '~*', '&*'])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with server:
        client = redis.asyncio.Redis(host='127.0.0.1', port=server.sockets[0].getsockname()[1],
                                    username='app', password=PASSWORD, protocol=protocol)
        try:
            assert await client.setbit(b'b', 0, 1) == 0
            assert await client.getbit(b'b', 0) == 1
            assert await client.bitcount(b'b') == 1
            assert await client.bitpos(b'b', 1) == 0
            assert await client.bitop('NOT', b'd', b'b') == 1
            field = client.bitfield(b'b').set('u8', 8, 42).get('u8', 8)
            assert await field.execute() == [0, 42]
            async with client.pipeline() as pipe:
                assert await pipe.setbit(b'b', 1, 1).getbit(b'b', 1).execute() == [0, 1]
        finally:
            await client.aclose()

        def sync() -> None:
            client = redis.Redis(host='127.0.0.1', port=server.sockets[0].getsockname()[1],
                                 username='app', password=PASSWORD, protocol=protocol)
            try:
                assert client.setbit(b'sync', 9, 1) == 0
                assert client.getbit(b'sync', 9) == 1
                assert client.bitfield(b'sync').get('u8', 8).execute() == [64]
                with client.pipeline() as pipe:
                    assert pipe.setbit(b'sync', 0, 1).bitcount(b'sync').execute() == [0, 2]
            finally:
                client.close()

        await asyncio.to_thread(sync)


async def test_concurrent_clients_set_distinct_bits() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]

        async def write(index: int) -> None:
            async with peer(port) as client:
                assert await client.call(b'SETBIT', b'k', str(index).encode(), b'1') == Integer(0)

        await asyncio.gather(*(write(index) for index in range(100)))
        async with peer(port) as client:
            assert await client.call(b'BITCOUNT', b'k') == Integer(100)
