from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import Partition, config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_hll_geo_recovery_and_replication(tmp_path: Path) -> None:
    primary_config = config(tmp_path / 'primary')
    primary = await start_server(primary_config)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b'PFADD', b'h', b'\0\xff') == Integer(1)
        assert await writer.call(b'GEOADD', b'g', b'13.361389', b'38.115556', b'p') == Integer(1)
        assert await writer.call(b'PEXPIRE', b'h', b'60000') == Integer(1)
        replica = await start_server(config(tmp_path / 'replica', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            assert state.full_syncs == 1
            async with Partition(state):
                assert await writer.call(b'PFADD', b'h', b'second') == Integer(1)
                assert await writer.call(b'GEOADD', b'g', b'15.087269', b'37.502669', b'c') == (
                    Integer(1))
            await state.wait_offset(node(primary).offset)
            assert state.partial_syncs == 1
            assert await reader.call(b'PFCOUNT', b'h') == Integer(2)
            assert await reader.call(b'ZSCORE', b'g', b'p') == Bulk(b'3479099956230698')
            ttl = await writer.call(b'PTTL', b'h')
            assert isinstance(ttl, Integer) and ttl.payload > 0
            assert primary.persistence is not None
            assert (await primary.persistence.compact()).activated
    reopened = await start_server(primary_config)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'PFCOUNT', b'h') == Integer(2)
        assert await reader.call(b'ZSCORE', b'g', b'p') == Bulk(b'3479099956230698')


async def test_hll_all_keys_authorized_and_invalid_state_unchanged(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / 'acl', ['on', '+pfadd', '+pfcount', '+pfmerge', '~allowed:*'])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (server, peer(server.sockets[0].getsockname()[1]) as admin,
                peer(server.sockets[0].getsockname()[1]) as app):
        assert await admin.call(b'AUTH', b'admin', PASSWORD) == Simple(b'OK')
        assert await app.call(b'AUTH', b'app', PASSWORD) == Simple(b'OK')
        assert await admin.call(b'SET', b'private', b'secret') == Simple(b'OK')
        assert await app.call(b'PFADD', b'allowed:a', b'one') == Integer(1)
        for command in ((b'PFCOUNT', b'allowed:a', b'private'),
                        (b'PFMERGE', b'allowed:a', b'private'),
                        (b'PFMERGE', b'private', b'allowed:a')):
            reply = await app.call(*command)
            assert isinstance(reply, Error) and reply.payload.startswith(b'NOPERM')
        assert await app.call(b'PFCOUNT', b'allowed:a') == Integer(1)
        assert await admin.call(b'GET', b'private') == Bulk(b'secret')


async def test_hll_persistence_size_preflight(tmp_path: Path) -> None:
    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / 'journal'),
                                            aof_fsync='always', aof_max_record_bytes=1024))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        owner = server.persistence
        assert owner is not None
        before = owner.file.sequence
        result = await client.call(b'PFADD', b'h', b'value')
        assert isinstance(result, Error) and b'persistence' in result.payload
        assert await client.call(b'GET', b'h') == Bulk(None)
        assert owner.file.sequence == before


@pytest.mark.parametrize('protocol', [2, 3])
async def test_real_redispy_clients(protocol: int, tmp_path: Path) -> None:
    import redis
    import redis.asyncio

    acl = bootstrap(tmp_path / 'acl', ['on', '+@all', '~*', '&*'])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with server:
        port = server.sockets[0].getsockname()[1]
        client = redis.asyncio.Redis(host='127.0.0.1', port=port, username='app',
                                    password=PASSWORD, protocol=protocol, socket_timeout=5)
        try:
            assert await client.pfadd(b'h', b'\0\xff', b'a') == 1
            assert await client.pfcount(b'h') == 2
            assert await client.geoadd(b'g', [13.361389, 38.115556, b'p']) == 1
            assert (await client.geopos(b'g', b'p'))[0] is not None
            assert await client.zscore(b'g', b'p') == 3479099956230698.0
            assert await client.geosearch(b'g', member=b'p', radius=1, unit='km') == [b'p']
            result = await client.geosearch(b'g', member=b'p', radius=1, unit='km',
                                            withdist=True, withcoord=True)
            assert len(result) == 1 and result[0][0] == b'p'
            assert float(result[0][1]) == 0.0
        finally:
            await client.aclose()

        def sync() -> None:
            client = redis.Redis(host='127.0.0.1', port=port, username='app', password=PASSWORD,
                                 protocol=protocol, socket_timeout=5)
            try:
                with client.pipeline() as pipe:
                    assert pipe.pfadd(b'h', b'b').pfcount(b'h').execute() == [1, 3]
                assert client.geodist(b'g', b'p', b'p') == 0.0
            finally:
                client.close()

        await asyncio.to_thread(sync)
