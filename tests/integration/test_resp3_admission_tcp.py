from __future__ import annotations

from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Error, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_hello_auth_overflow_keeps_tcp_connection_unauthenticated(tmp_path: Path) -> None:
    server = await start_server(ServerConfig(
        port=0, max_reply_bytes=128, acl_file=bootstrap(tmp_path / 'acl.json')))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'HELLO', b'3', b'AUTH', b'admin', PASSWORD,
                                 b'SETNAME', b'new') == Error(
                                     b'ERR reply exceeds configured maximum size')
        assert await client.call(b'PING') == Error(b'NOAUTH Authentication required.')
        assert await client.call(b'AUTH', b'admin', PASSWORD) == Simple(b'OK')
        assert await client.call(b'PING') == Simple(b'PONG')


async def test_resp3_overflow_rolls_back_journal_and_replication(tmp_path: Path) -> None:
    import redis.asyncio
    from redis.exceptions import ExecAbortError

    primary_config = config(tmp_path / 'primary.aof', max_reply_bytes=160)
    primary = await start_server(primary_config)
    async with primary:
        port = primary.sockets[0].getsockname()[1]
        client = redis.asyncio.Redis(host='127.0.0.1', port=port, protocol=3, socket_timeout=5)
        try:
            for number in range(7):
                assert await client.zadd(b'z', {f'm{number:04}'.encode(): 1.25}) == 1
            replica = await start_server(config(tmp_path / 'replica.aof', max_reply_bytes=160,
                replica_host='127.0.0.1', replica_port=port))
            async with replica:
                await node(replica).wait_ready()
                owner = primary.persistence
                assert owner is not None
                before = owner.file.sequence, node(primary).offset
                with pytest.raises(ExecAbortError):
                    async with client.pipeline() as pipe:
                        pipe.set(b'result', b'committed')
                        pipe.zrange(b'z', 0, -1, withscores=True)
                        await pipe.execute()
                assert await client.get(b'result') is None
                assert (owner.file.sequence, node(primary).offset) == before
                replica_client = redis.asyncio.Redis(
                    host='127.0.0.1', port=replica.sockets[0].getsockname()[1], protocol=3)
                try:
                    assert await replica_client.get(b'result') is None
                finally:
                    await replica_client.aclose()
        finally:
            await client.aclose()
    restarted = await start_server(primary_config)
    async with restarted:
        client = redis.asyncio.Redis(host='127.0.0.1',
                                     port=restarted.sockets[0].getsockname()[1], protocol=3)
        try:
            assert await client.get(b'result') is None
        finally:
            await client.aclose()
