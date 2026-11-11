from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _frame(reader: asyncio.StreamReader) -> bytes:
    line = await asyncio.wait_for(reader.readline(), 3)
    if line[:1] == b'$':
        size = int(line[1:-2])
        return line if size < 0 else line + await reader.readexactly(size + 2)
    if line[:1] in (b'*', b'%', b'~', b'>'):
        count = int(line[1:-2])
        for _ in range(max(0, count * (2 if line[:1] == b'%' else 1))):
            line += await _frame(reader)
    return line


def _request(*args: bytes) -> bytes:
    return serialize_reply(Array(tuple(Bulk(arg) for arg in args)))


async def test_hello_auth_failure_switching_and_pipeline(tmp_path: Path) -> None:
    server = await start_server(ServerConfig(
        port=0, acl_file=bootstrap(tmp_path / 'acl.json', ['on', '+@all', '~*', '&*'])))
    async with server:
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        try:
            writer.write(_request(b'HELLO', b'3', b'AUTH', b'app', b'wrong'))
            await writer.drain()
            assert (await _frame(reader)).startswith(b'-WRONGPASS')
            writer.write(_request(b'PING'))
            await writer.drain()
            assert (await _frame(reader)).startswith(b'-NOAUTH')
            writer.write(_request(b'HELLO', b'3', b'AUTH', b'app', PASSWORD,
                                  b'SETNAME', b'binary-client') + _request(b'PING'))
            await writer.drain()
            assert (await _frame(reader)).startswith(b'%7\r\n')
            assert await _frame(reader) == b'+PONG\r\n'
            writer.write(_request(b'HELLO', b'4') + _request(b'HELLO', b'2')
                         + _request(b'CLIENT', b'GETNAME'))
            await writer.drain()
            assert (await _frame(reader)).startswith(b'-NOPROTO')
            assert (await _frame(reader)).startswith(b'*14\r\n')
            assert await _frame(reader) == b'$13\r\nbinary-client\r\n'
        finally:
            writer.close()
            await writer.wait_closed()


async def test_redispy_resp3_authenticated_sync_and_async(tmp_path: Path) -> None:
    import redis
    import redis.asyncio

    server = await start_server(ServerConfig(
        port=0, acl_file=bootstrap(tmp_path / 'acl.json', ['on', '+@all', '~*', '&*'])))
    async with server:
        port = server.sockets[0].getsockname()[1]

        def sync() -> None:
            client = redis.Redis(host='127.0.0.1', port=port, username='app',
                                 password=PASSWORD, protocol=3, socket_timeout=5)
            try:
                assert client.ping()
                assert client.set(b'bin', b'\x00\xff')
                assert client.get(b'bin') == b'\x00\xff'
                assert client.hset(b'h', b'f', b'v') == 1
                assert client.hgetall(b'h') == {b'f': b'v'}
                assert client.sadd(b's', b'x') == 1
                assert client.smembers(b's') == {b'x'}
                with client.pipeline() as pipe:
                    assert pipe.set(b'p', b'v').get(b'p').execute() == [True, b'v']
                with client.pipeline() as pipe:
                    pipe.watch(b'p')
                    assert pipe.get(b'p') == b'v'
                    pipe.multi()
                    assert pipe.set(b'p', b'next').get(b'p').execute() == [True, b'next']
                assert client.zadd(b'z', {b'a': 1.25, b'b': 2.0}) == 2
                assert client.zscore(b'z', b'a') == 1.25
                assert client.zrange(b'z', 0, -1, withscores=True) == [[b'a', 1.25], [b'b', 2.0]]
                stream_id = client.xadd(b'stream', {b'f': b'v'})
                assert client.xread({b'stream': b'0'}) == {
                    b'stream': [[(stream_id, {b'f': b'v'})]]}
                assert client.xgroup_create(b'stream', b'g', id='0')
                assert client.xreadgroup(b'g', b'c', {b'stream': b'>'}) == {
                    b'stream': [[(stream_id, {b'f': b'v'})]]}
                with client.pubsub() as sub:
                    sub.subscribe(b'ch')
                    assert sub.get_message(timeout=2)['type'] == 'subscribe'
                    sub.psubscribe(b'c*')
                    assert sub.get_message(timeout=2)['type'] == 'psubscribe'
                    assert client.publish(b'ch', b'\x00\xff') == 2
                    messages = [sub.get_message(timeout=2), sub.get_message(timeout=2)]
                    assert {item['type'] for item in messages} == {'message', 'pmessage'}
                    assert all(item['data'] == b'\x00\xff' for item in messages)
                client.connection_pool.disconnect()
                assert client.get(b'bin') == b'\x00\xff'
            finally:
                client.close()

        await asyncio.wait_for(asyncio.to_thread(sync), 20)
        client = redis.asyncio.Redis(host='127.0.0.1', port=port, username='app',
                                    password=PASSWORD, protocol=3, socket_timeout=5)
        try:
            assert await client.ping()
            assert await client.get(b'bin') == b'\x00\xff'
            assert await client.hgetall(b'h') == {b'f': b'v'}
            assert await client.smembers(b's') == {b'x'}
            async with client.pubsub() as sub:
                await sub.subscribe(b'async')
                assert (await sub.get_message(timeout=2))['type'] == 'subscribe'
                assert await client.publish(b'async', b'payload') == 1
                assert (await sub.get_message(timeout=2))['data'] == b'payload'
        finally:
            await client.aclose()


async def test_redispy_resp3_tls_acl(tmp_path: Path) -> None:
    import redis

    tls_dir = os.environ.get('PYREDIS_TEST_TLS_DIR')
    if not tls_dir:
        from _support.tls_fixtures import generate
        tls_path = tmp_path / 'tls'
        tls_path.mkdir()
        generate(tls_path)
    else:
        tls_path = Path(tls_dir)
    server = await start_server(ServerConfig(
        port=0, acl_file=bootstrap(tmp_path / 'acl.json', ['on', '+@all', '~*', '&*']),
        tls_cert_file=str(tls_path / 'server.pem'), tls_key_file=str(tls_path / 'server.key')))
    async with server:
        port = server.sockets[0].getsockname()[1]

        def sync() -> None:
            client = redis.Redis(host='localhost', port=port, username='app', password=PASSWORD,
                                 protocol=3, ssl=True, ssl_ca_certs=str(tls_path / 'ca.pem'),
                                 ssl_check_hostname=True, socket_timeout=5)
            try:
                assert client.ping()
                assert client.set(b'tls', b'\xff')
                assert client.get(b'tls') == b'\xff'
            finally:
                client.close()
        await asyncio.wait_for(asyncio.to_thread(sync), 20)


async def test_resp3_acl_denied_write_has_no_effect(tmp_path: Path) -> None:
    import redis.asyncio
    from redis.exceptions import NoPermissionError

    server = await start_server(ServerConfig(
        port=0, acl_file=bootstrap(tmp_path / 'acl.json',
                                  ['on', '+HELLO', '+CLIENT', '+PING', '+GET', '~*', '&*'])))
    async with server:
        client = redis.asyncio.Redis(host='127.0.0.1', port=server.sockets[0].getsockname()[1],
                                    username='app', password=PASSWORD, protocol=3)
        try:
            assert await client.ping()
            with pytest.raises(NoPermissionError):
                await client.set(b'blocked', b'value')
            assert await client.get(b'blocked') is None
        finally:
            await client.aclose()
