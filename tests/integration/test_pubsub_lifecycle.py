from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.pubsub import ack, message, read
from _support.recovery import Partition, config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_disconnect_reconnect_and_shutdown_remove_every_subscription() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as publisher:
            for _ in range(10):
                async with peer(port) as subscriber:
                    assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
                    assert await publisher.call(b'PUBLISH', b'c', b'live') == Integer(1)
                    assert await read(subscriber) == [message(b'c', b'live')]
                async with asyncio.timeout(5):
                    while server.executor.pubsub.channels:
                        await asyncio.sleep(0)
                assert await publisher.call(b'PUBLISH', b'c', b'offline') == Integer(0)
            async with peer(port) as subscriber:
                assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
                # A PING fence proves there is no retained/offline publication before it.
                assert await subscriber.call(b'PING', b'fence') == message_pong(b'fence')
                server.close()
                assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''
    assert not server.executor.pubsub.clients and not server.executor.pubsub.channels
    assert server.client_task_count == 0
    assert not [task for task in asyncio.all_tasks() if task.get_name() == 'pyredis-pubsub-writer']


def message_pong(payload: bytes) -> Array:
    return Array((Bulk(b'pong'), Bulk(payload)))


async def test_replica_pubsub_is_local_transient_and_survives_full_resync(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof', replication_backlog_bytes=1))
    async with primary:
        primary_port = primary.sockets[0].getsockname()[1]
        replica = await start_server(config(tmp_path / 'replica.aof',
            replica_host='127.0.0.1', replica_port=primary_port))
        async with replica:
            replica_port = replica.sockets[0].getsockname()[1]
            await node(replica).wait_ready()
            async with peer(primary_port) as primary_sub, peer(primary_port) as writer, \
                    peer(replica_port) as replica_sub, peer(replica_port) as local:
                for subscriber in (primary_sub, replica_sub):
                    assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
                before = (node(primary).offset, node(replica).offset,
                          replica.executor.store.mutation_revision)
                assert await writer.call(b'PUBLISH', b'c', b'primary') == Integer(1)
                assert await read(primary_sub) == [message(b'c', b'primary')]
                assert await replica_sub.call(b'PING') == message_pong(b'')
                assert await local.call(b'PUBLISH', b'c', b'replica') == Integer(1)
                assert await read(replica_sub) == [message(b'c', b'replica')]
                assert await primary_sub.call(b'PING') == message_pong(b'')
                assert before == (node(primary).offset, node(replica).offset,
                                  replica.executor.store.mutation_revision)
                for command in ((b'INFO', b'replication'), (b'SET', b'k', b'v')):
                    reply = await replica_sub.call(*command)
                    assert isinstance(reply, Error) and b'only (P|S)SUBSCRIBE' in reply.payload
                async with Partition(node(replica)):
                    assert await local.call(b'PUBLISH', b'c', b'disconnected') == Integer(1)
                    assert await read(replica_sub) == [message(b'c', b'disconnected')]
                    denied = await local.call(b'SET', b'k', b'bad')
                    assert isinstance(denied, Error) and denied.payload.startswith(b'READONLY')
                    assert await writer.call(b'SET', b'k', b'good') == Simple(b'OK')
                await node(replica).wait_offset(node(primary).offset)
                assert node(replica).full_syncs == 2
                assert await local.call(b'PUBLISH', b'c', b'after sync') == Integer(1)
                assert await read(replica_sub) == [message(b'c', b'after sync')]
                assert await replica_sub.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', b'c', 0)
                dedicated = await replica_sub.call(b'PYREDIS.SYNC')
                assert dedicated == Error(b'ERR replication requires a dedicated connection')
                assert await asyncio.wait_for(replica_sub.reader.read(), 5) == b''
