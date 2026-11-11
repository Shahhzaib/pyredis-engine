from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.pubsub import ack, message, read
from _support.recovery import config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.store import StringValue
from pyredis_engine.persistence.codec import Entry, Group
from pyredis_engine.persistence.errors import CapacityError
from pyredis_engine.protocol import Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_metrics_observe_protocol_errors_and_idle_deadlines() -> None:
    server = await start_server(ServerConfig(port=0, read_timeout=0.05))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as malformed:
            malformed.writer.write(b'!\r\n')
            await malformed.writer.drain()
            assert await asyncio.wait_for(malformed.reader.read(), 5) == b'-ERR Protocol error\r\n'
        async with peer(port) as idle:
            assert await asyncio.wait_for(idle.reader.read(), 5) == b''
        assert server.metrics()['protocol_errors'] == server.metrics()['read_timeouts'] == 1


async def test_concurrent_memory_admission_and_observable_metrics() -> None:
    server = await start_server(ServerConfig(port=0, maxmemory_bytes=2000))
    async with server:
        port = server.sockets[0].getsockname()[1]
        barrier = asyncio.Barrier(30)

        async def write(index: int):
            async with peer(port) as client:
                await asyncio.wait_for(barrier.wait(), 5)
                return await client.call(b'SET', bytes([index]), b'v' * 7)

        results = await asyncio.gather(*(write(index) for index in range(30)))
        assert results.count(Simple(b'OK')) == 10
        assert sum(isinstance(result, Error) for result in results) == 20
        metrics = server.metrics()
        assert metrics['memory_accounted_bytes'] == metrics['maxmemory_bytes'] == 2000
        assert metrics['memory_rejections'] == 20 and metrics['commands'] == 30
        assert metrics['connections_accepted'] == 30
    assert server.metrics()['connections_active'] == 0 and server.client_task_count == 0


async def test_bounded_rejection_work_during_connection_flood(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyredis_engine import server as module

    server = await start_server(ServerConfig(port=0, max_clients=1))
    entered, release = asyncio.Event(), asyncio.Event()
    original = module._safe_send

    async def pause(writer, payload, config):
        if payload.startswith(b'-ERR max number'):
            entered.set()
            await release.wait()
        await original(writer, payload, config)

    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as admitted:
            assert await admitted.call(b'PING') == Simple(b'PONG')
            monkeypatch.setattr(module, '_safe_send', pause)
            overflow = [await asyncio.open_connection('127.0.0.1', port) for _ in range(30)]
            try:
                await asyncio.wait_for(entered.wait(), 5)
                async with asyncio.timeout(5):
                    while server.metrics()['connections_rejected'] < 30:
                        await asyncio.sleep(0)
                assert server.client_task_count <= 2
                assert server.metrics()['connections_active'] == 1
                assert await admitted.call(b'PING') == Simple(b'PONG')
            finally:
                release.set()
                for _, writer in overflow:
                    writer.close()
                await asyncio.gather(*(writer.wait_closed() for _, writer in overflow),
                                     return_exceptions=True)


async def test_budget_rejection_preserves_persistent_replication_and_pubsub_state(tmp_path: Path):
    primary = await start_server(config(tmp_path / 'primary.aof', maxmemory_bytes=400))
    async with primary:
        port = primary.sockets[0].getsockname()[1]
        replica = await start_server(config(tmp_path / 'replica.aof', maxmemory_bytes=400,
            replica_host='127.0.0.1', replica_port=port))
        async with replica, peer(port) as writer, peer(port) as subscriber:
            await node(replica).wait_ready()
            await subscriber.call(b'SUBSCRIBE', b'c')
            assert await writer.call(b'SET', b'k', b'v') == Simple(b'OK')
            await node(replica).wait_offset(node(primary).offset)
            assert primary.persistence is not None
            before = (primary.persistence.file.sequence, node(primary).offset,
                      primary.executor.store.mutation_revision)
            rejected = await writer.call(b'MSET', b'k', b'replaced', b'too much', b'x' * 200)
            assert isinstance(rejected, Error) and rejected.payload.startswith(b'OOM ')
            assert before == (primary.persistence.file.sequence, node(primary).offset,
                              primary.executor.store.mutation_revision)
            assert await writer.call(b'PUBLISH', b'c', b'm') == Integer(1)
            assert await read(subscriber) == [message(b'c', b'm')]
            assert await subscriber.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', b'c', 0)
            result = await primary.persistence.compact()
            assert result.activated
            assert primary.metrics()['memory_accounted_bytes'] == 194
    restarted = await start_server(config(tmp_path / 'primary.aof', maxmemory_bytes=400))
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'GET', b'k') == Bulk(b'v')
        assert restarted.metrics()['memory_accounted_bytes'] == 194


async def test_oversize_replication_group_rejected_before_any_state_or_accounting_change(tmp_path):
    server = await start_server(config(tmp_path / 'replica.aof', maxmemory_bytes=200))
    async with server:
        owner = server.persistence
        assert owner is not None
        client = server.executor.new_client()
        try:
            await owner.execute([b'SET', b'k', b'v'], client)
            before = owner.file.sequence, dict(owner.dataset.sizes), owner.dataset.bytes
            with pytest.raises(CapacityError, match='maxmemory'):
                await owner.apply_effect(Group(False, (Entry(b'new', StringValue(b'v'), None),)))
            assert before == (owner.file.sequence, owner.dataset.sizes, owner.dataset.bytes)
            assert server.executor.store.get_string(b'k') == b'v'
            assert server.executor.store.get_string(b'new') is None
            assert not server.is_serving()
        finally:
            server.executor.close_client(client)


async def test_lower_recovery_budget_refuses_startup_without_modifying_journal(tmp_path: Path):
    from dataclasses import replace

    path = tmp_path / 'budget.aof'
    limits = ServerConfig(port=0, aof_path=str(path), aof_fsync='always', maxmemory_bytes=300)
    server = await start_server(limits)
    async with server, peer(server.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b'SET', b'k', b'value') == Simple(b'OK')
    original = path.read_bytes()
    with pytest.raises(CapacityError, match='maxmemory'):
        await start_server(replace(limits, maxmemory_bytes=100))
    assert path.read_bytes() == original
    restarted = await start_server(limits)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'GET', b'k') == Bulk(b'value')
