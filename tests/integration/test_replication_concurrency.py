from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.replication]


def config(path: Path, **options: object) -> ServerConfig:
    return ServerConfig(port=0, aof_path=str(path), snapshot_enabled=True,
                        aof_fsync='always', replication_enabled=True, **options)


async def dormant(interval: float) -> None:
    await asyncio.Event().wait()


@pytest.mark.parametrize('clear', [False, True])
async def test_fullsync_boundary_with_100_clients_expiry_and_compaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clear: bool,
) -> None:
    clock = ManualClock(1000)
    primary = await start_server(config(tmp_path / 'primary.aof'), clock=clock, expiry_wait=dormant)
    owner = primary.persistence
    assert owner is not None and owner.generations is not None
    entered, release = threading.Event(), threading.Event()

    def pause(stage: str) -> None:
        if stage == 'replication_snapshot':
            entered.set()
            assert release.wait(15)

    monkeypatch.setattr(owner.generations, '_stage', pause)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'before', b'\x00\xff')
        await writer.call(b'SET', b'expired', b'v', b'PX', b'2')
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]), clock=clock)
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            compacting = None
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                loading = await reader.call(b'GET', b'before')
                assert isinstance(loading, Error) and loading.payload.startswith(b'LOADING ')
                if clear:
                    await writer.call(b'FLUSHDB')
                compacting = asyncio.create_task(owner.compact())

                async def write(index: int) -> None:
                    member = str(index).encode()
                    async with peer(primary.sockets[0].getsockname()[1]) as client:
                        replies = await client.pipeline([(b'MULTI',), (b'INCR', b'counter'),
                            (b'INCR', b'paired'), (b'HSET', b'h', member, b'\x00\xff'),
                            (b'RPUSH', b'l', member), (b'SADD', b's', member),
                            (b'ZADD', b'z', member, member), (b'EXEC',)])
                        assert isinstance(replies[-1], Array) and replies[-1].payload is not None
                        assert len(replies[-1].payload) == 6

                await asyncio.gather(*(write(i) for i in range(100)))
                clock.advance(3)
                await owner.expire_batch()
                assert not compacting.done()  # Pin is leased; writes still completed.
            finally:
                release.set()
            assert replica.replication is not None and primary.replication is not None
            await replica.replication.wait_ready()
            if compacting is not None:
                assert (await compacting).activated
            await replica.replication.wait_offset(primary.replication.offset)
            assert await reader.call(b'MGET', b'counter', b'paired') == Array(
                (Bulk(b'100'), Bulk(b'100')))
            assert await reader.call(b'GET', b'before') == Bulk(None if clear else b'\x00\xff')
            assert await reader.call(b'GET', b'expired') == Bulk(None)
            assert replica.executor.store.physical_entry(b'expired')[0] is None
            for command in [(b'HLEN', b'h'), (b'LLEN', b'l'), (b'SCARD', b's'), (b'ZCARD', b'z')]:
                assert await reader.call(*command) == Integer(100)
            for command in [(b'LRANGE', b'l', b'0', b'-1'),
                            (b'ZRANGE', b'z', b'0', b'-1', b'WITHSCORES')]:
                assert await reader.call(*command) == await writer.call(*command)
            assert replica.replication.offset == primary.replication.offset
    assert not list(tmp_path.glob('.pyredis-sync-*'))


@pytest.mark.parametrize('side', ['primary', 'replica'])
async def test_replication_offsets_advance_only_after_durability_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, side: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(primary.sockets[0].getsockname()[1]) as writer:
            assert primary.replication is not None and replica.replication is not None
            await replica.replication.wait_ready()
            node = primary if side == 'primary' else replica
            assert node.persistence is not None
            entered, release = threading.Event(), threading.Event()
            original = node.persistence.file.sync

            def pause() -> None:
                entered.set()
                assert release.wait(10)
                original()

            monkeypatch.setattr(node.persistence.file, 'sync', pause)
            before = replica.replication.offset
            pending = asyncio.create_task(writer.call(b'INCR', b'k'))
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                assert replica.replication.offset == before
                if side == 'primary':
                    assert primary.replication.offset == before
                    assert not pending.done()
            finally:
                release.set()
            assert await pending == Integer(1)
            await replica.replication.wait_offset(primary.replication.offset)
            assert replica.replication.offset > before


async def test_replica_hides_expiry_without_local_deletion_or_journal_effect(
    tmp_path: Path,
) -> None:
    primary_clock, replica_clock = ManualClock(1000), ManualClock(1000)
    primary = await start_server(config(tmp_path / 'primary.aof'), clock=primary_clock,
                                 expiry_wait=dormant)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'k', b'v', b'PX', b'2')
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]), clock=replica_clock)
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            assert replica.replication is not None and primary.replication is not None
            assert replica.persistence is not None and primary.persistence is not None
            await replica.replication.wait_ready()
            sequence = replica.persistence.file.sequence
            replica_clock.advance(3)
            assert await reader.call(b'GET', b'k') == Bulk(None)
            assert await reader.call(b'PTTL', b'k') == Integer(-2)
            await replica.persistence.expire_batch()
            assert replica.executor.store.physical_entry(b'k')[0] is not None
            assert replica.persistence.file.sequence == sequence
            primary_clock.advance(3)
            await primary.persistence.expire_batch()
            await replica.replication.wait_offset(primary.replication.offset)
            replica_clock.advance(-3)
            assert await reader.call(b'GET', b'k') == Bulk(None)
            assert replica.executor.store.physical_entry(b'k')[0] is None


async def test_write_arriving_during_heartbeat_is_not_consumed_by_abandoned_getter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyredis_engine.replication import node

    primary = await start_server(config(tmp_path / 'primary.aof',
        replication_heartbeat_interval=0.01))
    async with primary:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(primary.sockets[0].getsockname()[1]) as writer:
            assert primary.replication is not None and replica.replication is not None
            await replica.replication.wait_ready()
            entered, release = asyncio.Event(), asyncio.Event()
            original = node.send

            async def pause(stream: asyncio.StreamWriter, data: bytes,
                            limits: ServerConfig) -> None:
                if data.startswith(b'H') and not entered.is_set():
                    entered.set()
                    await release.wait()
                await original(stream, data, limits)

            monkeypatch.setattr(node, 'send', pause)
            try:
                await asyncio.wait_for(entered.wait(), 5)
                assert await writer.call(b'INCR', b'k') == Integer(1)
            finally:
                release.set()
            await replica.replication.wait_offset(primary.replication.offset)
            async with peer(replica.sockets[0].getsockname()[1]) as reader:
                assert await reader.call(b'GET', b'k') == Bulk(b'1')


async def test_replication_streams_snapshot_and_exec_larger_than_resp_bulk_limit(
    tmp_path: Path,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof', max_bulk_bytes=65536))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        for key in (b'a', b'b', b'c'):
            await writer.call(b'SET', key, b'x' * 60000)
        replica = await start_server(config(tmp_path / 'replica.aof', max_bulk_bytes=65536,
            replica_host='127.0.0.1', replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            assert primary.replication is not None and replica.replication is not None
            await replica.replication.wait_ready()
            assert await reader.call(b'GET', b'c') == Bulk(b'x' * 60000)
            await writer.call(b'MULTI')
            for key in (b'a', b'b'):
                await writer.call(b'SET', key, b'y' * 60000)
            await writer.call(b'EXEC')
            await replica.replication.wait_offset(primary.replication.offset)
            assert await reader.call(b'MGET', b'a', b'b') == Array(
                (Bulk(b'y' * 60000), Bulk(b'y' * 60000)))
