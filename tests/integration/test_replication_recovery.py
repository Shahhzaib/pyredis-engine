from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from _support.acceptance_cases import normalize
from _support.comparison import peer
from _support.recovery import Partition, config, node

from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer
from pyredis_engine.replication.recovery import RecoverySession
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.replication]


@pytest.mark.parametrize('evict', [False, True])
async def test_partition_partial_or_full_sync_preserves_transaction_state(
    tmp_path: Path, evict: bool,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof',
        replication_backlog_bytes=1 if evict else 1024 * 1024))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'binary', b'\x00\xff')
        await node(primary).wait_offset(node(primary).offset)
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            assert state.full_syncs == 1
            async with Partition(state):
                blocked = await reader.call(b'GET', b'binary')
                assert isinstance(blocked, Error) and blocked.payload.startswith(b'MASTERDOWN ')
                replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'a'),
                    (b'INCR', b'b'), (b'EXEC',)])
                assert replies[-1] == Array((Integer(1), Integer(1)))
            await state.wait_offset(node(primary).offset)
            assert state.full_syncs == (2 if evict else 1)
            assert state.partial_syncs == (0 if evict else 1)
            assert await reader.call(b'MGET', b'a', b'b', b'binary') == Array(
                (Bulk(b'1'), Bulk(b'1'), Bulk(b'\x00\xff')))
            assert state.offset == node(primary).offset and state.proof == node(primary).proof


async def test_primary_restart_changes_history_and_forces_full_resync(tmp_path: Path) -> None:
    initial = config(tmp_path / 'primary.aof')
    primary = await start_server(initial)
    port = primary.sockets[0].getsockname()[1]
    replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
                                         replica_port=port))
    async with replica:
        state = node(replica)
        await state.wait_ready()
        old_id = state.replication_id
        async with Partition(state):
            primary.close()
            await primary.wait_closed()
            primary = await start_server(replace(initial, port=port))
            async with peer(port) as writer:
                await writer.call(b'SET', b'new', b'history')
        async with primary:
            await state.wait_offset(node(primary).offset)
            assert state.full_syncs == 2 and state.replication_id != old_id
            assert state.replication_id == node(primary).replication_id
            async with peer(replica.sockets[0].getsockname()[1]) as reader:
                assert await reader.call(b'GET', b'new') == Bulk(b'history')


@pytest.mark.parametrize('damage', ['id', 'offset', 'proof', 'local_state'])
async def test_incompatible_resume_claim_or_local_divergence_falls_back_safely(
    tmp_path: Path, damage: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'k', b'authoritative')
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            async with Partition(state):
                assert state.resume is not None
                if damage == 'id':
                    wrong = b'0' * 40 if state.replication_id != b'0' * 40 else b'1' * 40
                    state.resume = replace(state.resume, history_id=wrong)
                elif damage == 'offset':
                    state.resume = replace(state.resume, offset=state.offset + 1)
                elif damage == 'proof':
                    state.resume = replace(state.resume, proof=b'!' * 32)
                else:
                    assert replica.persistence is not None
                    client = replica.executor.new_client()
                    try:
                        await replica.persistence.execute([b'SET', b'rogue', b'bad'], client)
                    finally:
                        replica.executor.close_client(client)
            await state.wait_offset(node(primary).offset)
            assert state.last_sync == 'full' and state.full_syncs == 2
            assert await reader.call(b'GET', b'rogue') == Bulk(None)
            assert await reader.call(b'GET', b'k') == Bulk(b'authoritative')
            assert state.proof == node(primary).proof


async def test_two_replicas_recover_100_concurrent_transactions_and_live_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyredis_engine.replication import recovery

    async def dormant(interval: float) -> None:
        await asyncio.Event().wait()

    clock = ManualClock(1000)
    primary = await start_server(config(tmp_path / 'primary.aof'), clock=clock,
                                 expiry_wait=dormant)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'expiring', b'v', b'PX', b'2')
        await writer.call(b'SET', b'binary', b'\x00\xff')
        upstream = dict(replica_host='127.0.0.1', replica_port=primary.sockets[0].getsockname()[1])
        first = await start_server(config(tmp_path / 'first.aof', **upstream), clock=clock)
        second = await start_server(config(tmp_path / 'second.aof', **upstream), clock=clock)
        async with first, second:
            await asyncio.gather(node(first).wait_ready(), node(second).wait_ready())
            entered, release = asyncio.Event(), asyncio.Event()
            original_send = recovery.send
            resumed = []

            async def pause(stream: asyncio.StreamWriter, data: bytes, limits: object) -> None:
                if data.startswith(b'+PYREDIS-CONTINUE'):
                    resumed.append(stream)
                    if len(resumed) == 2:
                        entered.set()
                    await release.wait()
                await original_send(stream, data, limits)

            async with Partition(node(first)), Partition(node(second)):
                async def write(index: int) -> None:
                    member = str(index).encode()
                    async with peer(primary.sockets[0].getsockname()[1]) as client:
                        replies = await client.pipeline([(b'MULTI',), (b'INCR', b'a'),
                            (b'INCR', b'b'), (b'HSET', b'h', member, b'\x00\xff'),
                            (b'RPUSH', b'l', member), (b'SADD', b's', member),
                            (b'ZADD', b'z', member, member), (b'EXEC',)])
                        assert isinstance(replies[-1], Array) and replies[-1].payload is not None
                        assert len(replies[-1].payload) == 6

                assert primary.persistence is not None
                compacting = asyncio.create_task(primary.persistence.compact())
                await asyncio.gather(*(write(index) for index in range(100)))
                assert (await compacting).activated
                clock.advance(3)
                await primary.persistence.expire_batch()
                monkeypatch.setattr(recovery, 'send', pause)
            try:
                await asyncio.wait_for(entered.wait(), 5)
                # Both suffixes are subscribed; these writes arrive after the
                # admitted catch-up barrier and must follow it without a gap.
                for _ in range(20):
                    replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'a'),
                        (b'INCR', b'b'), (b'EXEC',)])
                    assert isinstance(replies[-1], Array)
            finally:
                release.set()
            target = node(primary).offset
            await asyncio.gather(node(first).wait_offset(target), node(second).wait_offset(target))
            clock.advance(-3)
            for replica in (first, second):
                assert node(replica).last_sync == 'partial'
                assert node(replica).full_syncs == 1 and node(replica).partial_syncs == 1
                assert node(replica).proof == node(primary).proof
                async with peer(replica.sockets[0].getsockname()[1]) as reader:
                    assert await reader.call(b'MGET', b'a', b'b') == Array(
                        (Bulk(b'120'), Bulk(b'120')))
                    assert await reader.call(b'GET', b'expiring') == Bulk(None)
                    assert replica.executor.store.physical_entry(b'expiring')[0] is None
                    for command in [(b'HGETALL', b'h'), (b'LRANGE', b'l', b'0', b'-1'),
                        (b'SCARD', b's'), (b'ZRANGE', b'z', b'0', b'-1', b'WITHSCORES')]:
                        actual, expected = await reader.call(*command), await writer.call(*command)
                        assert normalize(actual, command) == normalize(expected, command)
                    assert await reader.call(b'GET', b'binary') == Bulk(b'\x00\xff')


@pytest.mark.parametrize('policy,code', [('always', b'A'), ('everysec', b'E'), ('none', b'N')])
async def test_ack_policy_and_application_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: str, code: bytes,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        replica_config = replace(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]), aof_fsync=policy)
        replica = await start_server(replica_config)
        async with replica:
            await node(replica).wait_ready()
            assert replica.persistence is not None
            entered, release = threading.Event(), threading.Event()
            operation = 'sync' if policy == 'always' else 'append'
            original = getattr(replica.persistence.file, operation)

            def pause(*args: object) -> object:
                entered.set()
                assert release.wait(10)
                return original(*args)

            monkeypatch.setattr(replica.persistence.file, operation, pause)
            before = node(replica).offset
            try:
                await writer.call(b'INCR', b'k')
                assert await asyncio.to_thread(entered.wait, 5)
                assert node(replica).offset == before
                for session in node(primary).sessions:
                    assert isinstance(session, RecoverySession) and session.tracker is not None
                    assert session.tracker.acknowledged in (None, before)
            finally:
                release.set()
            await node(replica).wait_offset(node(primary).offset)
            async with asyncio.timeout(5):
                while True:
                    sessions = list(node(primary).sessions)
                    if sessions and all(isinstance(s, RecoverySession) and s.tracker is not None
                        and s.tracker.acknowledged == node(primary).offset for s in sessions):
                        break
                    node(primary).changed.clear()
                    await node(primary).changed.wait()
            assert all(isinstance(s, RecoverySession) and s.tracker is not None
                       and s.tracker.policy == code for s in node(primary).sessions)


async def test_evicted_history_full_recovery_captures_new_writes_and_compaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof', replication_backlog_bytes=1))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            assert primary.persistence is not None and primary.persistence.generations is not None
            entered, release = threading.Event(), threading.Event()

            def pause(stage: str) -> None:
                if stage == 'replication_snapshot':
                    entered.set()
                    assert release.wait(10)

            monkeypatch.setattr(primary.persistence.generations, '_stage', pause)
            async with Partition(state):
                await writer.call(b'INCR', b'k')
            compacting = None
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                compacting = asyncio.create_task(primary.persistence.compact())
                for count in range(2, 22):
                    assert await writer.call(b'INCR', b'k') == Integer(count)
                assert not compacting.done()
            finally:
                release.set()
            assert compacting is not None and (await compacting).activated
            await state.wait_offset(node(primary).offset)
            assert state.full_syncs == 2 and state.last_sync == 'full'
            assert await reader.call(b'GET', b'k') == Bulk(b'21')


async def test_replica_restart_uses_full_sync_without_unsafe_durable_resume_claim(
    tmp_path: Path,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        local = config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
                       replica_port=primary.sockets[0].getsockname()[1])
        first = await start_server(local)
        async with first:
            await node(first).wait_ready()
            await writer.call(b'INCR', b'k')
            await node(first).wait_offset(node(primary).offset)
        await writer.call(b'INCR', b'k')
        second = await start_server(local)
        async with second, peer(second.sockets[0].getsockname()[1]) as reader:
            await node(second).wait_ready()
            assert node(second).last_sync == 'full' and node(second).partial_syncs == 0
            assert await reader.call(b'GET', b'k') == Bulk(b'2')
