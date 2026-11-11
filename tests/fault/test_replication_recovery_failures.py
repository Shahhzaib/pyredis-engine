from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.recovery import Partition, config, node

from pyredis_engine.protocol import Bulk, Integer, Simple, read_command
from pyredis_engine.replication.backlog import advance
from pyredis_engine.replication.recovery import RecoverySession
from pyredis_engine.replication.recovery_wire import acknowledgement, parse_full
from pyredis_engine.replication.wire import HEADER, ReplicationError, packet, receive
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.replication]


@pytest.mark.parametrize('damage', ['history', 'ahead', 'proof', 'policy', 'length',
                                   'checksum', 'stale'])
async def test_bad_ack_closes_only_peer_and_never_regresses_progress(
    tmp_path: Path, damage: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as replica:
        greeting = await replica.call(b'PYREDIS.SYNC', b'2', b'?', b'-1', b'?')
        assert isinstance(greeting, Simple)
        info, proof, target = parse_full(greeting.payload, primary._config)
        await replica.reader.readexactly(info.size)
        assert await receive(replica.reader, primary._config, info.offset, version=2) == (
            b'R', target, proof)
        sessions = list(node(primary).sessions)
        assert len(sessions) == 1 and isinstance(sessions[0], RecoverySession)
        session = sessions[0]
        assert session.tracker is not None and primary.persistence is not None
        offset = info.offset
        good = acknowledgement(info.replication_id, offset, proof, 'always')
        replica.writer.write(good)
        await replica.writer.drain()
        async with asyncio.timeout(5):
            while session.tracker.acknowledged != offset:
                node(primary).changed.clear()
                await node(primary).changed.wait()
        if damage == 'stale':
            async with peer(primary.sockets[0].getsockname()[1]) as writer:
                await writer.call(b'INCR', b'k')
            kind, end, body = await receive(replica.reader, primary._config, offset, version=2)
            assert kind == b'E'
            next_proof = advance(proof, packet(kind, end, body)[-32:])
            replica.writer.write(acknowledgement(info.replication_id, end, next_proof, 'always'))
            await replica.writer.drain()
            async with asyncio.timeout(5):
                while session.tracker.acknowledged != end:
                    node(primary).changed.clear()
                    await node(primary).changed.wait()
            good_offset = end
            bad = good
        else:
            good_offset = offset
            history = b'b' * 40 if damage == 'history' else info.replication_id
            wrong_proof = b'!' * 32 if damage == 'proof' else proof
            bad = acknowledgement(history, offset + (1 if damage == 'ahead' else 0),
                                  wrong_proof, 'none' if damage == 'policy' else 'always')
            if damage == 'length':
                bad = HEADER.pack(b'A', offset, 2**32 - 1)
            elif damage == 'checksum':
                bad = bad[:-1] + bytes((bad[-1] ^ 1,))
        sequence = primary.persistence.file.sequence
        replica.writer.write(bad)
        await replica.writer.drain()
        await asyncio.wait_for(replica.reader.read(), 5)
        assert session.tracker.acknowledged == good_offset
        assert primary.persistence.file.sequence == sequence and primary.is_serving()
        async with peer(primary.sockets[0].getsockname()[1]) as writer:
            assert await writer.call(b'PING') == Simple(b'PONG')


@pytest.mark.parametrize('damage', ['duplicate', 'gap', 'reorder', 'schema', 'proof'])
async def test_stream_divergence_discards_resume_and_recovers_with_full_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str,
) -> None:
    from pyredis_engine.replication import node as node_module

    primary = await start_server(config(tmp_path / 'primary.aof',
                                         replication_heartbeat_interval=0.01))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica:
            state = node(replica)
            await state.wait_ready()
            entered, release, first_seen = asyncio.Event(), asyncio.Event(), asyncio.Event()

            async def hold(delay: float) -> None:
                entered.set()
                await release.wait()

            state.retry_wait = hold
            original = node_module.send
            saved = []
            corrupted = False

            async def corrupt(stream: asyncio.StreamWriter, data: bytes, limits: object) -> None:
                nonlocal corrupted
                desired = b'H' if damage == 'proof' else b'E'
                if data[:1] != desired or corrupted:
                    await original(stream, data, limits)
                    return
                kind, end, length = HEADER.unpack_from(data)
                body = data[HEADER.size:HEADER.size + length]
                if damage == 'reorder' and not saved:
                    saved.append(data)
                    first_seen.set()
                    return
                corrupted = True
                if damage == 'duplicate':
                    await original(stream, data + data, limits)
                elif damage == 'gap':
                    await original(stream, packet(kind, end + 1, body), limits)
                elif damage == 'reorder':
                    await original(stream, data + saved[0], limits)
                else:
                    await original(stream, packet(kind, end, b'\xff' * length), limits)

            monkeypatch.setattr(node_module, 'send', corrupt)
            try:
                await writer.call(b'INCR', b'k')
                if damage == 'reorder':
                    await asyncio.wait_for(first_seen.wait(), 5)
                await writer.call(b'INCR', b'k')
                await asyncio.wait_for(entered.wait(), 5)
                assert state.resume is None and corrupted
            finally:
                release.set()
            await state.wait_offset(node(primary).offset)
            assert state.full_syncs == 2 and state.last_sync == 'full'
            async with peer(replica.sockets[0].getsockname()[1]) as reader:
                assert await reader.call(b'GET', b'k') == Bulk(b'2')
            assert state.proof == node(primary).proof


async def test_retry_budget_backoff_cap_and_cancellation(tmp_path: Path) -> None:
    completed = []

    async def refuse(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await read_command(reader, config(tmp_path / 'unused'))
            writer.write(b'-ERR unavailable\r\n')
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            completed.append(True)

    fake = await asyncio.start_server(refuse, '127.0.0.1', 0)
    async with fake:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=fake.sockets[0].getsockname()[1], replication_retry_attempts=5,
            replication_retry_delay=0.125, replication_retry_max_delay=0.5))
        async with replica:
            state = node(replica)
            delays = []

            async def observe(delay: float) -> None:
                delays.append(delay)

            state.retry_wait = observe
            assert state.task is not None
            await asyncio.wait_for(state.task, 5)
            assert delays == [0.125, 0.25, 0.5, 0.5]
            assert state.attempts_total == 5 and state.phase == 'down'
            with pytest.raises(ReplicationError):
                await state.wait_ready()
        assert len(completed) == 5
        cancelled = await start_server(config(tmp_path / 'cancelled.aof', replica_host='127.0.0.1',
            replica_port=fake.sockets[0].getsockname()[1]))
        entered = asyncio.Event()

        async def forever(delay: float) -> None:
            entered.set()
            await asyncio.Event().wait()

        node(cancelled).retry_wait = forever
        await asyncio.wait_for(entered.wait(), 5)
        cancelled.close()
        await asyncio.wait_for(cancelled.wait_closed(), 5)
        assert node(cancelled).task is not None and node(cancelled).task.done()
    assert not any(t.name.startswith(('pyredis-aof', 'pyredis-snapshot'))
                   for t in threading.enumerate())


@pytest.mark.parametrize('reason', ['timeout', 'pending_cap'])
async def test_nonacknowledging_peer_is_disconnected_with_bounded_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reason: str,
) -> None:
    from pyredis_engine.replication import recovery

    deadline = asyncio.Event()
    if reason == 'timeout':
        async def expire(reader: asyncio.StreamReader, limits: object) -> tuple[int, bytes]:
            await deadline.wait()
            raise TimeoutError('injected ACK deadline')

        monkeypatch.setattr(recovery, 'receive_ack', expire)
    primary = await start_server(config(tmp_path / 'primary.aof', replication_ack_max_pending=1))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as replica:
        reply = await replica.call(b'PYREDIS.SYNC', b'2', b'?', b'-1', b'?')
        assert isinstance(reply, Simple)
        info, proof, target = parse_full(reply.payload, primary._config)
        await replica.reader.readexactly(info.size)
        await receive(replica.reader, primary._config, info.offset, version=2)
        session = next(iter(node(primary).sessions))
        assert isinstance(session, RecoverySession) and session.tracker is not None
        if reason == 'timeout':
            deadline.set()
        else:
            async with peer(primary.sockets[0].getsockname()[1]) as writer:
                await writer.call(b'INCR', b'k')
        await asyncio.wait_for(replica.reader.read(), 5)
        assert session.tracker.acknowledged is None and len(session.tracker.pending) <= 1
        assert primary.is_serving()


async def test_brief_handshakes_do_not_reset_budget_but_healthy_interval_does(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyredis_engine.replication.wire import ReplicationEOF

    replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
        replica_port=1, replication_retry_attempts=3))
    state = node(replica)
    state.retry_failures = 2
    calls = 0

    async def connection() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            state.ready_since = (asyncio.get_running_loop().time()
                                 - state.config.replication_retry_reset_after - 1)
        raise ReplicationEOF('injected disconnect')

    delays = []

    async def observe(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(state, '_connection', connection)
    state.retry_wait = observe
    async with replica:
        assert state.task is not None
        await asyncio.wait_for(state.task, 5)
        assert calls == 3 and len(delays) == 2 and state.retry_failures == 3


async def test_heartbeat_without_catchup_barrier_has_a_finite_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof',
                                         replication_heartbeat_interval=0.001))
    async with primary:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1], replication_retry_attempts=2))
        async with replica:
            state = node(replica)
            await state.wait_ready()
            async with Partition(state):
                # Initial installation is already verified; inject a stalled
                # partial handshake that continues sending valid heartbeats.
                state.config = replace(state.config, replication_catchup_timeout=0.01)
                monkeypatch.setattr(node(primary), '_barrier', lambda session: node(primary).offset)
            assert state.task is not None
            await asyncio.wait_for(state.task, 5)
            assert state.phase == 'down' and state.partial_syncs == 0
            assert state.retry_failures == 2


async def test_effect_continuity_guard_rejects_before_any_state_or_journal_mutation(
    tmp_path: Path,
) -> None:
    from pyredis_engine.engine.store import StringValue
    from pyredis_engine.persistence.codec import Entry, Group
    from pyredis_engine.persistence.errors import ReplicaDivergence

    server = await start_server(config(tmp_path / 'engine.aof'))
    async with server, peer(server.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'k', b'good')
        owner = server.persistence
        assert owner is not None
        before = owner.file.sequence, server.executor.store.mutation_revision, owner.dataset.bytes
        with pytest.raises(ReplicaDivergence):
            await owner.apply_effect(Group(False, (Entry(b'k', StringValue(b'bad'), None),)),
                                     boundary=(before[0], before[1] - 1))
        assert owner.failure is None
        assert (owner.file.sequence, server.executor.store.mutation_revision,
                owner.dataset.bytes) == before
        assert await writer.call(b'GET', b'k') == Bulk(b'good')


@pytest.mark.parametrize('limit', ['bytes', 'frames'])
async def test_full_sync_queue_exhaustion_retries_cleanly_after_write_burst(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof',
        replication_queue_bytes=200 if limit == 'bytes' else 1024 * 1024,
        replication_queue_frames=2 if limit == 'frames' else 4096))
    assert primary.persistence is not None and primary.persistence.generations is not None
    entered, release = threading.Event(), threading.Event()
    retrying, reconnect = asyncio.Event(), asyncio.Event()
    calls = 0

    def pause(stage: str) -> None:
        nonlocal calls
        if stage == 'replication_snapshot':
            calls += 1
            if calls == 1:
                entered.set()
                assert release.wait(10)

    monkeypatch.setattr(primary.persistence.generations, '_stage', pause)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))

        async def hold(delay: float) -> None:
            retrying.set()
            await reconnect.wait()

        state = node(replica)
        state.retry_wait = hold
        async with replica:
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                for count in range(1, 11):
                    assert await writer.call(b'INCR', b'k') == Integer(count)
                await asyncio.wait_for(retrying.wait(), 5)
                assert primary.is_serving() and state.resume is None
            finally:
                release.set()
                reconnect.set()
            await state.wait_offset(node(primary).offset)
            assert state.full_syncs == 1 and state.attempts_total == 2
            async with peer(replica.sockets[0].getsockname()[1]) as reader:
                assert await reader.call(b'GET', b'k') == Bulk(b'10')
    assert not list(tmp_path.glob('.pyredis-sync-*'))
