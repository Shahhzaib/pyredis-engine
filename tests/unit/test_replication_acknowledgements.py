from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.replication.backlog import Backlog
from pyredis_engine.replication.recovery_wire import (
    AckTracker,
    ResumeRequest,
    acknowledgement,
    continuation,
    full_greeting,
    parse_continuation,
    parse_full,
    receive_ack,
)
from pyredis_engine.replication.wire import HEADER, ReplicationError, SnapshotInfo, packet, receive

HISTORY = b'a' * 40


@pytest.mark.parametrize('policy,code', [('always', b'A'), ('everysec', b'E'), ('none', b'N')])
def test_ack_monotonic_exact_boundary_and_policy(policy: str, code: bytes) -> None:
    log = Backlog(10000, 10, HISTORY)
    tracker = AckTracker(HISTORY, 0, log.proof, 2)
    tracker.accept(0, HISTORY + log.proof + code)
    first = log.append(b'first')
    tracker.offer(first)
    tracker.accept(log.offset, HISTORY + log.proof + code)
    tracker.accept(log.offset, HISTORY + log.proof + code)  # duplicate heartbeat ACK
    assert tracker.acknowledged == len(first) and tracker.policy == code
    with pytest.raises(ReplicationError):
        tracker.accept(0, HISTORY + log.proof + code)
    assert tracker.acknowledged == len(first)


@pytest.mark.parametrize('damage', ['history', 'ahead', 'middle', 'proof', 'policy', 'length',
                                   'changed_policy'])
def test_invalid_ack_does_not_change_progress(damage: str) -> None:
    log = Backlog(10000, 10, HISTORY)
    tracker = AckTracker(HISTORY, 0, log.proof, 10)
    tracker.accept(0, HISTORY + log.proof + b'A')
    tracker.offer(log.append(b'first'))
    offset, payload = log.offset, HISTORY + log.proof + b'A'
    if damage == 'history':
        payload = b'b' * 40 + payload[40:]
    elif damage == 'ahead':
        offset += 1
    elif damage == 'middle':
        offset -= 1
    elif damage == 'proof':
        payload = HISTORY + b'!' * 32 + b'A'
    elif damage in ('policy', 'changed_policy'):
        payload = payload[:-1] + (b'X' if damage == 'policy' else b'E')
    else:
        payload = payload[:-1]
    with pytest.raises(ReplicationError):
        tracker.accept(offset, payload)
    assert tracker.acknowledged == 0 and tracker.policy == b'A'


def test_unacknowledged_boundaries_have_a_hard_count_cap() -> None:
    log = Backlog(10000, 10, HISTORY)
    tracker = AckTracker(HISTORY, 0, log.proof, 2)
    tracker.accept(0, HISTORY + log.proof + b'A')
    tracker.offer(log.append(b'first'))
    tracker.offer(log.append(b'other'))
    with pytest.raises(ReplicationError, match='limit'):
        tracker.offer(log.append(b'last!'))
    assert len(tracker.pending) == 3  # two pending plus acknowledged anchor


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['kind', 'length', 'checksum', 'truncated', 'good'])
async def test_ack_frame_validation_and_fragmentation(damage: str) -> None:
    data = acknowledgement(HISTORY, 123, b'p' * 32, 'always')
    if damage == 'kind':
        data = packet(b'E', 0, b'abcde')
    elif damage == 'length':
        data = HEADER.pack(b'A', 123, 2**32 - 1)
    elif damage == 'checksum':
        data = data[:-1] + bytes((data[-1] ^ 1,))
    elif damage == 'truncated':
        data = data[:-1]
    reader = asyncio.StreamReader()
    for byte in data:
        reader.feed_data(bytes((byte,)))
    reader.feed_eof()
    if damage == 'good':
        assert await receive_ack(reader, ServerConfig()) == (123, HISTORY + b'p' * 32 + b'A')
    else:
        with pytest.raises((ReplicationError, ConnectionError)):
            await receive_ack(reader, ServerConfig())


@pytest.mark.asyncio
async def test_stalled_ack_read_times_out() -> None:
    with pytest.raises(TimeoutError):
        await receive_ack(asyncio.StreamReader(), replace(ServerConfig(),
            replication_ack_timeout=0.01, replication_heartbeat_interval=0.001))


@pytest.mark.parametrize('offset,proof', [(b'-1', b'?'), (b'1.0', b'a' * 64),
    (str(2**64).encode(), b'a' * 64), (b'1', b'X' * 64)])
def test_invalid_resume_values_request_full_sync(offset: bytes, proof: bytes) -> None:
    assert ResumeRequest.decode([b'PYREDIS.SYNC', b'2', HISTORY, offset, proof]) is None


def test_v2_metadata_and_exact_continuation_validation() -> None:
    info = SnapshotInfo(HISTORY, 100, b's' * 16, b'g' * 16, 9, 1000)
    proof = b'p' * 32
    assert parse_full(full_greeting(info, proof, 200), ServerConfig()) == (info, proof, 200)
    request = ResumeRequest(HISTORY, 100, proof)
    assert parse_continuation(continuation(HISTORY, 100, proof, 200), request) == 200
    with pytest.raises(ReplicationError):
        parse_continuation(continuation(HISTORY, 101, proof, 200), request)
    with pytest.raises(ReplicationError):
        parse_full(full_greeting(info, proof, 99), ServerConfig())


@pytest.mark.asyncio
async def test_v2_heartbeat_and_barrier_do_not_advance_offsets() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(packet(b'H', 10, b'p' * 32) + packet(b'R', 10, b'p' * 32))
    assert await receive(reader, ServerConfig(), 10, version=2) == (b'H', 10, b'p' * 32)
    assert await receive(reader, ServerConfig(), 10, version=2) == (b'R', 10, b'p' * 32)


@pytest.mark.parametrize('options', [dict(replication_recovery=1), dict(replication_recovery=True),
    dict(replication_backlog_bytes=0), dict(replication_backlog_frames=False),
    dict(replication_retry_attempts=0), dict(replication_retry_attempts=1025),
    dict(replication_retry_delay=0), dict(replication_retry_max_delay=0.01),
    dict(replication_retry_reset_after=float('nan')), dict(replication_ack_timeout=1),
    dict(replication_ack_max_pending=0), dict(replication_catchup_timeout=float('inf'))])
def test_recovery_configuration_limits(options: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ServerConfig(**options)
