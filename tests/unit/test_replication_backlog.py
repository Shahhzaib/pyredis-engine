from __future__ import annotations

import random

import pytest

from pyredis_engine.replication.backlog import Backlog
from pyredis_engine.replication.wire import OVERHEAD


def test_whole_frame_boundaries_eviction_and_proof_validation() -> None:
    history = Backlog(2 * (OVERHEAD + 5), 10, b'a' * 40)
    origin = history.proof
    first = history.append(b'first')
    first_proof = history.proof
    second = history.append(b'other')
    assert history.resume(0, origin) == (first, second)
    third = history.append(b'last!')
    assert history.resume(0, origin) is None
    assert history.resume(len(first), first_proof) == (second, third)
    assert history.resume(len(first) + 1, first_proof) is None
    assert history.resume(history.offset, b'x' * 32) is None
    assert history.resume(history.offset, history.proof) == ()
    assert history.bytes <= history.max_bytes and len(history.records) <= history.max_frames


@pytest.mark.parametrize('max_bytes,max_frames', [(1, 10), (10000, 1)])
def test_too_large_or_count_evicted_history_keeps_only_safe_boundaries(
    max_bytes: int, max_frames: int,
) -> None:
    history = Backlog(max_bytes, max_frames, b'a' * 40)
    history.append(b'first')
    history.append(b'other')
    assert history.bytes <= max_bytes and len(history.records) <= max_frames
    assert history.resume(history.offset, history.proof) == ()
    assert history.resume(0, b'\x00' * 32) is None


def test_seeded_history_model_retains_exact_bounded_suffixes() -> None:
    rng = random.Random(10)
    history = Backlog(1000, 8, b'a' * 40)
    expected = []
    proofs = {0: history.proof}
    for _ in range(200):
        start = history.offset
        frame = history.append(rng.randbytes(rng.randrange(5, 400)))
        expected.append((start, history.offset, frame))
        proofs[history.offset] = history.proof
        while sum(len(item[2]) for item in expected) > 1000 or len(expected) > 8:
            expected.pop(0)
        assert history.bytes == sum(len(item[2]) for item in expected)
        assert len(history.boundaries) <= len(history.records) + 1
        for offset in history.boundaries:
            assert history.resume(offset, proofs[offset]) == tuple(
                item[2] for item in expected if item[0] >= offset)
