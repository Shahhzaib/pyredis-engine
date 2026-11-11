from __future__ import annotations

from collections import deque

import pytest
from _support import comparison, redis_reference
from _support.acceptance_cases import TTLRange, normalize
from _support.comparison import Pair

from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple


class ScriptedPeer:
    def __init__(self, *responses: Reply) -> None:
        self.responses = deque(responses)

    async def call(self, *command: bytes) -> Reply:
        return self.responses.popleft()


@pytest.mark.asyncio
@pytest.mark.parametrize("left,right", [
    (Bulk(b"1"), Integer(1)), (Simple(b"OK"), Bulk(b"OK")),
    (Error(b"ERR first"), Error(b"ERR other")),
    (Array((Bulk(b"a"), Bulk(b"b"))), Array((Bulk(b"b"), Bulk(b"a")))),
])
async def test_exact_comparison_retains_types_errors_and_order(left: Reply, right: Reply) -> None:
    pair = Pair(ScriptedPeer(left), ScriptedPeer(right), None)
    with pytest.raises(AssertionError):
        await pair.check((b"ZRANGE", b"k", b"0", b"-1"))


@pytest.mark.parametrize("command,reply", [
    ((b"SMEMBERS", b"k"), Array((Bulk(b"a"), Bulk(b"a")))),
    ((b"HGETALL", b"k"), Array((Bulk(b"a"),))),
    ((b"HGETALL", b"k"), Array((Bulk(b"a"), Bulk(b"1"), Bulk(b"a"), Bulk(b"2")))),
])
def test_unordered_comparisons_reject_incomplete_or_duplicate_content(
    command: tuple[bytes, ...], reply: Reply,
) -> None:
    with pytest.raises(AssertionError):
        normalize(reply, command)


@pytest.mark.asyncio
@pytest.mark.parametrize("actual,valid", [(59980, True), (59000, False)])
async def test_relative_ttl_budget_includes_set_execution_offset(
    monkeypatch: pytest.MonkeyPatch, actual: int, valid: bool,
) -> None:
    times = iter((0, 20_000_000, 21_000_000, 22_000_000))
    monkeypatch.setattr(comparison.time, "monotonic_ns", lambda: next(times))
    pair = Pair(ScriptedPeer(Simple(b"OK"), Integer(actual)),
                ScriptedPeer(Simple(b"OK"), Integer(60000)), None)
    await pair.check((b"SET", b"k", b"v", b"PX", b"60000"), Simple(b"OK"))
    if valid:
        await pair.check((b"PTTL", b"k"), TTLRange(60000))
    else:
        with pytest.raises(AssertionError):
            await pair.check((b"PTTL", b"k"), TTLRange(60000))


@pytest.mark.asyncio
async def test_connection_establishment_has_explicit_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    async def blocked(*args: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(redis_reference.asyncio, "open_connection", blocked)
    with pytest.raises(TimeoutError):
        await redis_reference.connect(12345, timeout=0.01)


@pytest.mark.asyncio
async def test_failed_expiry_options_preserve_original_timing_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    times = iter((0, 20_000_000, 21_000_000, 22_000_000, 23_000_000, 24_000_000))
    monkeypatch.setattr(comparison.time, "monotonic_ns", lambda: next(times))
    error = Error(b"ERR invalid expire time in 'getex' command")
    pair = Pair(ScriptedPeer(Simple(b"OK"), error, Integer(59980)),
                ScriptedPeer(Simple(b"OK"), error, Integer(60000)), None)
    await pair.check((b"SET", b"k", b"v", b"PX", b"60000"), Simple(b"OK"))
    await pair.check((b"GETEX", b"k", b"PX", b"0"), error)
    await pair.check((b"PTTL", b"k"), TTLRange(60000))
