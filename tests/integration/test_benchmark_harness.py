from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pyredis_engine.protocol import Simple
from scripts.benchmarks.client import Child, Peer
from scripts.benchmarks.runner import Settings, measure_row
from scripts.benchmarks.workloads import Case, initialize, verify

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_all_command_benchmarks_count_samples_and_validate_state() -> None:
    async with Child({}) as child:
        peer = await Peer.open(child.port)
        try:
            for command in ('GET', 'SET', 'INCR', 'HSET', 'RPUSH', 'SADD', 'ZADD', 'ZRANGE'):
                assert await peer.call(b'FLUSHDB') == Simple(b'OK')
                result = await measure_row(child.port, Case(command, 2, 4),
                    Settings(samples=2, operations=2, warmup=1, minimum_seconds=0), child)
                assert result['correctness'] == 'passed' and result['operations'] == 8
                assert len(result['samples']) == 2 and result['latency']['count'] == 8
                assert result['final']['memory']['rss_bytes'] > 0
                assert result['final']['metrics']['memory_accounted_bytes'] > 0
            # A corrupted final dataset cannot be accepted just because timings succeeded.
            await peer.call(b'FLUSHDB')
            case = Case('GET', 1, 4)
            await initialize(peer, case)
            await peer.call(b'SET', b'k:000000', b'wrong')
            with pytest.raises(AssertionError):
                await verify(peer, case, [0])
        finally:
            await peer.close()
    assert child.process is not None and child.process.returncode == 0


async def test_failed_worker_exit_cannot_be_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    child = Child({})
    child.process = SimpleNamespace(returncode=7)
    monkeypatch.setattr(child, 'close', AsyncMock())
    with pytest.raises(RuntimeError, match='exit 7'):
        await child.__aexit__(None, None, None)


async def test_cancellation_reaps_owned_benchmark_worker() -> None:
    started = asyncio.Event()
    child = Child({})
    async def wait() -> None:
        async with child:
            started.set()
            await asyncio.Event().wait()
    task = asyncio.create_task(wait())
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert child.process is not None and child.process.returncode == 0
