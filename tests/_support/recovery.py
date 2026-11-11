from __future__ import annotations

import asyncio
from pathlib import Path

from pyredis_engine.config import ServerConfig
from pyredis_engine.replication.recovery import RecoveryReplication
from pyredis_engine.server import ManagedServer


def config(path: Path, **options: object) -> ServerConfig:
    return ServerConfig(port=0, aof_path=str(path), snapshot_enabled=True,
        aof_fsync='always', replication_enabled=True, replication_recovery=True, **options)


def node(server: ManagedServer) -> RecoveryReplication:
    result = server.replication
    assert isinstance(result, RecoveryReplication)
    return result


async def until_phase(server: ManagedServer, phase: str) -> None:
    state = node(server)
    async with asyncio.timeout(5):
        while state.phase != phase:
            state.changed.clear()
            await state.changed.wait()


class Partition:
    """Hold one failed connection at its retry sleep, with no real-time assertions."""
    def __init__(self, state: RecoveryReplication) -> None:
        self.state = state
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.original = state.retry_wait
        self.delays: list[float] = []

    async def wait(self, delay: float) -> None:
        self.delays.append(delay)
        self.entered.set()
        await self.release.wait()

    async def __aenter__(self) -> Partition:
        self.state.retry_wait = self.wait
        assert self.state.upstream is not None
        self.state.upstream.transport.abort()
        await asyncio.wait_for(self.entered.wait(), 5)
        return self

    async def __aexit__(self, *args: object) -> None:
        self.state.retry_wait = self.original
        self.release.set()
