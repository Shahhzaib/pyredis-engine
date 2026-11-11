from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.persistence.errors import CapacityError
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.persistence]


@pytest.mark.parametrize("policy", ["none", "everysec", "always"])
async def test_acknowledgment_and_graceful_sync_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: str,
) -> None:
    async def dormant(interval: float) -> None:
        await asyncio.Event().wait()

    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"),
                                            aof_fsync=policy), aof_sync_wait=dormant)
    owner = server.persistence
    assert owner is not None
    calls = []
    original = owner.file.sync

    def sync() -> None:
        calls.append(owner.file.sequence)
        original()

    monkeypatch.setattr(owner.file, "sync", sync)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"INCR", b"k") == Integer(1)
        assert calls == ([1] if policy == "always" else [])
    assert calls == ([] if policy == "none" else [1])
    assert owner.closed
    assert not any(thread.name.startswith("pyredis-aof") for thread in threading.enumerate())


async def test_always_waits_for_fsync_without_blocking_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"),
                                            aof_fsync="always"))
    owner = server.persistence
    assert owner is not None
    entered, release = threading.Event(), threading.Event()
    original = owner.file.sync

    def blocked() -> None:
        entered.set()
        assert release.wait(10)
        original()

    monkeypatch.setattr(owner.file, "sync", blocked)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        pending = asyncio.create_task(connection.call(b"INCR", b"k"))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            alive = asyncio.Event()
            asyncio.get_running_loop().call_soon(alive.set)
            await asyncio.wait_for(alive.wait(), 1)
            assert not pending.done()
        finally:
            release.set()
        assert await pending == Integer(1)


async def test_everysec_sync_uses_injectable_scheduler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    wake, synced = asyncio.Event(), asyncio.Event()

    async def wait(interval: float) -> None:
        await wake.wait()
        wake.clear()

    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof")),
                                aof_sync_wait=wait)
    owner = server.persistence
    assert owner is not None
    original = owner.file.sync
    loop = asyncio.get_running_loop()

    def sync() -> None:
        original()
        loop.call_soon_threadsafe(synced.set)

    monkeypatch.setattr(owner.file, "sync", sync)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"SET", b"k", b"v") == Simple(b"OK")
        assert owner.file.sequence == 1 and owner.file.synced_sequence == 0
        wake.set()
        await asyncio.wait_for(synced.wait(), 5)
        assert owner.file.synced_sequence == 1


async def test_active_expiration_is_journaled_without_key_reads(tmp_path: Path) -> None:
    clock, wake, done = ManualClock(1000), asyncio.Event(), asyncio.Event()

    async def wait(interval: float) -> None:
        await wake.wait()
        wake.clear()

    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    server = await start_server(config, clock=clock, expiry_wait=wait)
    owner = server.persistence
    assert owner is not None
    original = owner.expire_batch

    async def observed() -> int:
        removed = await original()
        done.set()
        return removed

    owner.expire_batch = observed  # type: ignore[method-assign]
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        await connection.call(b"SET", b"k", b"v", b"PX", b"1")
        clock.advance(2)
        assert server.executor.store.physical_count == 1
        wake.set()
        await asyncio.wait_for(done.wait(), 5)
        assert owner.file.sequence == 2 and server.executor.store.physical_count == 0
    restarted = await start_server(config, clock=ManualClock(0))
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"k") == Bulk(None)


async def test_capacity_rejects_exec_before_effects_and_keeps_connection_usable(
    tmp_path: Path,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_max_keys=1,
                          aof_max_record_bytes=1024)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"SET", b"k", b"v") == Simple(b"OK")
        await connection.call(b"WATCH", b"k")
        replies = await connection.pipeline([
            (b"MULTI",), (b"SET", b"new", b"v"), (b"EXEC",)])
        assert isinstance(replies[-1], Error) and replies[-1].payload.startswith(b"EXECABORT ")
        assert server.executor.watched_key_count == 0
        assert await connection.call(b"GET", b"k") == Bulk(b"v")
        assert await connection.call(b"GET", b"new") == Bulk(None)
        assert isinstance(await connection.call(b"SET", b"k", b"v" * 800), Error)
        assert await connection.call(b"GET", b"k") == Bulk(b"v")
        assert await connection.call(b"DEL", b"k") == Integer(1)
        assert await connection.call(b"SET", b"new", b"v") == Simple(b"OK")
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"new") == Bulk(b"v")


async def test_recovery_limit_failure_preserves_file_and_releases_lock(tmp_path: Path) -> None:
    path = tmp_path / "journal.aof"
    config = ServerConfig(port=0, aof_path=str(path))
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        await connection.call(b"RPUSH", b"list", b"a", b"b", b"c")
    original = path.read_bytes()
    for limited in (ServerConfig(port=0, aof_path=str(path), max_collection_items=2),
                    ServerConfig(port=0, aof_path=str(path), aof_max_dataset_bytes=1)):
        with pytest.raises(CapacityError):
            await start_server(limited)
        assert path.read_bytes() == original
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"LLEN", b"list") == Integer(3)


async def test_100_clients_commit_order_replays_exactly(tmp_path: Path) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    server = await start_server(config)
    barrier = asyncio.Barrier(100)
    async with server:
        port = server.sockets[0].getsockname()[1]

        async def worker() -> int:
            async with peer(port) as connection:
                await asyncio.wait_for(barrier.wait(), 10)
                reply = await connection.call(b"INCR", b"k")
                assert isinstance(reply, Integer)
                return reply.payload

        replies = await asyncio.wait_for(asyncio.gather(*(worker() for _ in range(100))), 30)
        assert sorted(replies) == list(range(1, 101))
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"k") == Bulk(b"100")


async def test_expiry_of_large_key_after_reducing_record_limit(tmp_path: Path) -> None:
    path = tmp_path / "journal.aof"
    clock = ManualClock(1000)
    key = b"k" * 1000
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_max_record_bytes=4096),
                                clock=clock)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"SET", key, b"v", b"PX", b"1") == Simple(b"OK")
    clock.advance(2)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_max_record_bytes=1024),
                                clock=clock)
    async with server:
        assert server.persistence is not None
        assert await server.persistence.expire_batch() == 1
        assert server.executor.store.physical_count == 0


async def test_many_expirations_split_into_bounded_effect_groups(tmp_path: Path) -> None:
    async def dormant(interval: float) -> None:
        await asyncio.Event().wait()

    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"),
                          aof_max_record_bytes=1024, aof_fsync="none")
    clock = ManualClock(1000)
    server = await start_server(config, clock=clock, expiry_wait=dormant)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        replies = await connection.pipeline([
            (b"SET", f"key{n}".encode(), b"v", b"PX", b"1") for n in range(150)])
        assert replies == [Simple(b"OK")] * 150
        clock.advance(2)
        assert server.persistence is not None
        batches = []
        while server.executor.store.physical_count:
            removed = await server.persistence.expire_batch()
            assert 0 < removed <= config.expiry_batch_size
            batches.append(removed)
        assert sum(batches) == 150 and len(batches) > 1
    server = await start_server(config, clock=ManualClock(0))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"DBSIZE") == Integer(0)


async def test_listener_bind_failure_closes_journal_worker_and_lock(tmp_path: Path) -> None:
    first = await start_server(ServerConfig(port=0))
    path = tmp_path / "journal.aof"
    async with first:
        port = first.sockets[0].getsockname()[1]
        with pytest.raises(OSError):
            await start_server(ServerConfig(port=port, aof_path=str(path)))
        assert not any(thread.name.startswith("pyredis-aof") for thread in threading.enumerate())
    second = await start_server(ServerConfig(port=0, aof_path=str(path)))
    async with second, peer(second.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"PING") == Simple(b"PONG")


async def test_shrinking_mset_fits_postimage_budget_without_copying_old_values(
    tmp_path: Path,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_max_record_bytes=1024)
    server = await start_server(config)
    keys = [f"k{n}".encode() for n in range(8)]
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        for key in keys:
            assert await connection.call(b"SET", key, b"v" * 100) == Simple(b"OK")
        arguments = tuple(part for key in keys for part in (key, b"x"))
        assert await connection.call(b"MSET", *arguments) == Simple(b"OK")
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        reply = await connection.call(b"MGET", *keys)
        assert reply.payload == tuple(Bulk(b"x") for _ in keys)


async def test_100_exec_groups_have_atomic_replies_and_recover_in_commit_order(
    tmp_path: Path,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    server = await start_server(config)
    barrier = asyncio.Barrier(100)
    async with server:
        port = server.sockets[0].getsockname()[1]

        async def transaction() -> int:
            async with peer(port) as connection:
                await asyncio.wait_for(barrier.wait(), 10)
                replies = await connection.pipeline([
                    (b"MULTI",), (b"INCR", b"k"), (b"INCR", b"k"), (b"EXEC",)])
                batch = replies[-1]
                assert isinstance(batch, Array) and batch.payload is not None
                first, second = batch.payload
                assert isinstance(first, Integer) and isinstance(second, Integer)
                assert second.payload == first.payload + 1
                return first.payload

        results = await asyncio.wait_for(
            asyncio.gather(*(transaction() for _ in range(100))), 30)
        assert sorted(results) == list(range(1, 200, 2))
        assert server.persistence is not None and server.persistence.file.sequence == 100
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"k") == Bulk(b"200")
