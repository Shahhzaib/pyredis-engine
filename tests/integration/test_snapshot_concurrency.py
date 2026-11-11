from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.persistence.errors import JournalError
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.persistence]


async def test_snapshot_pins_prefix_while_100_clients_commit_exec_and_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True)
    clock = ManualClock(1000)
    server = await start_server(config, clock=clock)
    owner = server.persistence
    assert owner is not None and owner.generations is not None
    entered, release = threading.Event(), threading.Event()
    main_thread = threading.get_ident()

    def pause(stage: str) -> None:
        if stage == 'snapshot_write':
            assert threading.get_ident() != main_thread
            entered.set()
            assert release.wait(15)

    monkeypatch.setattr(owner.generations, '_stage', pause)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'SET', b'stable', b'v')
        await client.call(b'WATCH', b'stable')
        for name in (b'passive', b'active'):
            await client.call(b'SET', name, b'v', b'PX', b'1')
        pending = asyncio.create_task(owner.compact())
        try:
            assert await asyncio.to_thread(entered.wait, 5)

            async def write(index: int) -> None:
                key = str(index).encode()
                async with peer(server.sockets[0].getsockname()[1]) as writer:
                    replies = await writer.pipeline([
                        (b'MULTI',), (b'INCR', b'counter'), (b'INCR', b'paired'),
                        (b'HSET', b'h', key, b'\x00\xff'), (b'RPUSH', b'l', key),
                        (b'SADD', b's', key), (b'ZADD', b'z', key, key), (b'EXEC',)])
                    assert isinstance(replies[-1], Array)
                    assert replies[-1].payload is not None and len(replies[-1].payload) == 6

            await asyncio.gather(*(write(i) for i in range(100)))
            clock.advance(2)
            assert await client.call(b'GET', b'passive') == Bulk(None)
            await owner.expire_batch()
        finally:
            release.set()
        result = await pending
        assert result.activated and result.cutover_sequence > result.base_sequence
        assert await client.call(b'MULTI') == Simple(b'OK')
        assert await client.call(b'GET', b'stable') == Simple(b'QUEUED')
        assert await client.call(b'EXEC') == Array((Bulk(b'v'),))
        assert await client.call(b'INCR', b'counter') == Integer(101)
        expected_list = await client.call(b'LRANGE', b'l', b'0', b'-1')
        expected_zset = await client.call(b'ZRANGE', b'z', b'0', b'-1', b'WITHSCORES')
        sequence = owner.file.sequence
    # A clock rollback must not resurrect the committed expiration effects.
    clock = ManualClock(1000)
    server = await start_server(config, clock=clock)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert server.persistence is not None and server.persistence.file.sequence == sequence
        assert await client.call(b'MGET', b'counter', b'paired') == Array(
            (Bulk(b'101'), Bulk(b'100')))
        assert await client.call(b'MGET', b'active', b'passive') == Array((Bulk(None), Bulk(None)))
        assert await client.call(b'HLEN', b'h') == Integer(100)
        assert await client.call(b'SCARD', b's') == Integer(100)
        assert await client.call(b'LLEN', b'l') == Integer(100)
        assert await client.call(b'LRANGE', b'l', b'0', b'-1') == expected_list
        assert await client.call(b'ZRANGE', b'z', b'0', b'-1', b'WITHSCORES') == expected_zset


async def test_cancellation_and_shutdown_drain_snapshot_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'), snapshot_enabled=True)
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None and owner.generations is not None
    entered, release = threading.Event(), threading.Event()

    def pause(stage: str) -> None:
        if stage == 'snapshot_write':
            entered.set()
            assert release.wait(10)

    monkeypatch.setattr(owner.generations, '_stage', pause)
    async with peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')
    pending = asyncio.create_task(owner.compact())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        with pytest.raises(JournalError, match='already running'):
            await owner.compact()
        pending.cancel()
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done()
        server.close()
        closing = asyncio.create_task(server.wait_closed())
        await asyncio.sleep(0)
        assert not closing.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await closing
    assert owner.closed and not owner.pending
    assert not any(t.name.startswith(('pyredis-aof', 'pyredis-snapshot'))
                   for t in threading.enumerate())
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'1')


@pytest.mark.parametrize('policy', ['always', 'everysec', 'none'])
async def test_clear_and_empty_snapshot_compact_on_start(tmp_path: Path, policy: str) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync=policy, snapshot_enabled=True)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'SET', b'obsolete', b'v')
        await client.call(b'FLUSHDB')
    config = replace(config, compact_on_start=True)
    for _ in range(2):
        server = await start_server(config)
        async with server, peer(server.sockets[0].getsockname()[1]) as client:
            assert await client.call(b'DBSIZE') == Integer(0)
            assert server.persistence is not None
            assert server.persistence.file.sequence == server.persistence.file.synced_sequence
