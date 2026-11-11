from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.store import StringValue
from pyredis_engine.persistence.codec import Entry, Group, encode_group
from pyredis_engine.persistence.snapshot import write_snapshot
from pyredis_engine.protocol import (
    Array,
    Bulk,
    Error,
    Integer,
    Simple,
    read_command,
    serialize_reply,
)
from pyredis_engine.replication.wire import HEADER, OVERHEAD, ReplicationError, SnapshotInfo, packet
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.replication]


def config(path: Path, **options: object) -> ServerConfig:
    return ServerConfig(port=0, aof_path=str(path), snapshot_enabled=True,
                        aof_fsync='always', replication_enabled=True, **options)


@pytest.mark.parametrize('damage', ['version', 'identity', 'checksum', 'truncated', 'oversized',
    'gap', 'duplicate', 'frame_checksum', 'frame_schema', 'frame_length'])
async def test_untrusted_primary_never_applies_invalid_snapshot_or_effect(
    tmp_path: Path, damage: str,
) -> None:
    local = config(tmp_path / 'replica.aof')
    seeded = await start_server(replace(local, replication_enabled=False))
    async with seeded, peer(seeded.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'old', b'preserved')
    source = tmp_path / 'reference.snapshot'
    store_id, generation = b's' * 16, b'g' * 16
    write_snapshot(source, store_id, generation, 5,
        iter((Entry(b'new', StringValue(b'\x00\xff'), None),)), 1, local)
    snapshot = source.read_bytes()
    info = SnapshotInfo(b'a' * 40, 0, store_id, generation, 5, len(snapshot))
    header = info.encode()
    tail = b''
    if damage == 'version':
        header = header.replace(b'PYREDIS-FULLSYNC 1', b'PYREDIS-FULLSYNC 2')
    elif damage == 'identity':
        header = replace(info, store_id=b'x' * 16).encode()
    elif damage == 'checksum':
        snapshot = snapshot[:-1] + bytes((snapshot[-1] ^ 1,))
    elif damage == 'truncated':
        snapshot = snapshot[:-1]
    elif damage == 'oversized':
        header = replace(info, size=local.snapshot_max_bytes + 1).encode()
    else:
        payload = encode_group(Group(False, (Entry(b'bad', StringValue(b'x'), None),)), local)
        end = len(payload) + OVERHEAD
        if damage == 'gap':
            end += 1
        elif damage == 'duplicate':
            end = 0
        elif damage == 'frame_schema':
            payload = b'\xff' * 5
            end = len(payload) + OVERHEAD
        tail = packet(b'E', end, payload)
        if damage == 'frame_checksum':
            tail = tail[:-1] + bytes((tail[-1] ^ 1,))
        elif damage == 'frame_length':
            tail = HEADER.pack(b'E', end, local.aof_max_record_bytes + 1)
    stopped, done = asyncio.Event(), asyncio.Event()

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            assert await read_command(reader, local) == [b'PYREDIS.SYNC', b'1']
            writer.write(serialize_reply(Simple(header)) + snapshot + tail)
            await writer.drain()
            if damage != 'truncated':
                await stopped.wait()
        finally:
            writer.close()
            await writer.wait_closed()
            done.set()

    fake = await asyncio.start_server(respond, '127.0.0.1', 0)
    async with fake:
        replica = await start_server(replace(local, replica_host='127.0.0.1',
            replica_port=fake.sockets[0].getsockname()[1]))
        try:
            async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
                assert replica.replication is not None
                async with asyncio.timeout(5):
                    while replica.replication.phase != 'down':
                        replica.replication.changed.clear()
                        await replica.replication.changed.wait()
                assert replica.replication.last_error
                assert replica.replication.offset == 0
                result = await reader.call(b'GET', b'bad')
                assert isinstance(result, Error) and result.payload.startswith(b'MASTERDOWN ')
                assert replica.executor.store.physical_entry(b'bad')[0] is None
        finally:
            stopped.set()
            await asyncio.wait_for(done.wait(), 5)
    recovered = await start_server(replace(local, replication_enabled=False))
    async with recovered, peer(recovered.sockets[0].getsockname()[1]) as reader:
        if damage in ('version', 'identity', 'checksum', 'truncated', 'oversized'):
            assert await reader.call(b'MGET', b'old', b'new') == Array(
                (Bulk(b'preserved'), Bulk(None)))
        else:
            assert await reader.call(b'MGET', b'old', b'new') == Array(
                (Bulk(None), Bulk(b'\x00\xff')))
        assert await reader.call(b'GET', b'bad') == Bulk(None)
    assert not list(tmp_path.glob('.pyredis-sync-*'))


@pytest.mark.parametrize('limit', ['bytes', 'frames'])
async def test_slow_initial_sync_queue_overflow_disconnects_only_replica(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof',
        replication_queue_bytes=1 if limit == 'bytes' else 1024,
        replication_queue_frames=1 if limit == 'frames' else 4096))
    assert primary.persistence is not None and primary.persistence.generations is not None
    entered, release = threading.Event(), threading.Event()

    def pause(stage: str) -> None:
        if stage == 'replication_snapshot':
            entered.set()
            assert release.wait(10)

    monkeypatch.setattr(primary.persistence.generations, '_stage', pause)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica:
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                for count in range(1, 4):
                    assert await writer.call(b'INCR', b'k') == Integer(count)
                assert primary.is_serving()
                assert primary.replication is not None and replica.replication is not None
                assert any(s.failed for s in primary.replication.sessions)
                with pytest.raises(ReplicationError):
                    await replica.replication.wait_ready()
            finally:
                release.set()
    assert not list(tmp_path.glob('.pyredis-sync-*'))
    assert not any(t.name.startswith(('pyredis-aof', 'pyredis-snapshot'))
                   for t in threading.enumerate())


@pytest.mark.parametrize('stage', ['manifest_activate', 'manifest_activated', 'journal_rotate'])
async def test_replica_snapshot_cutover_fault_keeps_recoverable_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'new', b'v')
        local = config(tmp_path / 'replica.aof')
        seeded = await start_server(local)
        async with seeded, peer(seeded.sockets[0].getsockname()[1]) as client:
            await client.call(b'SET', b'old', b'v')
        from pyredis_engine.persistence.generations import GenerationFiles

        original = GenerationFiles._stage

        def fail(files: GenerationFiles, name: str) -> None:
            if files.root.name == 'replica.aof' and name == stage:
                raise OSError('injected replica ' + stage)
            original(files, name)

        monkeypatch.setattr(GenerationFiles, '_stage', fail)
        replica = await start_server(replace(local, replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        async with replica:
            assert replica.replication is not None and replica.persistence is not None
            with pytest.raises(ReplicationError):
                await replica.replication.wait_ready()
            assert replica.persistence.failure is not None
        recovered = await start_server(replace(local, replication_enabled=False))
        async with recovered, peer(recovered.sockets[0].getsockname()[1]) as reader:
            expected = (Bulk(b'v'), Bulk(None)) if stage == 'manifest_activate' else (
                Bulk(None), Bulk(b'v'))
            assert await reader.call(b'MGET', b'old', b'new') == Array(expected)


async def test_shutdown_during_download_file_acquisition_closes_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    from pyredis_engine.replication import node

    primary = await start_server(config(tmp_path / 'primary.aof'))
    entered, release = threading.Event(), threading.Event()
    original = node.os.open
    descriptors = []

    def pause(path: object, flags: int, mode: int = 0o777, **kwargs: object) -> int:
        fd = original(path, flags, mode, **kwargs)
        if Path(path).name == 'received.snapshot':
            descriptors.append(fd)
            entered.set()
            assert release.wait(10)
        return fd

    async with primary:
        monkeypatch.setattr(node.os, 'open', pause)
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
            replica_port=primary.sockets[0].getsockname()[1]))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            replica.close()
            closing = asyncio.create_task(replica.wait_closed())
            await asyncio.sleep(0)
            assert not closing.done()
        finally:
            release.set()
        await closing
        assert descriptors
        for fd in descriptors:
            with pytest.raises(OSError):
                os.fstat(fd)
    assert not list(tmp_path.glob('.pyredis-sync-*'))
