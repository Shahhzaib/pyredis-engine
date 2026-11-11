from __future__ import annotations

import asyncio
import sys
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence import snapshot
from pyredis_engine.persistence.errors import JournalError
from pyredis_engine.protocol import Bulk, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.persistence]

PRE_ACTIVATION = ['snapshot_rebuild', 'snapshot_create', 'snapshot_write', 'snapshot_sync',
    'snapshot_verify', 'snapshot_replace', 'snapshot_directory_sync', 'snapshot_final_verify',
    'journal_rotation', 'journal_tail', 'journal_sync', 'journal_verify',
    'journal_directory_sync', 'backup_create', 'manifest_create', 'manifest_sync',
    'manifest_verify', 'manifest_activate']
AMBIGUOUS = ['legacy_release', 'manifest_activated', 'directory_sync', 'journal_rotate']


@pytest.mark.parametrize('stage,already_compacted', [
    (stage, prior) for prior in (False, True)
    for stage in PRE_ACTIVATION + AMBIGUOUS + ['old_generation_cleanup']
    if not prior or stage not in ('backup_create', 'legacy_release')])
async def test_each_cutover_fault_preserves_recoverable_acknowledged_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str, already_compacted: bool,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True)
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None and owner.generations is not None

    def fail(name: str) -> None:
        if name == stage:
            raise OSError('injected ' + stage)

    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'INCR', b'k') == Integer(1)
        if already_compacted:
            assert (await owner.compact()).activated
        monkeypatch.setattr(owner.generations, '_stage', fail)
        result = await owner.compact()
        assert result.error == 'injected ' + stage
        assert result.activated == (stage == 'old_generation_cleanup')
        assert (owner.failure is not None) == (stage in AMBIGUOUS)
        if owner.failure is None:
            assert await client.call(b'INCR', b'k') == Integer(2)
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'1' if stage in AMBIGUOUS else b'2')


@pytest.mark.parametrize('damage', ['missing_snapshot', 'corrupt_snapshot', 'wrong_version',
    'missing_journal', 'corrupt_journal', 'missing_root', 'corrupt_manifest', 'missing_lease'])
async def test_selected_generation_damage_fails_closed_without_fallback(
    tmp_path: Path, damage: str,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True, aof_repair_tail=True)
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None and owner.generations is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')
        assert (await owner.compact()).activated
        await client.call(b'INCR', b'k')
    files = owner.generations
    assert files.manifest is not None
    current = files.manifest.current
    if damage.endswith('snapshot') or damage == 'wrong_version':
        target = files.path(current.uid, 'snap')
    elif damage.endswith('journal'):
        target = files.path(current.uid, 'aof')
    elif damage == 'missing_lease':
        target = files.root.with_name(files.root.name + '.lock')
    else:
        target = files.root
    if damage.startswith('missing'):
        target.unlink()
    else:
        raw = bytearray(target.read_bytes())
        raw[9 if damage == 'wrong_version' else -1] ^= 1
        target.write_bytes(raw)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with pytest.raises((OSError, JournalError)):
        await start_server(config)
    # A missing lease may be recreated with a new identity, but cannot load old data.
    after = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert all(after[name] == contents for name, contents in before.items())
    assert not any(t.name.startswith(('pyredis-aof', 'pyredis-snapshot'))
                   for t in threading.enumerate())


async def test_orphans_and_unrelated_files_are_ignored_and_snapshot_flag_is_required(
    tmp_path: Path,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'), snapshot_enabled=True)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')
    unrelated = tmp_path / 'unrelated.txt'
    orphan = tmp_path / ('engine.aof.g-' + 'ff' * 16 + '.snap.tmp')
    for path in (unrelated, orphan):
        path.write_bytes(b'leave me alone')
    with pytest.raises(JournalError, match='snapshot mode'):
        await start_server(replace(config, snapshot_enabled=False))
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'1')
        assert server.persistence is not None
        assert (await server.persistence.compact()).activated
    assert unrelated.read_bytes() == orphan.read_bytes() == b'leave me alone'


@pytest.mark.parametrize('limit', ['snapshot', 'tail', 'history'])
async def test_compaction_limits_abort_maintenance_without_rejecting_later_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          snapshot_enabled=True, snapshot_max_bytes=128 if limit == 'snapshot'
                          else 1024 * 1024, snapshot_max_tail_bytes=1 if limit == 'tail' else 1024,
                          snapshot_max_generations=2 if limit == 'history' else 64)
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None and owner.generations is not None
    entered, release = threading.Event(), threading.Event()

    def pause(stage: str) -> None:
        if stage == 'snapshot_write' and limit == 'tail':
            entered.set()
            assert release.wait(10)

    monkeypatch.setattr(owner.generations, '_stage', pause)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')
        if limit == 'history':
            assert (await owner.compact()).activated
        pending = asyncio.create_task(owner.compact())
        try:
            if limit == 'tail':
                assert await asyncio.to_thread(entered.wait, 5)
                await client.call(b'INCR', b'k')
        finally:
            release.set()
        result = await pending
        assert not result.activated and result.error is not None
        assert owner.failure is None
        assert await client.call(b'INCR', b'k') == Integer(3 if limit == 'tail' else 2)
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'3' if limit == 'tail' else b'2')


@pytest.mark.parametrize('operation', ['partial_write', 'fsync', 'checksum'])
async def test_actual_snapshot_io_and_checksum_failures_preserve_old_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          aof_fsync='always', snapshot_enabled=True)
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None and owner.generations is not None
    original_write, original_sync = snapshot._write_all, snapshot.os.fsync

    def write(fd: int, data: bytes) -> None:
        if operation == 'partial_write':
            original_write(fd, data[:len(data) // 2])
            raise OSError('injected short snapshot write')
        original_write(fd, data)

    def sync(fd: int) -> None:
        if operation == 'fsync' and threading.current_thread().name.startswith('pyredis-snapshot'):
            raise OSError('injected snapshot fsync')
        original_sync(fd)

    def damage(stage: str) -> None:
        if stage == 'snapshot_verify' and operation == 'checksum':
            candidates = list(tmp_path.glob('*.snap.tmp'))
            assert len(candidates) == 1
            raw = bytearray(candidates[0].read_bytes())
            raw[-1] ^= 1
            candidates[0].write_bytes(raw)

    monkeypatch.setattr(snapshot, '_write_all', write)
    monkeypatch.setattr(snapshot.os, 'fsync', sync)
    monkeypatch.setattr(owner.generations, '_stage', damage)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')
        result = await owner.compact()
        assert not result.activated and result.error is not None
        assert owner.failure is None
        assert await client.call(b'INCR', b'k') == Integer(2)
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'2')


async def test_source_sync_failure_is_fatal_and_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'),
                          snapshot_enabled=True, aof_fsync='always')
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')

        def fail() -> None:
            raise OSError('injected source sync')

        monkeypatch.setattr(owner.file, 'sync', fail)
        result = await owner.compact()
        assert not result.activated and owner.failure is not None
        assert not server.is_serving()
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'1')


async def test_retirement_cleanup_and_cleanup_failure_keep_predecessor_and_foreign_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'engine.aof'), snapshot_enabled=True)
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None and owner.generations is not None
    files = owner.generations
    foreign = tmp_path / 'foreign.aof'
    foreign.write_bytes(b'untouched')

    def fail(stage: str) -> None:
        if stage == 'cleanup_unlink':
            raise OSError('injected unlink failure')

    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'k')
        assert (await owner.compact()).activated
        assert files.manifest is not None
        legacy = files.path(files.manifest.retained[0].uid, 'aof')
        monkeypatch.setattr(files, '_stage', fail)
        result = await owner.compact()
        assert result.activated and result.cleanup_deferred
        if sys.platform == 'win32':
            assert result.error is None  # No directory sync: deliberately no unlink.
        else:
            assert result.error == 'injected unlink failure'
        assert legacy.exists()
        monkeypatch.setattr(files, '_stage', lambda _: None)
        result = await owner.compact()
        assert result.activated
        assert legacy.exists() == (sys.platform == 'win32')
        assert files.manifest is not None
        predecessor = files.manifest.retained[0]
        assert files.path(predecessor.uid, 'snap').is_file()
        assert files.path(predecessor.uid, 'aof').is_file()
        assert await client.call(b'INCR', b'k') == Integer(2)
    assert foreign.read_bytes() == b'untouched'
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'k') == Bulk(b'2')
