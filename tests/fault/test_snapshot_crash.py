from __future__ import annotations

import asyncio
import os
import queue
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.persistence]

STAGES = ['snapshot_rebuild', 'snapshot_create', 'snapshot_write', 'snapshot_sync',
    'snapshot_verify', 'snapshot_replace', 'snapshot_directory_sync', 'snapshot_final_verify',
    'journal_rotation', 'journal_tail', 'journal_sync', 'journal_verify',
    'journal_directory_sync', 'backup_create', 'manifest_create', 'manifest_sync',
    'manifest_verify', 'manifest_activate', 'legacy_release', 'manifest_activated',
    'directory_sync', 'journal_rotate', 'old_generation_cleanup']


@contextmanager
def child(path: Path, stage: str, policy: str = 'always') -> Iterator[
    tuple[subprocess.Popen[str], queue.Queue[str]]
]:
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, '-u', str(root / 'tests' / '_support' / 'snapshot_child.py'),
               str(path), stage, policy]
    process = subprocess.Popen(command, cwd=root,
                               env=dict(os.environ, PYTHONPATH=str(root / 'src')),
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    lines: queue.Queue[str] = queue.Queue()

    def drain() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put(line.strip())

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        yield process, lines
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        reader.join(timeout=5)
        assert not reader.is_alive()
        assert process.stdout is not None and process.stdin is not None
        process.stdout.close()
        process.stdin.close()


def send(process: subprocess.Popen[str], line: str) -> None:
    assert process.stdin is not None
    process.stdin.write(line + '\n')
    process.stdin.flush()


async def line(lines: queue.Queue[str]) -> str:
    return await asyncio.to_thread(lines.get, True, 10)


@pytest.mark.parametrize('stage', STAGES)
async def test_kill_at_each_publication_boundary_recovers_exact_committed_execs(
    tmp_path: Path, stage: str,
) -> None:
    path = tmp_path / 'engine.aof'
    with child(path, stage) as (process, lines):
        port_line = await line(lines)
        assert port_line.startswith('PORT '), port_line
        async with peer(int(port_line.split()[1])) as client:
            assert await client.call(b'INCR', b'counter') == Integer(1)
            send(process, 'compact')
            reached = await line(lines)
            if reached == 'PINNED':
                # The snapshot is frozen at 1; these effects must appear once in
                # either the old source log or the new journal tail.
                replies = await client.pipeline([(b'MULTI',), (b'INCR', b'counter'),
                    (b'SET', b'binary', b'\x00\xff'), (b'EXEC',)])
                assert replies[-1] == Array((Integer(2), Simple(b'OK')))
                send(process, 'resume')
                reached = await line(lines)
                expected = b'2'
            else:
                expected = b'1'
            assert reached == 'STAGE ' + stage
            process.kill()
            process.wait(timeout=5)
    config = ServerConfig(port=0, aof_path=str(path), aof_fsync='always', snapshot_enabled=True)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'counter') == Bulk(expected)
        assert await client.call(b'GET', b'binary') == Bulk(
            b'\x00\xff' if expected == b'2' else None)
        assert await client.call(b'INCR', b'counter') == Integer(int(expected) + 1)


@pytest.mark.parametrize('policy', ['always', 'everysec', 'none'])
async def test_kill_after_new_generation_ack_and_during_replay_is_repeatable(
    tmp_path: Path, policy: str,
) -> None:
    path = tmp_path / 'engine.aof'
    with child(path, 'none', policy) as (process, lines):
        port_line = await line(lines)
        assert port_line.startswith('PORT '), port_line
        async with peer(int(port_line.split()[1])) as client:
            await client.call(b'INCR', b'counter')
            send(process, 'compact')
            assert await line(lines) == 'PINNED'
            send(process, 'resume')
            assert await line(lines) == 'COMPACTED True'
            assert await client.call(b'INCR', b'counter') == Integer(2)
            process.kill()
            process.wait(timeout=5)
    for stage in ('journal_replay', 'journal_replay_group'):
        with child(path, stage, policy) as (process, lines):
            assert await line(lines) == 'STAGE ' + stage
            process.kill()
            process.wait(timeout=5)
    config = ServerConfig(port=0, aof_path=str(path), aof_fsync=policy, snapshot_enabled=True)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        # Process kill retains OS caches; this is not a power-loss guarantee for
        # the weaker policies.
        assert await client.call(b'GET', b'counter') == Bulk(b'2')


async def test_cleanup_retains_on_windows_or_survives_posix_unlink_kill(tmp_path: Path) -> None:
    path = tmp_path / 'engine.aof'
    config = ServerConfig(port=0, aof_path=str(path), aof_fsync='always', snapshot_enabled=True)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'INCR', b'counter')
        assert server.persistence is not None
        assert (await server.persistence.compact()).activated
        result = await server.persistence.compact()
        assert result.activated
        if sys.platform == 'win32':
            assert result.cleanup_deferred
            return  # This branch is deliberately exercised separately on POSIX in Docker.
    with child(path, 'cleanup_unlink') as (process, lines):
        assert (await line(lines)).startswith('PORT ')
        send(process, 'compact')
        assert await line(lines) == 'PINNED'
        send(process, 'resume')
        assert await line(lines) == 'STAGE cleanup_unlink'
        process.kill()
        process.wait(timeout=5)
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'counter') == Bulk(b'1')
