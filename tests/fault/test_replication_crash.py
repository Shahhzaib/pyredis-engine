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
from pyredis_engine.protocol import Array, Bulk, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.replication, pytest.mark.persistence]


@contextmanager
def child(path: Path, port: int, stage: str) -> Iterator[
    tuple[subprocess.Popen[str], queue.Queue[str]]
]:
    root = Path(__file__).resolve().parents[2]
    process = subprocess.Popen([sys.executable, '-u',
        str(root / 'tests' / '_support' / 'replica_child.py'), str(path), str(port), stage],
        cwd=root, env=dict(os.environ, PYTHONPATH=str(root / 'src')), stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
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


async def line(lines: queue.Queue[str]) -> str:
    return await asyncio.to_thread(lines.get, True, 10)


@pytest.mark.parametrize('stage', ['manifest_activate', 'manifest_activated', 'none'])
async def test_process_kill_preserves_old_or_installed_snapshot_and_committed_exec(
    tmp_path: Path, stage: str,
) -> None:
    primary = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / 'primary.aof'),
        snapshot_enabled=True, aof_fsync='always', replication_enabled=True))
    replica_path = tmp_path / 'replica.aof'
    replica_config = ServerConfig(port=0, aof_path=str(replica_path), snapshot_enabled=True,
                                  aof_fsync='always')
    old = await start_server(replica_config)
    async with old, peer(old.sockets[0].getsockname()[1]) as client:
        await client.call(b'SET', b'old', b'v')
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'new', b'\x00\xff')
        with child(replica_path, primary.sockets[0].getsockname()[1], stage) as (process, lines):
            assert (await line(lines)).startswith('PORT ')
            if stage == 'none':
                assert await line(lines) == 'READY'
                replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'a'),
                    (b'INCR', b'b'), (b'EXEC',)])
                assert replies[-1] == Array((Integer(1), Integer(1)))
                assert primary.replication is not None and process.stdin is not None
                offset = primary.replication.offset
                process.stdin.write(str(offset) + '\n')
                process.stdin.flush()
                assert await line(lines) == 'OFFSET ' + str(offset)
            else:
                assert await line(lines) == 'STAGE ' + stage
            process.kill()
            process.wait(timeout=5)
    recovered = await start_server(replica_config)
    async with recovered, peer(recovered.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'GET', b'old') == Bulk(
            b'v' if stage == 'manifest_activate' else None)
        assert await reader.call(b'GET', b'new') == Bulk(
            None if stage == 'manifest_activate' else b'\x00\xff')
        expected = Bulk(b'1' if stage == 'none' else None)
        assert await reader.call(b'MGET', b'a', b'b') == Array((expected, expected))
