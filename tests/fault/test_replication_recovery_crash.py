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
from _support.recovery import config, node

from pyredis_engine.protocol import Array, Bulk, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.replication, pytest.mark.persistence]


@contextmanager
def child(path: Path, role: str, port: int = 0, upstream: int = 0) -> Iterator[
    tuple[subprocess.Popen[str], queue.Queue[str]]
]:
    root = Path(__file__).resolve().parents[2]
    process = subprocess.Popen([sys.executable, '-u',
        str(root / 'tests' / '_support' / 'recovery_child.py'), str(path), role, str(port),
        str(upstream)], cwd=root, env=dict(os.environ, PYTHONPATH=str(root / 'src')),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
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


async def test_replica_process_kill_then_full_recovery_preserves_complete_exec(
    tmp_path: Path,
) -> None:
    primary = await start_server(config(tmp_path / 'primary.aof'))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        port = primary.sockets[0].getsockname()[1]
        path = tmp_path / 'replica.aof'
        with child(path, 'replica', upstream=port) as (process, lines):
            assert (await line(lines)).startswith('PORT ')
            assert await line(lines) == 'READY full'
            replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'a'),
                (b'INCR', b'b'), (b'EXEC',)])
            assert replies[-1] == Array((Integer(1), Integer(1)))
            assert process.stdin is not None
            process.stdin.write(str(node(primary).offset) + '\n')
            process.stdin.flush()
            assert await line(lines) == 'OFFSET ' + str(node(primary).offset)
            process.kill()
            process.wait(timeout=5)
        await writer.call(b'INCR', b'a')
        with child(path, 'replica', upstream=port) as (_, lines):
            port_line = await line(lines)
            assert port_line.startswith('PORT ')
            assert await line(lines) == 'READY full'
            async with peer(int(port_line.split()[1])) as reader:
                assert await reader.call(b'MGET', b'a', b'b') == Array((Bulk(b'2'), Bulk(b'1')))


async def test_primary_process_kill_and_restart_changes_id_and_replica_reconciles(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'primary.aof'
    with child(path, 'primary') as (process, lines):
        port_line = await line(lines)
        assert port_line.startswith('PORT ')
        port = int(port_line.split()[1])
        replica = await start_server(config(tmp_path / 'replica.aof', replica_host='127.0.0.1',
                                             replica_port=port))
        async with replica:
            state = node(replica)
            await state.wait_ready()
            old_id = state.replication_id
            async with peer(port) as writer:
                await writer.call(b'INCR', b'k')
                info = await writer.call(b'INFO', b'replication')
                assert isinstance(info, Bulk) and info.payload is not None
                fields = dict(row.split(b':', 1) for row in info.payload.splitlines()
                              if b':' in row)
                await state.wait_offset(int(fields[b'master_repl_offset']))
            entered, release = asyncio.Event(), asyncio.Event()

            async def hold(delay: float) -> None:
                entered.set()
                await release.wait()

            state.retry_wait = hold
            process.kill()
            process.wait(timeout=5)
            await asyncio.wait_for(entered.wait(), 5)
            with child(path, 'primary', port=port) as (_, restarted_lines):
                assert await line(restarted_lines) == 'PORT ' + str(port)
                release.set()
                await state.wait_ready()
                assert state.full_syncs == 2 and state.replication_id != old_id
                async with peer(replica.sockets[0].getsockname()[1]) as reader:
                    assert await reader.call(b'GET', b'k') == Bulk(b'1')
