from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.pubsub import ack

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.mark.parametrize('name,policy', [('SIGTERM', 'everysec'), ('SIGINT', 'always')])
async def test_handled_signal_drains_clients_and_journal_then_restart(
    tmp_path: Path, name: str, policy: str,
) -> None:
    root = Path(__file__).resolve().parents[2]
    path = tmp_path / 'signal.aof'
    command = ([sys.executable, '-u', str(root / 'tests/_support/operations_child.py'),
                str(path), policy] if os.name == 'nt' else
               [sys.executable, '-u', '-m', 'pyredis_engine', '--port', '0', '--appendonly',
                str(path), '--appendfsync', policy, '--maxmemory', '5000'])
    process = await asyncio.create_subprocess_exec(*command, cwd=root,
        env=dict(os.environ, PYTHONPATH=str(root / 'src')), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    assert process.stdout is not None and process.stderr is not None and process.stdin is not None
    try:
        ready = await asyncio.wait_for(process.stdout.readline(), 8)
        assert ready.startswith(b'PyRedis Engine listening on '), ready
        port = int(ready.rsplit(b':', 1)[1])
        async with peer(port) as writer, peer(port) as subscriber:
            assert await subscriber.call(b'SUBSCRIBE', b'private-channel') == ack(
                b'subscribe', b'private-channel', 1)
            replies = await writer.pipeline([(b'MULTI',), (b'SET', b'k', b'secret\x00\xff'),
                                             (b'INCR', b'n'), (b'EXEC',)])
            assert replies[-1] == Array((Simple(b'OK'), Integer(1)))
            await writer.call(b'MULTI')
            await writer.call(b'SET', b'not-committed', b'no')
            if os.name == 'nt':
                process.stdin.write(name.encode() + b'\n')
                await process.stdin.drain()
            else:
                process.send_signal(getattr(signal, name))
            assert await asyncio.wait_for(process.wait(), 8) == 0
            assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''
        diagnostics = await process.stderr.read()
        assert b'server stopped; metrics=' in diagnostics
        assert b'secret' not in diagnostics and b'private-channel' not in diagnostics
    finally:
        if process.returncode is None:
            process.kill()
        await asyncio.wait_for(process.wait(), 5)
        process.stdin.close()
    server = await start_server(ServerConfig(port=0, aof_path=str(path), maxmemory_bytes=5000))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'MGET', b'k', b'n', b'not-committed') == Array(
            (Bulk(b'secret\x00\xff'), Bulk(b'1'), Bulk(None)))
        assert server.metrics()['memory_accounted_bytes'] == 395
