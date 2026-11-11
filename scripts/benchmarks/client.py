"""Isolated server ownership and a deadline-bounded RESP2 benchmark client."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Reply, read_value, serialize_reply

ROOT = Path(__file__).resolve().parents[2]
READ_LIMITS = ServerConfig(max_args=100000)


class Peer:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader, self.writer = reader, writer

    @classmethod
    async def open(cls, port: int) -> Peer:
        return cls(*await asyncio.wait_for(asyncio.open_connection('127.0.0.1', port), 5))

    async def call(self, *args: bytes) -> Reply:
        async with asyncio.timeout(10):
            self.writer.write(serialize_reply(Array(tuple(Bulk(arg) for arg in args))))
            await self.writer.drain()
            result = await read_value(self.reader, READ_LIMITS)
            if result is None:
                raise RuntimeError('unexpected benchmark peer EOF')
            return result

    async def close(self) -> None:
        self.writer.close()
        try:
            await asyncio.wait_for(self.writer.wait_closed(), 2)
        except (OSError, TimeoutError):
            self.writer.transport.abort()


class Child:
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        self.process: asyncio.subprocess.Process | None = None
        self.lock = asyncio.Lock()
        self.port = 0
        self.diagnostics = bytearray()
        self.drain_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Child:
        acquiring = asyncio.create_task(asyncio.create_subprocess_exec(sys.executable, '-m',
            'scripts.benchmarks.worker', json.dumps(self.options), cwd=ROOT,
            env=dict(os.environ, PYTHONPATH=str(ROOT / 'src'), PYTHONDONTWRITEBYTECODE='1'),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE))
        try:
            self.process = await asyncio.shield(acquiring)
        except asyncio.CancelledError:
            self.process = await acquiring
            await self.close()
            raise
        async def drain() -> None:
            assert self.process is not None and self.process.stderr is not None
            while block := await self.process.stderr.read(4096):
                self.diagnostics.extend(block)
                del self.diagnostics[:-8192]
        self.drain_task = asyncio.create_task(drain())
        try:
            result = await self._read()
            self.port = result['port']
            return self
        except BaseException:
            # Process acquisition boundary: preserve cancellation/traceback after cleanup.
            await self.close()
            raise

    async def _read(self) -> dict[str, Any]:
        assert self.process is not None and self.process.stdout is not None
        line = await asyncio.wait_for(self.process.stdout.readline(), 40)
        if not line:
            message = self.diagnostics.decode(errors='replace')
            raise RuntimeError('benchmark worker failed: ' + message)
        result: dict[str, Any] = json.loads(line)
        return result

    async def control(self, action: str, **args: Any) -> dict[str, Any]:
        assert self.process is not None and self.process.stdin is not None
        async with self.lock:
            self.process.stdin.write(json.dumps({'action': action, **args}).encode() + b'\n')
            await self.process.stdin.drain()
            result = await self._read()
            if result['memory']['peak_rss_bytes'] > 256 * 1024 * 1024:
                raise RuntimeError('owned benchmark worker exceeded 256 MiB RSS safety cap')
            return result

    async def close(self) -> None:
        process = self.process
        if process is None:
            return
        if process.returncode is None:
            try:
                if process.stdin is not None:
                    process.stdin.write(b'{"action":"close"}\n')
                    await process.stdin.drain()
                await asyncio.wait_for(process.wait(), 5)
            except (OSError, TimeoutError):
                if process.returncode is None:
                    process.kill()
                await asyncio.wait_for(process.wait(), 5)
        if process.stdin is not None:
            process.stdin.close()
        if self.drain_task is not None:
            await self.drain_task

    async def __aexit__(self, *args: object) -> None:
        await self.close()
        if args and args[0] is None and self.process is not None and self.process.returncode != 0:
            raise RuntimeError(f'benchmark worker exit {self.process.returncode}: '
                               + self.diagnostics.decode(errors='replace'))
