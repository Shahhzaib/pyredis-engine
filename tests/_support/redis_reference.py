"""Private test-process ownership, bounded I/O and Redis identity checks."""
from __future__ import annotations

import asyncio
import re
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import (
    Array,
    Bulk,
    ProtocolError,
    Reply,
    Simple,
    read_value,
    serialize_reply,
)


class ReferenceError(RuntimeError):
    """Configured Redis failed validation/startup; never an availability skip."""


def configured_executable(explicit: str | None, discovered: str | None) -> str | None:
    if explicit is None:
        return discovered
    if not explicit.strip() or not Path(explicit).is_file():
        raise ReferenceError("PYREDIS_REDIS_SERVER must name an installed Redis executable")
    return str(Path(explicit).resolve())


def redis_version(output: bytes) -> tuple[int, int, int]:
    match = re.search(rb"Redis server v=(\d+)\.(\d+)\.(\d+)(?=\s|$)", output)
    if match is None:
        raise ReferenceError("unrecognized Redis version output")
    version = (int(match[1]), int(match[2]), int(match[3]))
    if version < (6, 2, 0):
        raise ReferenceError(f"Redis >=6.2.0 required for GETEX/counted pops; got {version}")
    return version


async def stop_process(process: asyncio.subprocess.Process, timeout: float = 2) -> None:
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(process.wait(), timeout)
    except TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await asyncio.wait_for(process.wait(), timeout)


async def spawn(*command: str) -> asyncio.subprocess.Process:
    pending = asyncio.create_task(asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT))
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError:
        process = await pending
        drain = asyncio.create_task(Diagnostics().drain(process))
        await finish_cleanup(process, (drain,))
        raise


async def close_writer(writer: asyncio.StreamWriter, timeout: float = 2) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout)
    except (TimeoutError, ConnectionError):
        writer.transport.abort()


async def connect(
    port: int, timeout: float = 2,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout)


async def exchange(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: tuple[bytes, ...],
    timeout: float = 5,
) -> Reply:
    async with asyncio.timeout(timeout):
        writer.write(serialize_reply(Array(tuple(Bulk(part) for part in command))))
        await writer.drain()
        reply = await read_value(reader, ServerConfig(max_args=10_000))
        if reply is None:
            raise ReferenceError("unexpected EOF during Redis comparison")
        return reply


@dataclass
class Reference:
    port: int
    version: tuple[int, int, int]
    process: asyncio.subprocess.Process


class Diagnostics:
    def __init__(self) -> None:
        self.tail = b""
        self.ready = asyncio.Event()

    async def drain(self, process: asyncio.subprocess.Process) -> None:
        assert process.stdout is not None
        while chunk := await process.stdout.read(4096):
            self.tail = (self.tail + chunk)[-8192:]
            if b"Ready to accept connections" in self.tail:
                self.ready.set()

    @property
    def text(self) -> str:
        return self.tail.decode("utf-8", errors="replace")


async def check_identity(reference: Reference, timeout: float) -> None:
    async with asyncio.timeout(timeout):
        reader, writer = await connect(reference.port, timeout)
        try:
            if await exchange(reader, writer, (b"PING",), timeout) != Simple(b"PONG"):
                raise ReferenceError("Redis startup PING failed")
            info = await exchange(reader, writer, (b"INFO", b"server"), timeout)
            if not isinstance(info, Bulk) or info.payload is None:
                raise ReferenceError("Redis startup INFO response type is invalid")
            fields = dict(line.split(b":", 1) for line in info.payload.splitlines() if b":" in line)
            expected_version = b".".join(str(part).encode() for part in reference.version)
            if (fields.get(b"process_id") != str(reference.process.pid).encode()
                    or fields.get(b"redis_version") != expected_version):
                raise ReferenceError("Redis identity/version does not match spawned process")
        finally:
            await close_writer(writer)


@asynccontextmanager
async def private_reference(
    command: tuple[str, ...], directory: Path, *, startup_timeout: float = 5,
    connection_timeout: float = 2, version_timeout: float = 5, attempts: int = 3,
) -> AsyncIterator[Reference]:
    try:
        version_process = await spawn(*command, "--version")
    except OSError as error:
        raise ReferenceError(f"cannot launch configured Redis: {error}") from error
    try:
        try:
            async with asyncio.timeout(version_timeout):
                assert version_process.stdout is not None
                output = b""
                while chunk := await version_process.stdout.read(8193 - len(output)):
                    output += chunk
                    if len(output) > 8192:
                        raise ReferenceError("Redis version output exceeds 8192 bytes")
                await version_process.wait()
        except TimeoutError as error:
            raise ReferenceError("Redis version probe timed out") from error
        if version_process.returncode != 0:
            raise ReferenceError(f"Redis version probe failed: {output[-8192:]!r}")
        version = redis_version(output)
    finally:
        # Drain unread output even on oversized/timed-out probes: paused PIPE
        # readers can otherwise prevent subprocess.wait() completing after kill.
        drain_version = asyncio.create_task(Diagnostics().drain(version_process))
        await finish_cleanup(version_process, (drain_version,))

    for attempt in range(attempts):
        # Redis --port 0 disables TCP. Retry only verified bind collisions.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        process = await spawn(*command, "--bind", "127.0.0.1", "--port", str(port),
                              "--save", "", "--appendonly", "no", "--dir", str(directory),
                              "--daemonize", "no", "--loglevel", "notice")
        diagnostics = Diagnostics()
        drain = asyncio.create_task(diagnostics.drain(process))
        ready = asyncio.create_task(diagnostics.ready.wait())
        exited = asyncio.create_task(process.wait())
        try:
            await asyncio.wait((ready, exited), timeout=startup_timeout,
                               return_when=asyncio.FIRST_COMPLETED)
            if not diagnostics.ready.is_set() or process.returncode is not None:
                if process.returncode is not None:
                    await drain
                    collision = b"address already in use" in diagnostics.tail.lower()
                    if collision and attempt + 1 < attempts:
                        continue
                raise ReferenceError(f"Redis startup failed/timed out: {diagnostics.text}")
            reference = Reference(port, version, process)
            try:
                await check_identity(reference, connection_timeout)
            except (OSError, TimeoutError, ProtocolError) as error:
                raise ReferenceError(f"Redis startup connection failed: {error}") from error
            yield reference
            return
        finally:
            await finish_cleanup(process, (drain, ready, exited))
    raise ReferenceError("Redis exhausted private port attempts")


async def cleanup_process(
    process: asyncio.subprocess.Process, tasks: tuple[asyncio.Task[object], ...],
) -> None:
    try:
        await stop_process(process)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result


async def finish_cleanup(
    process: asyncio.subprocess.Process, tasks: tuple[asyncio.Task[object], ...],
) -> None:
    cleanup_task = asyncio.create_task(cleanup_process(process, tasks))
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        await cleanup_task
        raise
