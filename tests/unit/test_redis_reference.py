from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest
from _support.redis_reference import (
    Diagnostics,
    ReferenceError,
    configured_executable,
    private_reference,
    redis_version,
    stop_process,
)

PYTHON = getattr(sys, "_base_executable", sys.executable)


def test_only_unconfigured_absence_is_unavailable(tmp_path: Path) -> None:
    assert configured_executable(None, None) is None
    assert configured_executable(None, PYTHON) == PYTHON
    assert configured_executable(PYTHON, None) == str(Path(PYTHON).resolve())
    for invalid in ("", " ", str(tmp_path / "missing.exe"), str(tmp_path)):
        with pytest.raises(ReferenceError):
            configured_executable(invalid, PYTHON)


@pytest.mark.parametrize("output,expected", [
    (b"Redis server v=6.2.0 sha=000 malloc=libc bits=64", (6, 2, 0)),
    (b"Redis server v=7.4.12 sha=000", (7, 4, 12)),
    (b"Redis server v=8.2.1 sha=000", (8, 2, 1)),
])
def test_reference_version(output: bytes, expected: tuple[int, int, int]) -> None:
    assert redis_version(output) == expected


@pytest.mark.parametrize("output", [b"Python 3.11", b"Redis server v=6.0.20 sha=x",
                                         b"Redis server v=7.4.0-rc1 sha=x", b""])
def test_bad_or_old_reference_version_fails(output: bytes) -> None:
    with pytest.raises(ReferenceError):
        redis_version(output)


# This synthetic child implements only startup/PING/INFO control for fixture tests.
# It is not a Redis oracle and never runs database comparisons.
CHILD = r'''
import asyncio, os, sys
from pathlib import Path
mode = sys.argv[1]
if "--version" in sys.argv:
    if mode == "version_timeout":
        import time; time.sleep(60)
    if mode == "version_output":
        print("x" * 200_000); sys.exit(0)
    print("Python 3.11" if mode == "bad_version" else "Redis server v=7.4.0 sha=000")
    sys.exit(0)
Path(sys.argv[sys.argv.index("--dir") + 1], "pid").write_text(str(os.getpid()))
if mode == "exit":
    print("invalid config", flush=True); sys.exit(2)
if mode in ("collision", "bind_exhaust"):
    marker = Path(sys.argv[sys.argv.index("--dir") + 1], "attempt")
    if mode == "bind_exhaust" or not marker.exists():
        marker.touch()
        print("bind: Address already in use", flush=True); sys.exit(1)
async def client(reader, writer):
    try:
        while await reader.readline():
            command = []
            # PING has one argument, INFO has two; read bulk command components.
            size = int((await reader.readline())[1:])
            command.append(await reader.readexactly(size + 2))
            if command[0] == b"INFO\r\n":
                size = int((await reader.readline())[1:])
                await reader.readexactly(size + 2)
            if mode == "connection_timeout":
                await asyncio.Event().wait()
            if command[0] == b"PING\r\n":
                writer.write(b"+wrong\r\n" if mode == "bad_ping" else b"+PONG\r\n")
            else:
                pid = os.getpid() + (1 if mode == "wrong_pid" else 0)
                payload = f"redis_version:7.4.0\r\nprocess_id:{pid}\r\n".encode()
                writer.write(b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n")
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
async def main():
    if mode == "startup_timeout":
        await asyncio.Event().wait()
    port = int(sys.argv[sys.argv.index("--port") + 1])
    server = await asyncio.start_server(client, "127.0.0.1", port)
    print("Ready to accept connections", flush=True)
    async with server:
        await server.serve_forever()
asyncio.run(main())
'''


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["normal", "collision"])
async def test_private_startup_identity_and_cleanup(tmp_path: Path, mode: str) -> None:
    child = tmp_path / "control.py"
    child.write_text(CHILD)
    async with private_reference((PYTHON, str(child), mode), tmp_path) as reference:
        assert reference.version == (7, 4, 0)
        assert reference.process.returncode is None
        process = reference.process
    assert process.returncode is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,message", [
    ("exit", "invalid config"), ("startup_timeout", "startup"),
    ("connection_timeout", "connection"), ("wrong_pid", "identity"),
    ("version_timeout", "version"),
    ("version_output", "8192"), ("bad_version", "version"),
    ("bind_exhaust", "Address already in use"), ("bad_ping", "PING"),
])
async def test_failed_startup_is_bounded_and_reaped(
    tmp_path: Path, mode: str, message: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from _support import redis_reference

    original_spawn = redis_reference.spawn
    processes = []

    async def tracked_spawn(*command: str) -> asyncio.subprocess.Process:
        process = await original_spawn(*command)
        processes.append(process)
        return process

    monkeypatch.setattr(redis_reference, "spawn", tracked_spawn)
    child = tmp_path / "control.py"
    child.write_text(CHILD)
    before = {task for task in asyncio.all_tasks()}
    with pytest.raises(ReferenceError, match=message):
        async with private_reference((PYTHON, str(child), mode), tmp_path,
                                     startup_timeout=0.5, connection_timeout=0.1,
                                     version_timeout=0.5):
            pytest.fail("failed startup yielded a reference")
    assert not {task for task in asyncio.all_tasks() if task not in before and not task.done()}
    assert processes and all(process.returncode is not None for process in processes)


@pytest.mark.asyncio
async def test_cancellation_cleans_reference_process(tmp_path: Path) -> None:
    child = tmp_path / "control.py"
    child.write_text(CHILD)
    ready = asyncio.Event()
    processes = []

    async def owner() -> None:
        async with private_reference((PYTHON, str(child), "normal"), tmp_path) as reference:
            processes.append(reference.process)
            ready.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(owner())
    await asyncio.wait_for(ready.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert processes[0].returncode is not None


@pytest.mark.asyncio
async def test_cancellation_during_startup_reaps_all_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from _support import redis_reference

    child = tmp_path / "control.py"
    child.write_text(CHILD)
    spawned = asyncio.Event()
    original = redis_reference.spawn
    processes = []

    async def track(*command: str) -> asyncio.subprocess.Process:
        process = await original(*command)
        processes.append(process)
        if "--version" not in command:
            spawned.set()
        return process

    monkeypatch.setattr(redis_reference, "spawn", track)

    async def owner() -> None:
        async with private_reference((PYTHON, str(child), "startup_timeout"), tmp_path):
            pytest.fail("startup should not complete")

    task = asyncio.create_task(owner())
    await asyncio.wait_for(spawned.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(processes) == 2 and all(process.returncode is not None for process in processes)


@pytest.mark.asyncio
async def test_spawn_cancellation_cannot_lose_a_created_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from _support import redis_reference

    original = asyncio.create_subprocess_exec
    acquired, release = asyncio.Event(), asyncio.Event()
    processes = []

    async def delayed(*command: str, **options: object) -> asyncio.subprocess.Process:
        process = await original(*command, **options)
        processes.append(process)
        assert process.stdout is not None
        await process.stdout.readexactly(1)
        acquired.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed)
    noisy = "import sys,time; sys.stdout.write('x'*200000); sys.stdout.flush(); time.sleep(60)"
    task = asyncio.create_task(redis_reference.spawn(PYTHON, "-c", noisy))
    await asyncio.wait_for(acquired.wait(), 5)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert processes[0].returncode is not None


@pytest.mark.asyncio
async def test_output_diagnostics_are_bounded() -> None:
    from types import SimpleNamespace

    reader = asyncio.StreamReader()
    reader.feed_data(b"x" * 200_000 + b"Ready to accept connections")
    reader.feed_eof()
    diagnostics = Diagnostics()
    await diagnostics.drain(SimpleNamespace(stdout=reader))
    assert diagnostics.ready.is_set()
    assert len(diagnostics.tail) == 8192


@pytest.mark.asyncio
async def test_stubborn_process_is_killed_and_reaped() -> None:
    class StubbornProcess:
        returncode = None
        terminated = False
        killed = False

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        async def wait(self) -> int:
            if self.killed:
                return 137
            await asyncio.Event().wait()
            raise AssertionError("unexpected wait completion")

    process = StubbornProcess()
    await stop_process(process, timeout=0.01)
    assert process.terminated and process.killed
