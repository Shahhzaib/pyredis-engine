from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence import journal
from pyredis_engine.persistence.errors import JournalError
from pyredis_engine.protocol import Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.persistence]


@pytest.mark.parametrize("operation", ["append", "sync"])
async def test_disk_failure_fail_stops_without_success_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"SET", b"safe", b"v") == Simple(b"OK")

        def fail(*args: object) -> None:
            raise OSError("injected disk failure")

        monkeypatch.setattr(owner.file, operation, fail)
        try:
            reply = await connection.call(b"SET", b"unacknowledged", b"v")
        except (ReferenceError, OSError):
            pass
        else:
            assert isinstance(reply, Error) and reply.payload.startswith(b"MISCONF")
        assert owner.failure is not None and not server.is_serving()
        client = server.executor.new_client()
        rejected = await owner.execute([b"SET", b"later", b"v"], client)
        assert rejected.close and isinstance(rejected.reply, Error)
        assert server.executor.store.get_string(b"later") is None
        server.executor.close_client(client)
    # The failed operation may have appended a complete unacknowledged record.
    # Every previously acknowledged always-policy write must survive.
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"safe") == Bulk(b"v")


async def test_bitmap_append_failure_keeps_acknowledged_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'bitmap-journal'), aof_fsync='always')
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b'SETBIT', b'k', b'0', b'1') == Integer(0)

        def fail(*args: object) -> None:
            raise OSError('injected bitmap journal failure')

        monkeypatch.setattr(owner.file, 'append', fail)
        try:
            reply = await connection.call(b'BITFIELD', b'k', b'SET', b'u8', b'8', b'255')
        except (ReferenceError, OSError):
            pass
        else:
            assert isinstance(reply, Error) and reply.payload.startswith(b'MISCONF')
        assert owner.failure is not None
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b'GET', b'k') == Bulk(b'\x80')


@pytest.mark.parametrize('command', [
    (b'PFADD', b'h', b'a'),
    (b'GEOADD', b'g', b'0', b'0', b'p'),
])
async def test_hll_geo_append_failure_is_fail_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: tuple[bytes, ...],
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'journal'), aof_fsync='always')
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b'SET', b'safe', b'value') == Simple(b'OK')

        def fail(*args: object) -> None:
            raise OSError('injected journal failure')

        monkeypatch.setattr(owner.file, 'append', fail)
        try:
            reply = await connection.call(*command)
        except (ReferenceError, OSError):
            pass
        else:
            assert isinstance(reply, Error) and reply.payload.startswith(b'MISCONF')
        assert owner.failure is not None
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b'GET', b'safe') == Bulk(b'value')
        assert await connection.call(b'TYPE', command[1]) == Simple(b'none')


async def test_everysec_background_sync_failure_is_visible_and_stops_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    wake = asyncio.Event()

    async def wait(interval: float) -> None:
        await wake.wait()
        wake.clear()

    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof")),
                                aof_sync_wait=wait)
    owner = server.persistence
    assert owner is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"INCR", b"k") == Integer(1)

        def fail() -> None:
            raise OSError("injected background sync failure")

        monkeypatch.setattr(owner.file, "sync", fail)
        wake.set()
        async with asyncio.timeout(5):
            while owner.failure is None:
                await asyncio.sleep(0)
        assert not server.is_serving()
    assert "injected background sync failure" in caplog.text


async def test_cancellation_cannot_orphan_inflight_disk_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None
    entered, release = threading.Event(), threading.Event()
    original = owner.file.sync

    def blocked() -> None:
        entered.set()
        assert release.wait(10)
        original()

    monkeypatch.setattr(owner.file, "sync", blocked)
    async with server:
        client = server.executor.new_client()
        pending = asyncio.create_task(owner.execute([b"INCR", b"k"], client))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel()
            await asyncio.sleep(0)
            pending.cancel()  # repeated cancellation still must await owned I/O
            await asyncio.sleep(0)
            assert owner.pending and not pending.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        server.executor.close_client(client)
        assert owner.file.sequence == owner.file.synced_sequence == 1
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"k") == Bulk(b"1")


async def test_missing_directory_and_permission_failure_release_startup_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing" / "journal.aof"
    with pytest.raises(FileNotFoundError):
        await start_server(ServerConfig(port=0, aof_path=str(missing)))
    assert not missing.parent.exists()
    target = tmp_path / "journal.aof"
    target.write_bytes(b"private existing content")
    original = journal.os.open

    def deny(path: object, flags: int, mode: int = 0o777) -> int:
        if Path(path) == target:
            raise PermissionError("injected access denied")
        return original(path, flags, mode)

    with monkeypatch.context() as context:
        context.setattr(journal.os, "open", deny)
        with pytest.raises(PermissionError):
            await start_server(ServerConfig(port=0, aof_path=str(target)))
    assert target.read_bytes() == b"private existing content"
    assert not any(thread.name.startswith("pyredis-aof") for thread in threading.enumerate())


async def test_graceful_final_sync_failure_is_not_silently_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def dormant(interval: float) -> None:
        await asyncio.Event().wait()

    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof")),
                                aof_sync_wait=dormant)
    owner = server.persistence
    assert owner is not None
    async with peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"SET", b"k", b"v") == Simple(b"OK")

    def fail() -> None:
        raise OSError("injected final sync failure")

    monkeypatch.setattr(owner.file, "sync", fail)
    server.close()
    with pytest.raises(JournalError, match="final sync"):
        await server.wait_closed()
    assert owner.closed and owner.failure is not None
    assert server.client_task_count == 0 and server.expiration_task.done()


async def test_unexpected_expiry_bug_still_closes_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    wake = asyncio.Event()

    async def wait(interval: float) -> None:
        await wake.wait()

    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof")),
                                expiry_wait=wait)
    owner = server.persistence
    assert owner is not None

    def bug(*args: object) -> int:
        raise RuntimeError("injected unexpected expiry bug")

    monkeypatch.setattr(server.executor, "expire_batch", bug)
    wake.set()
    try:
        with pytest.raises(RuntimeError, match="unexpected expiry bug"):
            await asyncio.wait_for(server.wait_closed(), 5)
        assert owner.closed and owner.file.fd is None
    finally:
        server.close()
        if not owner.closed:
            await owner.close()
        server.executor.close()


async def test_unexpected_client_bug_during_shutdown_still_closes_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal.aof")))
    owner = server.persistence
    assert owner is not None
    entered, release = threading.Event(), threading.Event()

    def bug(payload: bytes) -> None:
        entered.set()
        assert release.wait(10)
        raise RuntimeError("injected unexpected client bug")

    monkeypatch.setattr(owner.file, "append", bug)
    try:
        async with peer(server.sockets[0].getsockname()[1]) as connection:
            request = asyncio.create_task(connection.call(b"INCR", b"k"))
            assert await asyncio.to_thread(entered.wait, 5)
            server.close()
            closing = asyncio.create_task(server.wait_closed())
            await asyncio.sleep(0)  # waiter has captured the still-running client task
            release.set()
            with pytest.raises((ReferenceError, OSError)):
                await request
            with pytest.raises(RuntimeError, match="unexpected client bug"):
                await closing
            assert owner.closed and owner.file.fd is None
    finally:
        release.set()
        server.close()
        if not owner.closed:
            await owner.close()
        server.executor.close()
