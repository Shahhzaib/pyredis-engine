from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import Partition, config, node
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_blocked_multi_stream_read_wakes_and_records_pending() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        for key in (b"a", b"b"):
            assert await writer.call(b"XGROUP", b"CREATE", key, b"g", b"0", b"MKSTREAM") == Simple(
                b"OK"
            )
        pending = asyncio.create_task(
            reader.call(
                b"XREADGROUP",
                b"GROUP",
                b"g",
                b"c",
                b"COUNT",
                b"1",
                b"BLOCK",
                b"0",
                b"STREAMS",
                b"a",
                b"b",
                b">",
                b">",
            )
        )
        async with asyncio.timeout(5):
            while (
                not server.executor.waiters.count or not server.executor.waiters.pending[0].started
            ):
                await asyncio.sleep(0)
        assert not pending.done()
        assert await writer.call(b"XADD", b"b", b"1-0", b"f", b"v") == Bulk(b"1-0")
        result = await asyncio.wait_for(pending, 5)
        assert result == Array(
            (Array((Bulk(b"b"), Array((Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)))),)
        )
        assert (await writer.call(b"XPENDING", b"b", b"g")).payload[0] == Integer(1)
        assert not server.executor.waiters.count


async def test_waiting_consumers_receive_distinct_entries_in_order() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as first,
        peer(server.sockets[0].getsockname()[1]) as second,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        tasks = []
        for index, client in enumerate((first, second)):
            tasks.append(
                asyncio.create_task(
                    client.call(
                        b"XREADGROUP",
                        b"GROUP",
                        b"g",
                        str(index).encode(),
                        b"BLOCK",
                        b"0",
                        b"COUNT",
                        b"1",
                        b"STREAMS",
                        b"s",
                        b">",
                    )
                )
            )
            async with asyncio.timeout(5):
                while server.executor.waiters.count < index + 1 or not all(
                    w.started for w in server.executor.waiters.pending
                ):
                    await asyncio.sleep(0)
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"a") == Bulk(b"1-0")
        one = await asyncio.wait_for(tasks[0], 5)
        assert isinstance(one, Array) and not tasks[1].done()
        assert await writer.call(b"XADD", b"s", b"2-0", b"f", b"b") == Bulk(b"2-0")
        two = await asyncio.wait_for(tasks[1], 5)
        assert isinstance(two, Array) and one != two
        assert (await writer.call(b"XPENDING", b"s", b"g")).payload[0] == Integer(2)


@pytest.mark.parametrize("durable", [False, True])
async def test_blocked_reader_wakes_after_whole_producer_transaction(
    tmp_path: Path, durable: bool
) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal") if durable else None, aof_fsync="always"
    )
    server = await start_server(settings)
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        for key in (b"a", b"b"):
            assert await writer.call(b"XGROUP", b"CREATE", key, b"g", b"0", b"MKSTREAM") == Simple(
                b"OK"
            )
        pending = asyncio.create_task(
            reader.call(
                b"XREADGROUP",
                b"GROUP",
                b"g",
                b"c",
                b"BLOCK",
                b"0",
                b"STREAMS",
                b"a",
                b"b",
                b">",
                b">",
            )
        )
        async with asyncio.timeout(5):
            while (
                not server.executor.waiters.count or not server.executor.waiters.pending[0].started
            ):
                await asyncio.sleep(0)
        assert await writer.call(b"MULTI") == Simple(b"OK")
        for key in (b"a", b"b"):
            assert await writer.call(b"XADD", key, b"1-0", b"f", key) == Simple(b"QUEUED")
        assert await writer.call(b"EXEC") == Array((Bulk(b"1-0"), Bulk(b"1-0")))
        result = await asyncio.wait_for(pending, 5)
        assert isinstance(result, Array) and result.payload is not None and len(result.payload) == 2
        for key in (b"a", b"b"):
            assert (await writer.call(b"XPENDING", key, b"g")).payload[0] == Integer(1)


async def test_block_timeout_releases_waiter_and_leaves_pending_empty() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        assert await client.call(
            b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"10", b"STREAMS", b"s", b">"
        ) == Array(None)
        assert not server.executor.waiters.count
        assert (await client.call(b"XPENDING", b"s", b"g")).payload[0] == Integer(0)


async def test_blocked_group_waiter_rejects_excessive_retained_request_bytes() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        name = b"c" * 65536
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        result = await client.call(
            b"XREADGROUP", b"GROUP", b"g", name, b"BLOCK", b"0", b"STREAMS", b"s", b">"
        )
        assert isinstance(result, Error) and b"waiter limits" in result.payload
        assert not server.executor.waiters.count
        assert (await client.call(b"XPENDING", b"s", b"g")).payload[0] == Integer(0)


async def test_group_removal_and_disconnect_cleanup() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        pending = asyncio.create_task(
            reader.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"s", b">")
        )
        async with asyncio.timeout(5):
            while (
                not server.executor.waiters.count or not server.executor.waiters.pending[0].started
            ):
                await asyncio.sleep(0)
        assert await writer.call(b"XGROUP", b"DESTROY", b"s", b"g") == Integer(1)
        result = await asyncio.wait_for(pending, 5)
        assert isinstance(result, Error) and result.payload.startswith(b"NOGROUP")
        assert not server.executor.waiters.count
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        reader.writer.write(
            serialize_reply(
                Array(
                    tuple(
                        Bulk(arg)
                        for arg in (
                            b"XREADGROUP",
                            b"GROUP",
                            b"g",
                            b"c",
                            b"BLOCK",
                            b"0",
                            b"STREAMS",
                            b"s",
                            b">",
                        )
                    )
                )
            )
        )
        await reader.writer.drain()
        async with asyncio.timeout(5):
            while not server.executor.waiters.count:
                await asyncio.sleep(0)
        reader.writer.transport.abort()
        async with asyncio.timeout(5):
            while server.executor.waiters.count:
                await asyncio.sleep(0)


@pytest.mark.parametrize("durable", [False, True])
async def test_active_expiry_wakes_group_waiter(tmp_path: Path, durable: bool) -> None:
    clock = ManualClock(1000)
    settings = ServerConfig(
        port=0,
        expiry_interval=0.01,
        aof_path=str(tmp_path / "journal") if durable else None,
        aof_fsync="always",
    )
    server = await start_server(settings, clock=clock)
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        assert await writer.call(b"PEXPIRE", b"s", b"10") == Integer(1)
        pending = asyncio.create_task(
            reader.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"s", b">")
        )
        async with asyncio.timeout(5):
            while (
                not server.executor.waiters.count or not server.executor.waiters.pending[0].started
            ):
                await asyncio.sleep(0)
        clock.advance(11)
        result = await asyncio.wait_for(pending, 5)
        assert isinstance(result, Error) and result.payload.startswith(b"NOGROUP")
        assert not server.executor.waiters.count


async def test_blocked_group_read_rechecks_acl(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / "acl", ["on", "+xreadgroup", "~allowed:*"])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as client,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await client.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        assert await admin.call(
            b"XGROUP", b"CREATE", b"allowed:s", b"g", b"0", b"MKSTREAM"
        ) == Simple(b"OK")
        pending = asyncio.create_task(
            client.call(
                b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"allowed:s", b">"
            )
        )
        async with asyncio.timeout(5):
            while not server.executor.waiters.count:
                await asyncio.sleep(0)
        assert await admin.call(b"ACL", b"SETUSER", b"app", b"-xreadgroup") == Simple(b"OK")
        result = await asyncio.wait_for(pending, 5)
        assert isinstance(result, Error) and result.payload.startswith(b"NOPERM")
        assert not server.executor.waiters.count


async def test_blocked_delivery_waits_for_journal_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = await start_server(
        ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    )
    assert server.persistence is not None
    journal = server.persistence.file
    original = journal.append
    entered, release = threading.Event(), threading.Event()
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        pending = asyncio.create_task(
            reader.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"s", b">")
        )
        async with asyncio.timeout(5):
            while (
                not server.executor.waiters.count or not server.executor.waiters.pending[0].started
            ):
                await asyncio.sleep(0)
        calls = 0

        def pause_second(payload: bytes) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                entered.set()
                assert release.wait(5)
            original(payload)

        monkeypatch.setattr(journal, "append", pause_second)
        pushed = asyncio.create_task(writer.call(b"XADD", b"s", b"1-0", b"f", b"v"))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            assert not pending.done() and not pushed.done()
        finally:
            release.set()
        assert isinstance(await asyncio.wait_for(pending, 5), Array)
        assert await asyncio.wait_for(pushed, 5) == Bulk(b"1-0")


async def test_failed_blocked_delivery_is_not_replayed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    server = await start_server(settings)
    assert server.persistence is not None
    journal = server.persistence.file
    original = journal.append
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        pending = asyncio.create_task(
            reader.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"s", b">")
        )
        async with asyncio.timeout(5):
            while (
                not server.executor.waiters.count or not server.executor.waiters.pending[0].started
            ):
                await asyncio.sleep(0)
        calls = 0

        def fail_second(payload: bytes) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected group delivery append failure")
            original(payload)

        monkeypatch.setattr(journal, "append", fail_second)
        pushed = asyncio.create_task(writer.call(b"XADD", b"s", b"1-0", b"f", b"v"))
        with pytest.raises((ReferenceError, OSError)):
            await asyncio.wait_for(pending, 5)
        try:
            result = await asyncio.wait_for(pushed, 5)
            assert isinstance(result, Error)
        except (ReferenceError, OSError):
            pass
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XLEN", b"s") == Integer(1)
        assert (await client.call(b"XPENDING", b"s", b"g")).payload[0] == Integer(0)


async def test_multi_stream_delivery_replicates_after_partition(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / "primary"))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        for key in (b"a", b"b"):
            assert await writer.call(b"XADD", key, b"1-0", b"f", b"v") == Bulk(b"1-0")
            assert await writer.call(b"XGROUP", b"CREATE", key, b"g", b"0") == Simple(b"OK")
        replica = await start_server(
            config(
                tmp_path / "replica",
                replica_host="127.0.0.1",
                replica_port=primary.sockets[0].getsockname()[1],
            )
        )
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            async with Partition(state):
                result = await writer.call(
                    b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"a", b"b", b">", b">"
                )
                assert (
                    isinstance(result, Array)
                    and result.payload is not None
                    and len(result.payload) == 2
                )
            await state.wait_offset(node(primary).offset)
            assert state.partial_syncs == 1
            for key in (b"a", b"b"):
                assert await reader.call(b"XPENDING", key, b"g") == await writer.call(
                    b"XPENDING", key, b"g"
                )


async def test_exec_group_read_preflight_accounts_for_queued_additions(tmp_path: Path) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always", aof_max_record_bytes=1024
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(
            b"OK"
        )
        assert await client.call(b"MULTI") == Simple(b"OK")
        for number in range(1, 4):
            assert await client.call(b"XADD", b"s", f"{number}-0".encode(), b"f", b"v") == Simple(
                b"QUEUED"
            )
        assert await client.call(
            b"XREADGROUP", b"GROUP", b"g", b"c" * 300, b"STREAMS", b"s", b">"
        ) == Simple(b"QUEUED")
        reply = await client.call(b"EXEC")
        assert isinstance(reply, Error) and reply.payload.startswith(b"EXECABORT")
        assert await client.call(b"XLEN", b"s") == Integer(0)
        assert (await client.call(b"XPENDING", b"s", b"g")).payload[0] == Integer(0)
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XLEN", b"s") == Integer(0)


async def test_exec_group_reads_queued_writes_and_survives_compaction(tmp_path: Path) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always", snapshot_enabled=True
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        for key in (b"a", b"b"):
            assert await client.call(b"XGROUP", b"CREATE", key, b"g", b"0", b"MKSTREAM") == Simple(
                b"OK"
            )
        assert await client.call(b"MULTI") == Simple(b"OK")
        for key in (b"a", b"b"):
            assert await client.call(b"XADD", key, b"1-0", b"f", key) == Simple(b"QUEUED")
        assert await client.call(
            b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"a", b"b", b">", b">"
        ) == Simple(b"QUEUED")
        result = await client.call(b"EXEC")
        assert isinstance(result, Array) and result.payload is not None and len(result.payload) == 3
        assert result.payload[:2] == (Bulk(b"1-0"), Bulk(b"1-0"))
        assert isinstance(result.payload[2], Array) and len(result.payload[2].payload or ()) == 2
        assert server.persistence is not None and (await server.persistence.compact()).activated
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        for key in (b"a", b"b"):
            assert (await client.call(b"XPENDING", key, b"g")).payload[0] == Integer(1)
