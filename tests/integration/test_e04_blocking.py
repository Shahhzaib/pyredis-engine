from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import config as recovery_config
from _support.recovery import node
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence.errors import JournalError
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_immediate_key_priority_and_nonblocking_exec() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"RPUSH", b"b", b"one", b"two") == Integer(2)
        assert await client.call(b"LPUSH", b"a", b"first") == Integer(1)
        assert await client.call(b"BLPOP", b"a", b"b", b"0") == Array((Bulk(b"a"), Bulk(b"first")))
        assert await client.call(b"BRPOP", b"a", b"b", b"0") == Array((Bulk(b"b"), Bulk(b"two")))
        replies = await client.pipeline(
            [(b"MULTI",), (b"BLPOP", b"missing", b"0"), (b"SET", b"k", b"v"), (b"EXEC",)]
        )
        assert replies == [
            Simple(b"OK"),
            Simple(b"QUEUED"),
            Simple(b"QUEUED"),
            Array((Array(None), Simple(b"OK"))),
        ]


async def test_blocked_client_wakes_without_blocking_other_clients() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as consumer,
        peer(server.sockets[0].getsockname()[1]) as producer,
    ):
        pending = asyncio.create_task(consumer.call(b"BLPOP", b"q", b"0"))
        try:
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            assert await producer.call(b"PING") == Simple(b"PONG")
            assert await producer.call(b"RPUSH", b"q", b"\x00\xff") == Integer(1)
            assert await asyncio.wait_for(pending, 5) == Array((Bulk(b"q"), Bulk(b"\x00\xff")))
        finally:
            if not pending.done():
                pending.cancel()


async def test_temporary_wrong_type_does_not_wake_without_a_ready_list() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as consumer,
        peer(server.sockets[0].getsockname()[1]) as producer,
    ):
        pending = asyncio.create_task(consumer.call(b"BLPOP", b"first", b"second", b"0"))
        try:
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            assert await producer.call(b"SET", b"first", b"string") == Simple(b"OK")
            assert not pending.done()
            assert await producer.call(b"RPUSH", b"second", b"value") == Integer(1)
            result = await asyncio.wait_for(pending, 5)
            assert isinstance(result, Error) and result.payload.startswith(b"WRONGTYPE")
            assert await producer.call(b"LRANGE", b"second", b"0", b"-1") == Array(
                (Bulk(b"value"),)
            )
        finally:
            if not pending.done():
                pending.cancel()


async def test_fifo_waiters_wake_after_whole_transaction() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as first,
        peer(server.sockets[0].getsockname()[1]) as second,
        peer(server.sockets[0].getsockname()[1]) as producer,
    ):
        one = asyncio.create_task(first.call(b"BLPOP", b"q", b"0"))
        two: asyncio.Task[object] | None = None
        try:
            async with asyncio.timeout(5):
                while server.executor.waiters.count < 1:
                    await asyncio.sleep(0)
            two = asyncio.create_task(second.call(b"BLPOP", b"q", b"0"))
            async with asyncio.timeout(5):
                while server.executor.waiters.count < 2:
                    await asyncio.sleep(0)
            replies = await producer.pipeline(
                [(b"MULTI",), (b"RPUSH", b"q", b"a", b"b"), (b"EXEC",)]
            )
            assert replies[-1] == Array((Integer(2),))
            assert await asyncio.wait_for(one, 5) == Array((Bulk(b"q"), Bulk(b"a")))
            assert await asyncio.wait_for(two, 5) == Array((Bulk(b"q"), Bulk(b"b")))
            assert server.executor.waiters.count == 0
        finally:
            if not one.done():
                one.cancel()
            if two is not None and not two.done():
                two.cancel()


async def test_rejected_exec_does_not_wake_waiter_or_commit_push() -> None:
    server = await start_server(ServerConfig(port=0, max_reply_bytes=128))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as consumer,
        peer(server.sockets[0].getsockname()[1]) as producer,
    ):
        pending = asyncio.create_task(consumer.call(b"BLPOP", b"q", b"0"))
        try:
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            replies = await producer.pipeline(
                [(b"MULTI",), (b"RPUSH", b"q", b"lost"), (b"ECHO", b"x" * 120), (b"EXEC",)]
            )
            assert isinstance(replies[-1], Error) and replies[-1].payload.startswith(b"EXECABORT")
            assert not pending.done()
            assert await producer.call(b"RPUSH", b"q", b"kept") == Integer(1)
            assert await asyncio.wait_for(pending, 5) == Array((Bulk(b"q"), Bulk(b"kept")))
        finally:
            if not pending.done():
                pending.cancel()


async def test_many_waiters_keep_fifo_order() -> None:
    from contextlib import AsyncExitStack

    server = await start_server(ServerConfig(port=0))
    async with server, AsyncExitStack() as stack:
        port = server.sockets[0].getsockname()[1]
        consumers = [await stack.enter_async_context(peer(port)) for _ in range(32)]
        producer = await stack.enter_async_context(peer(port))
        tasks: list[asyncio.Task[object]] = []
        try:
            for index, consumer in enumerate(consumers):
                tasks.append(asyncio.create_task(consumer.call(b"BLPOP", b"q", b"0")))
                async with asyncio.timeout(5):
                    while server.executor.waiters.count < index + 1:
                        await asyncio.sleep(0)
            values = [str(index).encode() for index in range(len(consumers))]
            assert await producer.call(b"RPUSH", b"q", *values) == Integer(len(values))
            assert await asyncio.wait_for(asyncio.gather(*tasks), 5) == [
                Array((Bulk(b"q"), Bulk(value))) for value in values
            ]
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()


async def test_concurrent_producers_and_consumers_deliver_each_value_once() -> None:
    from contextlib import AsyncExitStack

    server = await start_server(ServerConfig(port=0))
    async with server, AsyncExitStack() as stack:
        port = server.sockets[0].getsockname()[1]
        consumers = [await stack.enter_async_context(peer(port)) for _ in range(20)]
        producers = [await stack.enter_async_context(peer(port)) for _ in range(20)]
        blocked = [asyncio.create_task(client.call(b"BLPOP", b"q", b"0")) for client in consumers]
        try:
            async with asyncio.timeout(5):
                while server.executor.waiters.count < len(consumers):
                    await asyncio.sleep(0)
            values = [str(number).encode() for number in range(len(producers))]
            replies = await asyncio.gather(
                *(
                    client.call(b"RPUSH", b"q", value)
                    for client, value in zip(producers, values, strict=True)
                )
            )
            assert all(reply == Integer(1) for reply in replies)
            results = await asyncio.wait_for(asyncio.gather(*blocked), 5)
            assert {
                reply.payload[1].payload
                for reply in results
                if isinstance(reply, Array)
                and reply.payload is not None
                and isinstance(reply.payload[1], Bulk)
            } == set(values)
            assert server.executor.waiters.count == 0
        finally:
            for task in blocked:
                if not task.done():
                    task.cancel()


async def test_timeout_and_disconnect_cleanup_with_buffered_pipeline() -> None:
    server = await start_server(ServerConfig(port=0, read_timeout=0.1))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"BLPOP", b"missing", b"0.05") == Array(None)
        assert await client.call(b"PING") == Simple(b"PONG")
        from pyredis_engine.protocol import serialize_reply

        client.writer.write(
            serialize_reply(Array((Bulk(b"BLPOP"), Bulk(b"q"), Bulk(b"0"))))
            + serialize_reply(Array((Bulk(b"PING"),)))
        )
        await client.writer.drain()
        async with asyncio.timeout(5):
            while server.executor.waiters.count == 0:
                await asyncio.sleep(0)
        client.writer.transport.abort()
        async with asyncio.timeout(5):
            while server.executor.waiters.count:
                await asyncio.sleep(0)


async def test_buffer_overflow_while_blocked_closes_and_records_protocol_error() -> None:
    settings = ServerConfig(
        port=0, max_bulk_bytes=64, max_frame_bytes=128, max_header_bytes=32, max_input_bytes=66
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        from pyredis_engine.protocol import serialize_reply

        client.writer.write(serialize_reply(Array((Bulk(b"BLPOP"), Bulk(b"q"), Bulk(b"0")))))
        await client.writer.drain()
        async with asyncio.timeout(5):
            while not server.executor.waiters.count:
                await asyncio.sleep(0)
        client.writer.write(b"x" * 67)
        await client.writer.drain()
        async with asyncio.timeout(5):
            while server.executor.waiters.count:
                await asyncio.sleep(0)
        assert server.metrics()["protocol_errors"] == 1


async def test_disconnect_while_initial_waiter_awaits_commit_lock(tmp_path: Path) -> None:
    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal")))
    assert server.persistence is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        lock = server.persistence.lock
        await lock.acquire()
        try:
            from pyredis_engine.protocol import serialize_reply

            client.writer.write(serialize_reply(Array((Bulk(b"BLPOP"), Bulk(b"q"), Bulk(b"0")))))
            await client.writer.drain()
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            client.writer.transport.abort()
            async with asyncio.timeout(2):
                while server.executor.waiters.count:
                    await asyncio.sleep(0)
        finally:
            lock.release()


async def test_disconnected_waiters_do_not_accumulate_lock_tasks(tmp_path: Path) -> None:
    from pyredis_engine.protocol import serialize_reply

    server = await start_server(ServerConfig(port=0, aof_path=str(tmp_path / "journal")))
    assert server.persistence is not None
    async with server:
        owner = server.persistence
        await owner.lock.acquire()
        try:
            for _ in range(12):
                async with peer(server.sockets[0].getsockname()[1]) as client:
                    client.writer.write(
                        serialize_reply(Array((Bulk(b"BLPOP"), Bulk(b"q"), Bulk(b"0"))))
                    )
                    await client.writer.drain()
                    async with asyncio.timeout(5):
                        while not server.executor.waiters.count:
                            await asyncio.sleep(0)
                    client.writer.transport.abort()
                    async with asyncio.timeout(5):
                        while server.executor.waiters.count:
                            await asyncio.sleep(0)
            assert len(owner.pending) <= 1
        finally:
            owner.lock.release()


async def test_acl_revocation_wakes_blocked_client_without_mutation(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / "acl", ["on", "+blpop", "~allowed:*"])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as client,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await client.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        pending = asyncio.create_task(client.call(b"BLPOP", b"allowed:q", b"0"))
        try:
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            assert await admin.call(b"ACL", b"SETUSER", b"app", b"-blpop") == Simple(b"OK")
            denied = await asyncio.wait_for(pending, 5)
            assert isinstance(denied, Error) and denied.payload.startswith(b"NOPERM")
            assert await admin.call(b"EXISTS", b"allowed:q") == Integer(0)
        finally:
            if not pending.done():
                pending.cancel()


async def test_failed_waiter_journal_append_stops_without_acknowledging_pop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    server = await start_server(settings)
    assert server.persistence is not None
    journal = server.persistence.file
    original = journal.append
    calls = 0

    def fail_second(payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected waiter append failure")
        original(payload)

    monkeypatch.setattr(journal, "append", fail_second)
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as client,
        peer(server.sockets[0].getsockname()[1]) as producer,
    ):
        pending = asyncio.create_task(client.call(b"BLPOP", b"q", b"0"))
        try:
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            pushed = asyncio.create_task(producer.call(b"RPUSH", b"q", b"value"))
            with pytest.raises((ReferenceError, OSError)):
                await asyncio.wait_for(pending, 5)
            try:
                reply = await asyncio.wait_for(pushed, 5)
                assert isinstance(reply, Error)
            except (ReferenceError, OSError):
                pass
        finally:
            if not pending.done():
                pending.cancel()
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"LRANGE", b"q", b"0", b"-1") == Array((Bulk(b"value"),))


async def test_waiter_reply_waits_for_its_journal_barrier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = await start_server(
        ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    )
    assert server.persistence is not None
    journal = server.persistence.file
    original = journal.append
    entered, release = threading.Event(), threading.Event()
    calls = 0

    def pause_second(payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            assert release.wait(5)
        original(payload)

    monkeypatch.setattr(journal, "append", pause_second)
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as client,
        peer(server.sockets[0].getsockname()[1]) as producer,
    ):
        pending = asyncio.create_task(client.call(b"BLPOP", b"q", b"0"))
        pushed: asyncio.Task[object] | None = None
        try:
            async with asyncio.timeout(5):
                while not server.executor.waiters.count:
                    await asyncio.sleep(0)
            pushed = asyncio.create_task(producer.call(b"RPUSH", b"q", b"v"))
            assert await asyncio.to_thread(entered.wait, 5)
            assert not pending.done() and not pushed.done()
        finally:
            release.set()
        assert await asyncio.wait_for(pending, 5) == Array((Bulk(b"q"), Bulk(b"v")))
        assert pushed is not None and await asyncio.wait_for(pushed, 5) == Integer(1)


async def test_waiter_pop_replicates_after_committed_producer(tmp_path: Path) -> None:
    primary = await start_server(recovery_config(tmp_path / "primary.aof"))
    async with primary:
        replica = await start_server(
            recovery_config(
                tmp_path / "replica.aof",
                replica_host="127.0.0.1",
                replica_port=primary.sockets[0].getsockname()[1],
            )
        )
        async with (
            replica,
            peer(primary.sockets[0].getsockname()[1]) as consumer,
            peer(primary.sockets[0].getsockname()[1]) as producer,
            peer(replica.sockets[0].getsockname()[1]) as reader,
        ):
            await node(replica).wait_ready()
            pending = asyncio.create_task(consumer.call(b"BRPOP", b"q", b"0"))
            try:
                async with asyncio.timeout(5):
                    while not primary.executor.waiters.count:
                        await asyncio.sleep(0)
                assert await producer.call(b"RPUSH", b"q", b"a", b"b") == Integer(2)
                assert await asyncio.wait_for(pending, 5) == Array((Bulk(b"q"), Bulk(b"b")))
                await node(replica).wait_offset(node(primary).offset)
                assert await reader.call(b"LRANGE", b"q", b"0", b"-1") == Array((Bulk(b"a"),))
                assert node(replica).offset == node(primary).offset
            finally:
                if not pending.done():
                    pending.cancel()


async def test_replication_callback_failure_does_not_acknowledge_pop(tmp_path: Path) -> None:
    settings = recovery_config(tmp_path / "primary.aof")
    primary = await start_server(settings)
    assert primary.persistence is not None
    owner = primary.persistence
    original = owner.on_commit
    calls = 0

    def fail_pop(payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise JournalError("injected replication callback failure")
        original(payload)

    owner.on_commit = fail_pop
    async with (
        primary,
        peer(primary.sockets[0].getsockname()[1]) as consumer,
        peer(primary.sockets[0].getsockname()[1]) as producer,
    ):
        pending = asyncio.create_task(consumer.call(b"BLPOP", b"q", b"0"))
        try:
            async with asyncio.timeout(5):
                while not primary.executor.waiters.count:
                    await asyncio.sleep(0)
            pushed = asyncio.create_task(producer.call(b"RPUSH", b"q", b"v"))
            with pytest.raises((ReferenceError, OSError)):
                await asyncio.wait_for(pending, 5)
            try:
                reply = await asyncio.wait_for(pushed, 5)
                assert isinstance(reply, Error)
            except (ReferenceError, OSError):
                pass
        finally:
            if not pending.done():
                pending.cancel()
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        # The pop's journal append completed before the injected callback failed.
        assert await client.call(b"LLEN", b"q") == Integer(0)


async def test_waiter_limits_and_reply_limit_leave_list_unchanged() -> None:
    server = await start_server(ServerConfig(port=0, max_reply_bytes=128))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        many = [str(index).encode() for index in range(65)]
        result = await client.call(b"BLPOP", *many, b"0")
        assert isinstance(result, Error) and b"waiter limits" in result.payload
        key = b"k" * 120
        assert await client.call(b"RPUSH", key, b"value") == Integer(1)
        result = await client.call(b"BLPOP", key, b"0.0000001")
        assert isinstance(result, Error) and b"reply exceeds" in result.payload
        assert await client.call(b"LLEN", key) == Integer(1)
