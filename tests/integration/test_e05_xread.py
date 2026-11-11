from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def result(key: bytes, identity: bytes, field: bytes, value: bytes) -> Array:
    return Array(
        (Array((Bulk(key), Array((Array((Bulk(identity), Array((Bulk(field), Bulk(value))))),)))),)
    )


async def test_xread_block_dollar_wakes_two_readers_without_consuming() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as one,
        peer(server.sockets[0].getsockname()[1]) as two,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"old") == Bulk(b"1-0")
        tasks = [
            asyncio.create_task(client.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"$"))
            for client in (one, two)
        ]
        try:
            async with asyncio.timeout(5):
                while server.executor.waiters.count < 2 or not all(
                    waiter.started for waiter in server.executor.waiters.pending
                ):
                    await asyncio.sleep(0)
            assert await writer.call(b"XADD", b"s", b"2-0", b"\x00", b"\xff") == Bulk(b"2-0")
            assert await asyncio.gather(*tasks) == [result(b"s", b"2-0", b"\x00", b"\xff")] * 2
            assert await writer.call(b"XLEN", b"s") == Integer(2)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()


async def test_xread_new_stream_timeout_and_transaction_nonblocking() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        assert await reader.call(b"XREAD", b"BLOCK", b"1", b"STREAMS", b"missing", b"0") == Array(
            None
        )
        pending = asyncio.create_task(
            reader.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"new", b"$")
        )
        try:
            async with asyncio.timeout(5):
                while (
                    not server.executor.waiters.count
                    or not server.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            assert await writer.call(b"XADD", b"new", b"1-0", b"f", b"v") == Bulk(b"1-0")
            assert await asyncio.wait_for(pending, 5) == result(b"new", b"1-0", b"f", b"v")
        finally:
            if not pending.done():
                pending.cancel()
        replies = await writer.pipeline(
            [(b"MULTI",), (b"XREAD", b"BLOCK", b"0", b"STREAMS", b"absent", b"0"), (b"EXEC",)]
        )
        assert replies[-1] == Array((Array(None),))


async def test_xread_acl_multiple_keys_and_revocation(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / "acl", ["on", "+xread", "~allowed:*"])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as reader,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await reader.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        denied = await reader.call(b"XREAD", b"STREAMS", b"allowed:s", b"private:s", b"0", b"0")
        assert isinstance(denied, Error) and denied.payload.startswith(b"NOPERM")
        pending = asyncio.create_task(
            reader.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"allowed:s", b"$")
        )
        try:
            async with asyncio.timeout(5):
                while (
                    not server.executor.waiters.count
                    or not server.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            assert await admin.call(b"ACL", b"SETUSER", b"app", b"-xread") == Simple(b"OK")
            denied = await asyncio.wait_for(pending, 5)
            assert isinstance(denied, Error) and denied.payload.startswith(b"NOPERM")
        finally:
            if not pending.done():
                pending.cancel()


async def test_blocked_xread_observes_whole_durable_transaction(tmp_path: Path) -> None:
    server = await start_server(
        ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    )
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        pending = asyncio.create_task(
            reader.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"a", b"b", b"$", b"$")
        )
        try:
            async with asyncio.timeout(5):
                while (
                    not server.executor.waiters.count
                    or not server.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            replies = await writer.pipeline(
                [
                    (b"MULTI",),
                    (b"XADD", b"b", b"1-0", b"f", b"b"),
                    (b"XADD", b"a", b"1-0", b"f", b"a"),
                    (b"EXEC",),
                ]
            )
            assert replies[-1] == Array((Bulk(b"1-0"), Bulk(b"1-0")))
            expected_a = result(b"a", b"1-0", b"f", b"a").payload
            expected_b = result(b"b", b"1-0", b"f", b"b").payload
            assert expected_a is not None and expected_b is not None
            assert await asyncio.wait_for(pending, 5) == Array((*expected_a, *expected_b))
        finally:
            if not pending.done():
                pending.cancel()


async def test_rejected_exec_does_not_wake_stream_reader() -> None:
    server = await start_server(ServerConfig(port=0, max_reply_bytes=128))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        pending = asyncio.create_task(reader.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"$"))
        try:
            async with asyncio.timeout(5):
                while (
                    not server.executor.waiters.count
                    or not server.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            replies = await writer.pipeline(
                [
                    (b"MULTI",),
                    (b"XADD", b"s", b"1-0", b"f", b"discard"),
                    (b"ECHO", b"x" * 120),
                    (b"EXEC",),
                ]
            )
            assert isinstance(replies[-1], Error) and replies[-1].payload.startswith(b"EXECABORT")
            assert not pending.done()
            assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"kept") == Bulk(b"1-0")
            assert await asyncio.wait_for(pending, 5) == result(b"s", b"1-0", b"f", b"kept")
        finally:
            if not pending.done():
                pending.cancel()


async def test_disconnected_stream_reader_is_removed_before_new_entry() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as reader,
        peer(server.sockets[0].getsockname()[1]) as writer,
    ):
        from pyredis_engine.protocol import serialize_reply

        frame = Array(
            (Bulk(b"XREAD"), Bulk(b"BLOCK"), Bulk(b"0"), Bulk(b"STREAMS"), Bulk(b"s"), Bulk(b"$"))
        )
        reader.writer.write(serialize_reply(frame))
        await reader.writer.drain()
        async with asyncio.timeout(5):
            while not server.executor.waiters.count:
                await asyncio.sleep(0)
        reader.writer.transport.abort()
        async with asyncio.timeout(5):
            while server.executor.waiters.count:
                await asyncio.sleep(0)
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await writer.call(b"XLEN", b"s") == Integer(1)


async def test_concurrent_writers_generate_unique_ordered_same_millisecond_ids() -> None:
    from contextlib import AsyncExitStack

    server = await start_server(ServerConfig(port=0), clock=ManualClock(1000))
    async with server, AsyncExitStack() as stack:
        port = server.sockets[0].getsockname()[1]
        writers = [await stack.enter_async_context(peer(port)) for _ in range(25)]
        replies = await asyncio.gather(
            *(
                writer.call(b"XADD", b"s", b"*", b"f", str(index).encode())
                for index, writer in enumerate(writers)
            )
        )
        assert set(replies) == {Bulk(f"1000-{number}".encode()) for number in range(25)}
        assert await writers[0].call(b"XLEN", b"s") == Integer(25)
