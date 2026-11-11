from __future__ import annotations

import asyncio

import pytest
from redis.asyncio import Redis

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_redispy_consumer_admin_pending_and_claim() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        client = Redis(
            host="127.0.0.1", port=server.sockets[0].getsockname()[1], protocol=2, socket_timeout=5
        )
        try:
            assert await client.xadd(b"s", {b"f": b"\x00\xff"}, id=b"1-0") == b"1-0"
            assert await client.xgroup_create(b"s", b"g", id=b"0")
            assert await client.xgroup_createconsumer(b"s", b"g", b"a") == 1
            assert await client.xreadgroup(b"g", b"a", {b"s": b">"}) == [
                [b"s", [(b"1-0", {b"f": b"\x00\xff"})]]
            ]
            assert await client.xclaim(b"s", b"g", b"b", 0, [b"1-0"], justid=True) == [b"1-0"]
            pending = await client.xpending_range(b"s", b"g", min=b"-", max=b"+", count=10)
            assert len(pending) == 1 and pending[0]["message_id"] == b"1-0"
            assert pending[0]["consumer"] == b"b" and pending[0]["times_delivered"] == 1
            result = await client.xautoclaim(b"s", b"g", b"c", 0, b"0-0", count=1)
            assert result == [b"0-0", [(b"1-0", {b"f": b"\x00\xff"})], []]
            assert await client.xgroup_delconsumer(b"s", b"g", b"c") == 1
            assert (await client.xpending(b"s", b"g"))["pending"] == 0
        finally:
            await client.aclose()


async def test_redispy_blocking_multi_stream_group_read() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        reader = Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=5)
        writer = Redis(host="127.0.0.1", port=port, protocol=2, socket_timeout=5)
        try:
            assert await writer.xgroup_create(b"a", b"g", id=b"0", mkstream=True)
            assert await writer.xgroup_create(b"b", b"g", id=b"0", mkstream=True)
            pending = asyncio.create_task(
                reader.xreadgroup(b"g", b"c", {b"a": b">", b"b": b">"}, count=1, block=0)
            )
            async with asyncio.timeout(5):
                while (
                    not server.executor.waiters.count
                    or not server.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            assert await writer.xadd(b"b", {b"f": b"\x00\xff"}, id=b"1-0") == b"1-0"
            assert await asyncio.wait_for(pending, 5) == [[b"b", [(b"1-0", {b"f": b"\x00\xff"})]]]
        finally:
            await reader.aclose()
            await writer.aclose()
