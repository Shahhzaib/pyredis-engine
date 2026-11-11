from __future__ import annotations

import pytest
from redis.asyncio import Redis

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_redispy_scan_iteration_and_pattern_pubsub() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        client = Redis(
            host="127.0.0.1", port=server.sockets[0].getsockname()[1], protocol=2, socket_timeout=5
        )
        try:
            for key in (b"item:\x00", b"item:\xff", b"other"):
                assert await client.set(key, b"value")
            seen = {key async for key in client.scan_iter(match=b"item:*", count=1)}
            assert seen == {b"item:\x00", b"item:\xff"}
            async with client.pubsub() as sub:
                await sub.psubscribe(b"news:*")
                confirmation = await sub.get_message(timeout=5)
                assert confirmation is not None and confirmation["type"] == "psubscribe"
                assert await client.publish(b"news:\xff", b"\x00") == 1
                event = await sub.get_message(timeout=5)
                assert event is not None and event["type"] == "pmessage"
                assert event["pattern"] == b"news:*" and event["data"] == b"\x00"
                await sub.punsubscribe()
        finally:
            await client.aclose()
