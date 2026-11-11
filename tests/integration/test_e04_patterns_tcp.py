from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.pubsub import ack, message, read
from _support.recovery import Partition, config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_pattern_binary_delivery_counts_and_cleanup() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as sub, peer(port) as pub:
            assert await sub.call(b"SUBSCRIBE", b"x:\xff") == ack(b"subscribe", b"x:\xff", 1)
            assert await sub.call(b"PSUBSCRIBE", b"x:*") == ack(b"psubscribe", b"x:*", 2)
            assert await pub.call(b"PUBLISH", b"x:\xff", b"\x00\xff") == Integer(2)
            assert await read(sub, 2) == [
                message(b"x:\xff", b"\x00\xff"),
                Array((Bulk(b"pmessage"), Bulk(b"x:*"), Bulk(b"x:\xff"), Bulk(b"\x00\xff"))),
            ]
            assert isinstance(await sub.call(b"GET", b"key"), Error)
            assert await sub.call(b"UNSUBSCRIBE") == ack(b"unsubscribe", b"x:\xff", 1)
            assert await sub.call(b"PING") == Array((Bulk(b"pong"), Bulk(b"")))
            assert await sub.call(b"PUNSUBSCRIBE") == ack(b"punsubscribe", b"x:*", 0)
            assert await sub.call(b"PING") == Simple(b"PONG")
    assert not server.executor.pubsub.channels and not server.executor.pubsub.patterns


async def test_pattern_acl_exact_grant_and_revocation(tmp_path: Path) -> None:
    rules = ["on", "+psubscribe", "+punsubscribe", "&a:*"]
    server = await start_server(ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl", rules)))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await app.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        assert isinstance(await app.call(b"PSUBSCRIBE", b"*"), Error)
        assert isinstance(await app.call(b"PSUBSCRIBE", b"a:1"), Error)
        assert await app.call(b"PSUBSCRIBE", b"a:*") == ack(b"psubscribe", b"a:*", 1)
        assert await admin.call(b"ACL", b"SETUSER", b"app", b"resetchannels") == Simple(b"OK")
        assert await asyncio.wait_for(app.reader.read(), 5) == b""
        assert await admin.call(b"PUBLISH", b"a:1", b"secret") == Integer(0)


async def test_disconnected_replica_keeps_local_pattern_pubsub(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / "primary.aof"))
    async with primary:
        replica = await start_server(
            config(
                tmp_path / "replica.aof",
                replica_host="127.0.0.1",
                replica_port=primary.sockets[0].getsockname()[1],
            )
        )
        async with (
            replica,
            peer(replica.sockets[0].getsockname()[1]) as sub,
            peer(replica.sockets[0].getsockname()[1]) as pub,
        ):
            state = node(replica)
            await state.wait_ready()
            async with Partition(state):
                assert await sub.call(b"PSUBSCRIBE", b"local:*") == ack(
                    b"psubscribe", b"local:*", 1
                )
                assert await pub.call(b"PUBLISH", b"local:1", b"message") == Integer(1)
                assert await read(sub) == [
                    Array((Bulk(b"pmessage"), Bulk(b"local:*"), Bulk(b"local:1"), Bulk(b"message")))
                ]
                assert await sub.call(b"PUNSUBSCRIBE") == ack(b"punsubscribe", b"local:*", 0)


async def test_concurrent_publishers_preserve_order_for_pattern_subscribers() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as first, peer(port) as second:
            for subscriber in (first, second):
                assert await subscriber.call(b"PSUBSCRIBE", b"events:*") == ack(
                    b"psubscribe", b"events:*", 1
                )

            async def publish(number: int) -> None:
                async with peer(port) as client:
                    commands = [
                        (b"PUBLISH", b"events:\xff", f"{number}:{n}".encode()) for n in range(5)
                    ]
                    assert await client.pipeline(commands) == [Integer(2)] * 5

            received = [asyncio.create_task(read(client, 25)) for client in (first, second)]
            await asyncio.gather(*(publish(number) for number in range(5)))
            deliveries = await asyncio.gather(*received)
            assert deliveries[0] == deliveries[1]
            assert set(deliveries[0]) == {
                Array(
                    (
                        Bulk(b"pmessage"),
                        Bulk(b"events:*"),
                        Bulk(b"events:\xff"),
                        Bulk(f"{number}:{n}".encode()),
                    )
                )
                for number in range(5)
                for n in range(5)
            }
    assert not server.executor.pubsub.patterns
