from __future__ import annotations

import pytest
from _support.comparison import Peer, peer
from _support.pubsub import ack, message, read

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


async def scan(client: Peer, *prefix: bytes, options: tuple[bytes, ...] = ()) -> list[bytes]:
    result: list[bytes] = []
    cursor = b"0"
    for _ in range(100):
        reply = await client.call(*prefix, cursor, *options)
        assert isinstance(reply, Array) and reply.payload is not None
        next_cursor, values = reply.payload
        assert isinstance(next_cursor, Bulk) and next_cursor.payload is not None
        assert isinstance(values, Array) and values.payload is not None
        for value in values.payload:
            assert isinstance(value, Bulk) and value.payload is not None
            result.append(value.payload)
        cursor = next_cursor.payload
        if cursor == b"0":
            return result
    pytest.fail("cursor failed to terminate")


@pytest.mark.parametrize("name", [b"SCAN", b"HSCAN", b"SSCAN", b"ZSCAN"])
async def test_scan_family_stable_data_and_match(reference_port: int, name: bytes) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for connection in (local, ref):
            assert await connection.call(b"FLUSHDB") == Simple(b"OK")
            assert await connection.call(b"SET", b"a:\x00", b"v") == Simple(b"OK")
            await connection.call(b"SET", b"b", b"v")
            await connection.call(b"HSET", b"h", b"a:\x00", b"v1", b"a:\xff", b"v2")
            await connection.call(b"SADD", b"s", b"a:\x00", b"a:\xff")
            await connection.call(b"ZADD", b"z", b"1.5", b"a:\x00", b"-inf", b"a:\xff")
        key = {b"HSCAN": b"h", b"SSCAN": b"s", b"ZSCAN": b"z"}.get(name)
        prefix = (name,) if key is None else (name, key)
        options = (b"MATCH", b"a:*", b"COUNT", b"1")
        local_values = await scan(local, *prefix, options=options)
        ref_values = await scan(ref, *prefix, options=options)
        if name in (b"HSCAN", b"ZSCAN"):
            assert dict(zip(local_values[::2], local_values[1::2], strict=True)) == dict(
                zip(ref_values[::2], ref_values[1::2], strict=True)
            )
        else:
            assert set(local_values) == set(ref_values)


async def test_scan_type_filter(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for connection in (local, ref):
            await connection.call(b"FLUSHDB")
            await connection.call(b"SET", b"string", b"v")
            await connection.call(b"SADD", b"set", b"v")
        options = (b"TYPE", b"set", b"COUNT", b"1")
        assert (
            set(await scan(local, b"SCAN", options=options))
            == set(await scan(ref, b"SCAN", options=options))
            == {b"set"}
        )


async def test_pattern_delivery_and_mixed_subscription_counts(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as sub, peer(port) as pub:
                assert await sub.call(b"SUBSCRIBE", b"a:\xff") == ack(b"subscribe", b"a:\xff", 1)
                assert await sub.call(b"PSUBSCRIBE", b"a:*") == ack(b"psubscribe", b"a:*", 2)
                assert await pub.call(b"PUBLISH", b"a:\xff", b"\x00") == Integer(2)
                assert await read(sub, 2) == [
                    message(b"a:\xff", b"\x00"),
                    Array((Bulk(b"pmessage"), Bulk(b"a:*"), Bulk(b"a:\xff"), Bulk(b"\x00"))),
                ]
                assert await sub.call(b"PUNSUBSCRIBE") == ack(b"punsubscribe", b"a:*", 1)
                assert await sub.call(b"UNSUBSCRIBE") == ack(b"unsubscribe", b"a:\xff", 0)


@pytest.mark.parametrize(
    "pattern,channel,matched",
    [
        (b"v:[a-c]?", b"v:b\xff", True),
        (b"v:[^a-c]?", b"v:z\x00", True),
        (b"v:[^a-c]?", b"v:b\x00", False),
        (b"v:\\*", b"v:*", True),
        (b"", b"", True),
    ],
)
async def test_pattern_binary_globs_against_redis(
    reference_port: int, pattern: bytes, channel: bytes, matched: bool
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as sub, peer(port) as pub:
                assert await sub.call(b"PSUBSCRIBE", pattern) == ack(b"psubscribe", pattern, 1)
                assert await pub.call(b"PUBLISH", channel, b"\x00\xff") == Integer(int(matched))
                if matched:
                    assert await read(sub) == [
                        Array((Bulk(b"pmessage"), Bulk(pattern), Bulk(channel), Bulk(b"\x00\xff")))
                    ]
                assert await sub.call(b"PUNSUBSCRIBE", pattern) == ack(b"punsubscribe", pattern, 0)
