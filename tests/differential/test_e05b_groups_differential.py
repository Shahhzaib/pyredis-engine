from __future__ import annotations

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize(
    "commands",
    [
        [
            (b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM"),
            (b"XGROUP", b"CREATE", b"s", b"g", b"0"),
            (b"XPENDING", b"s", b"g"),
            (b"XADD", b"s", b"1-0", b"f", b"v"),
            (b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"),
            (b"XPENDING", b"s", b"g"),
            (b"XDEL", b"s", b"1-0"),
            (b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b"0"),
            (b"XACK", b"s", b"g", b"1-0"),
            (b"XPENDING", b"s", b"g"),
            (b"XGROUP", b"DESTROY", b"s", b"g"),
            (b"XGROUP", b"DESTROY", b"s", b"g"),
        ],
        [
            (b"XADD", b"s", b"1-0", b"f", b"a"),
            (b"XADD", b"s", b"2-0", b"f", b"b"),
            (b"XGROUP", b"CREATE", b"s", b"g", b"0"),
            (b"XREADGROUP", b"GROUP", b"g", b"alice", b"COUNT", b"1", b"STREAMS", b"s", b">"),
            (b"XREADGROUP", b"GROUP", b"g", b"bob", b"COUNT", b"1", b"STREAMS", b"s", b">"),
            (b"XPENDING", b"s", b"g"),
            (b"XREADGROUP", b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"),
            (b"XACK", b"s", b"g", b"1-0", b"2-0"),
            (b"XPENDING", b"s", b"g"),
        ],
        [
            (b"XADD", b"s", b"1-0", b"f", b"v"),
            (b"XADD", b"s", b"2-0", b"f", b"w"),
            (b"XADD", b"s", b"3-0", b"f", b"x"),
            (b"XGROUP", b"CREATE", b"s", b"g", b"0"),
            (b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"),
            (b"XDEL", b"s", b"1-0"),
            (b"XAUTOCLAIM", b"s", b"g", b"b", b"999999", b"0-0", b"COUNT", b"1", b"JUSTID"),
            (b"XAUTOCLAIM", b"s", b"g", b"b", b"0", b"2-0", b"COUNT", b"1", b"JUSTID"),
            (b"XAUTOCLAIM", b"s", b"g", b"b", b"0", b"3-0", b"COUNT", b"1"),
            (b"XPENDING", b"s", b"g"),
        ],
        [
            (b"XADD", b"s", b"1-0", b"f", b"v"),
            (b"XGROUP", b"CREATE", b"s", b"g", b"$"),
            (b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"),
            (b"XGROUP", b"SETID", b"s", b"g", b"0"),
            (b"XREADGROUP", b"GROUP", b"g", b"c", b"NOACK", b"STREAMS", b"s", b">"),
            (b"XPENDING", b"s", b"g"),
        ],
        [
            (b"XGROUP", b"CREATE", b"s", b"g", b"0"),
            (b"XPENDING", b"s", b"g"),
            (b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"),
            (b"XACK", b"s", b"g", b"1-0"),
            (b"XGROUP", b"DESTROY", b"s", b"g"),
        ],
        [
            (b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM"),
            (b"XGROUP", b"CREATECONSUMER", b"s", b"g", b"a"),
            (b"XGROUP", b"CREATECONSUMER", b"s", b"g", b"a"),
            (b"XADD", b"s", b"1-0", b"f", b"v"),
            (b"XADD", b"s", b"2-0", b"f", b"w"),
            (b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"),
            (b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"JUSTID"),
            (b"XPENDING", b"s", b"g"),
            (b"XGROUP", b"DELCONSUMER", b"s", b"g", b"b"),
            (b"XGROUP", b"DELCONSUMER", b"s", b"g", b"b"),
            (b"XPENDING", b"s", b"g", b"IDLE", b"9999999", b"-", b"+", b"10"),
            (b"XDEL", b"s", b"2-0"),
            (b"XCLAIM", b"s", b"g", b"b", b"0", b"2-0"),
            (b"XPENDING", b"s", b"g"),
        ],
    ],
)
async def test_groups_against_redis(reference_port: int, commands: list[tuple[bytes, ...]]) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        assert await local.call(b"FLUSHDB") == await ref.call(b"FLUSHDB")
        for command in commands:
            assert await local.call(*command) == await ref.call(*command), command


async def test_extended_pending_shapes_and_idle_filter_against_redis(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as client:
                assert await client.call(b"FLUSHDB") == Simple(b"OK")
                assert await client.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
                assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
                assert isinstance(
                    await client.call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"),
                    Array,
                )
                reply = await client.call(b"XPENDING", b"s", b"g", b"-", b"+", b"10", b"a")
                assert (
                    isinstance(reply, Array)
                    and reply.payload is not None
                    and len(reply.payload) == 1
                )
                row = reply.payload[0]
                assert isinstance(row, Array) and row.payload is not None
                assert row.payload[:2] == (Bulk(b"1-0"), Bulk(b"a"))
                assert isinstance(row.payload[2], Integer) and row.payload[2].payload >= 0
                assert row.payload[3] == Integer(1)
                assert await client.call(
                    b"XPENDING", b"s", b"g", b"IDLE", b"999999", b"-", b"+", b"10"
                ) == Array(())
