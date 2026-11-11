from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


async def test_multi_stream_and_exec_against_redis(reference_port: int) -> None:
    commands = [
        (b"FLUSHDB",),
        (b"XGROUP", b"CREATE", b"a", b"g", b"0", b"MKSTREAM"),
        (b"XGROUP", b"CREATE", b"b", b"g", b"0", b"MKSTREAM"),
        (b"XADD", b"a", b"1-0", b"f", b"\x00"),
        (b"XADD", b"b", b"2-0", b"f", b"\xff"),
        (b"XREADGROUP", b"GROUP", b"g", b"c", b"COUNT", b"1", b"STREAMS", b"a", b"b", b">", b">"),
        (b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"a", b"b", b"0", b"0"),
        (b"MULTI",),
        (b"XREADGROUP", b"GROUP", b"g", b"d", b"BLOCK", b"0", b"STREAMS", b"a", b"b", b">", b">"),
        (b"EXEC",),
        (b"XREADGROUP", b"GROUP", b"g", b"d", b"BLOCK", b"10", b"STREAMS", b"a", b"b", b">", b">"),
        (b"XREADGROUP", b"GROUP", b"g", b"d", b"STREAMS", b"a", b"missing", b">", b">"),
        (b"XREADGROUP", b"GROUP", b"g", b"d", b"STREAMS", b"missing", b"bad"),
        (b"XREADGROUP", b"GROUP", b"g", b"d", b"STREAMS", b"a", b"b", b">"),
        (b"XPENDING", b"a", b"g"),
        (b"XGROUP", b"CREATE", b"a", b"duplicate", b"0"),
        (b"XADD", b"a", b"3-0", b"f", b"v"),
        (
            b"XREADGROUP",
            b"GROUP",
            b"duplicate",
            b"c",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"a",
            b"a",
            b">",
            b">",
        ),
        (b"XPENDING", b"a", b"duplicate"),
        (b"XGROUP", b"CREATE", b"b", b"noack", b"0"),
        (b"XREADGROUP", b"GROUP", b"noack", b"c", b"NOACK", b"STREAMS", b"b", b">"),
        (b"XPENDING", b"b", b"noack"),
    ]
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for command in commands:
            assert await local.call(*command) == await ref.call(*command), command


async def test_blocked_multi_stream_wakeup_against_redis(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        results = []
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as reader, peer(port) as writer:
                assert await writer.call(b"FLUSHDB") == Simple(b"OK")
                for key in (b"a", b"b"):
                    assert await writer.call(
                        b"XGROUP", b"CREATE", key, b"g", b"0", b"MKSTREAM"
                    ) == Simple(b"OK")
                identity = await reader.call(b"CLIENT", b"ID")
                assert isinstance(identity, Integer)
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
                    while True:
                        if port == reference_port:
                            listing = await writer.call(b"CLIENT", b"LIST")
                            assert isinstance(listing, Bulk) and listing.payload is not None
                            ident = b"id=" + str(identity.payload).encode()
                            ready = any(
                                ident in line.split()
                                and any(
                                    token.startswith(b"flags=") and b"b" in token[6:]
                                    for token in line.split()
                                )
                                for line in listing.payload.splitlines()
                            )
                        else:
                            ready = bool(
                                server.executor.waiters.count
                                and server.executor.waiters.pending[0].started
                            )
                        if ready:
                            break
                        await asyncio.sleep(0)
                assert await writer.call(b"XADD", b"b", b"1-0", b"f", b"\x00\xff") == Bulk(b"1-0")
                results.append(await asyncio.wait_for(pending, 5))
        assert results[0] == results[1]
        assert isinstance(results[0], Array)
