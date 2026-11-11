from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize(
    "commands",
    [
        [
            (b"XADD", b"s", b"0-0", b"f", b"v"),
            (b"XADD", b"s", b"7-1", b"\x00", b"\xff"),
            (b"XADD", b"s", b"7-1", b"f", b"v"),
            (b"XADD", b"s", b"bad", b"f", b"v"),
            (b"XLEN", b"s"),
        ],
        [
            (b"XADD", b"s", b"7-1", b"f", b"v", b"f", b"v2"),
            (b"XADD", b"s", b"8-0", b"g", b"v"),
            (b"XRANGE", b"s", b"-", b"+"),
            (b"XRANGE", b"s", b"(7-1", b"+", b"COUNT", b"1"),
            (b"XREVRANGE", b"s", b"+", b"-"),
        ],
        [
            (b"XADD", b"s", b"7-1", b"f", b"v"),
            (b"XADD", b"s", b"8-0", b"f", b"v"),
            (b"XDEL", b"s", b"7-1", b"8-0", b"7-1"),
            (b"XLEN", b"s"),
            (b"TYPE", b"s"),
            (b"XADD", b"s", b"7-5", b"f", b"v"),
            (b"XADD", b"s", b"9-0", b"f", b"v"),
            (b"XTRIM", b"s", b"MAXLEN", b"=", b"0"),
            (b"XLEN", b"s"),
        ],
        [
            (b"XADD", b"s", b"MAXLEN", b"=", b"1", b"1-0", b"a", b"v"),
            (b"XADD", b"s", b"MAXLEN", b"1", b"2-0", b"b", b"v"),
            (b"XRANGE", b"s", b"-", b"+"),
        ],
        [
            (b"SET", b"s", b"v"),
            (b"XLEN", b"s"),
            (b"XRANGE", b"s", b"-", b"+"),
            (b"XDEL", b"s", b"1-0"),
            (b"XTRIM", b"s", b"MAXLEN", b"0"),
            (b"XADD", b"s", b"1-0", b"f", b"v"),
            (b"GET", b"s"),
        ],
        [
            (b"XADD", b"s", b"1-0", b"f", b"v"),
            (b"XADD", b"t", b"2-0", b"g", b"w"),
            (b"XREAD", b"STREAMS", b"s", b"t", b"0", b"0"),
            (b"XREAD", b"COUNT", b"0", b"STREAMS", b"s", b"0"),
            (b"XREAD", b"STREAMS", b"s", b"$"),
            (b"XREAD", b"BLOCK", b"-1", b"STREAMS", b"s", b"0"),
            (b"XREAD", b"BLOCK", b"9223372036854775807", b"STREAMS", b"s", b"0"),
        ],
        [
            (b"XADD", b"s", b"7-1", b"f", b"a"),
            (b"XADD", b"s", b"7-2", b"f", b"b"),
            (b"XADD", b"s", b"8-0", b"f", b"c"),
            (b"XRANGE", b"s", b"7", b"7"),
            (b"XRANGE", b"s", b"(7", b"+"),
            (b"XREVRANGE", b"s", b"7", b"-"),
            (b"XRANGE", b"s", b"-", b"+", b"COUNT", b"0"),
        ],
        [
            (b"XRANGE", b"missing", b"bad", b"+"),
            (b"XDEL", b"missing", b"bad"),
            (b"XTRIM", b"missing", b"MAXLEN", b"bad"),
            (b"SET", b"wrong", b"v"),
            (b"XRANGE", b"wrong", b"bad", b"+"),
            (b"XDEL", b"wrong", b"bad"),
            (b"XADD", b"wrong", b"bad", b"f", b"v"),
        ],
    ],
)
async def test_stream_core_against_redis(
    reference_port: int, commands: list[tuple[bytes, ...]]
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        assert await local.call(b"FLUSHDB") == await ref.call(b"FLUSHDB")
        for command in commands:
            assert await local.call(*command) == await ref.call(*command), command


async def test_blocked_xread_new_stream_and_dollar(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as reader, peer(port) as writer:
                await writer.call(b"DEL", b"e05-block")
                pending = asyncio.create_task(
                    reader.call(b"XREAD", b"BLOCK", b"3000", b"STREAMS", b"e05-block", b"$")
                )
                try:
                    async with asyncio.timeout(5):
                        while True:
                            if port == reference_port:
                                state = await writer.call(b"CLIENT", b"LIST")
                                assert isinstance(state, Bulk) and state.payload is not None
                                ready = b"cmd=xread" in state.payload
                            else:
                                ready = bool(
                                    server.executor.waiters.pending
                                    and server.executor.waiters.pending[0].started
                                )
                            if ready:
                                break
                            await asyncio.sleep(0)
                    assert await writer.call(b"XADD", b"e05-block", b"1-0", b"f", b"v") == Bulk(
                        b"1-0"
                    )
                    assert await asyncio.wait_for(pending, 5) == Array(
                        (
                            Array(
                                (
                                    Bulk(b"e05-block"),
                                    Array(
                                        (Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)
                                    ),
                                )
                            ),
                        )
                    )
                finally:
                    if not pending.done():
                        pending.cancel()
