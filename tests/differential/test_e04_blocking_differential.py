from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize("timeout", [b"bad", b"-1", b"nan", b"inf", b"-inf", b" 1", b"1_0"])
async def test_invalid_timeouts(reference_port: int, timeout: bytes) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for client in (local, ref):
            await client.call(b"DEL", b"q")
            await client.call(b"RPUSH", b"q", b"value")
        assert await local.call(b"BLPOP", b"q", timeout) == await ref.call(b"BLPOP", b"q", timeout)
        assert await local.call(b"LRANGE", b"q", b"0", b"-1") == await ref.call(
            b"LRANGE", b"q", b"0", b"-1"
        )


@pytest.mark.parametrize("timeout", [b"0", b"0.0", b"-0", b"1e-999", b"0x1p2"])
async def test_immediate_valid_timeout_forms(reference_port: int, timeout: bytes) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for client in (local, ref):
            await client.call(b"DEL", b"q")
            await client.call(b"RPUSH", b"q", b"\x00\xff")
        assert await local.call(b"BRPOP", b"q", timeout) == await ref.call(b"BRPOP", b"q", timeout)


async def test_priority_wrong_type_and_transaction(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        commands = [
            (b"FLUSHDB",),
            (b"RPUSH", b"a", b"one", b"two"),
            (b"RPUSH", b"b", b"three"),
            (b"BLPOP", b"b", b"a", b"0"),
            (b"BRPOP", b"b", b"a", b"0"),
            (b"SET", b"bad", b"string"),
            (b"BLPOP", b"missing", b"bad", b"0"),
            (b"MULTI",),
            (b"BLPOP", b"missing", b"0"),
            (b"BRPOP", b"a", b"0"),
            (b"EXEC",),
        ]
        for command in commands:
            assert await local.call(*command) == await ref.call(*command), command


async def test_blocked_wakeup_and_fifo(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as first, peer(port) as second, peer(port) as producer:
                await producer.call(b"DEL", b"fifo")
                tasks = []
                try:
                    for index, client in enumerate((first, second), start=1):
                        tasks.append(asyncio.create_task(client.call(b"BLPOP", b"fifo", b"3")))
                        async with asyncio.timeout(5):
                            while True:
                                if port == reference_port:
                                    reply = await producer.call(b"CLIENT", b"LIST")
                                    assert isinstance(reply, Bulk) and reply.payload is not None
                                    ready = reply.payload.count(b"cmd=blpop") >= index
                                else:
                                    ready = server.executor.waiters.count >= index
                                if ready:
                                    break
                                await asyncio.sleep(0)
                    assert await producer.call(b"RPUSH", b"fifo", b"a", b"b") == Integer(2)
                    assert await asyncio.gather(*tasks) == [
                        Array((Bulk(b"fifo"), Bulk(b"a"))),
                        Array((Bulk(b"fifo"), Bulk(b"b"))),
                    ]
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()


async def test_empty_timeout_response(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as local,
        peer(reference_port) as ref,
    ):
        for client in (local, ref):
            assert await client.call(b"DEL", b"empty") in (Integer(0), Integer(1))
            assert await client.call(b"BLPOP", b"empty", b"0.001") == Array(None)
            assert await client.call(b"PING") == Simple(b"PONG")
