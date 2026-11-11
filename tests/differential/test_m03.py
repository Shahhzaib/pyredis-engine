"""Explicit, separately reported M03 external acceptance cases; no software installs."""
from __future__ import annotations

import asyncio
import time

import pytest
from _support.redis_reference import connect
from _support.redis_reference import exchange as bounded_exchange

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import (
    Bulk,
    Integer,
    Reply,
    Simple,
)
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]
NAMES = ["EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "TTL", "PTTL", "PERSIST",
         "SET_EX", "SET_PX", "SET_NX", "SET_XX", "SET_KEEPTTL", "GETEX",
         "GETEX_EX", "GETEX_PX", "GETEX_EXAT", "GETEX_PXAT", "GETEX_PERSIST"]


async def exchange(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: tuple[bytes, ...],
) -> Reply:
    return await bounded_exchange(reader, writer, command)


@pytest.mark.parametrize("name", NAMES)
async def test_m03_matches_real_redis(reference_port: int, name: str) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        local, output = await connect(server.sockets[0].getsockname()[1])
        reference, oracle = await connect(reference_port)
        try:
            await exchange(reference, oracle, (b"FLUSHDB",))

            async def compare(command: tuple[bytes, ...]) -> Reply:
                actual = await exchange(local, output, command)
                expected = await exchange(reference, oracle, command)
                assert actual == expected, command
                return actual

            await compare((b"SET", b"k", b"\x00\r\n\xff"))
            seconds = str(time.time_ns() // 1_000_000_000 + 60).encode()
            milliseconds = str(time.time_ns() // 1_000_000 + 60_000).encode()
            commands: dict[str, tuple[bytes, ...]] = {
                "EXPIRE": (b"EXPIRE", b"k", b"60"),
                "PEXPIRE": (b"PEXPIRE", b"k", b"60000"),
                "EXPIREAT": (b"EXPIREAT", b"k", seconds),
                "PEXPIREAT": (b"PEXPIREAT", b"k", milliseconds),
                "TTL": (b"TTL", b"missing"), "PTTL": (b"PTTL", b"missing"),
                "PERSIST": (b"PERSIST", b"k"),
                "SET_EX": (b"SET", b"k", b"v", b"EX", b"60"),
                "SET_PX": (b"SET", b"k", b"v", b"PX", b"60000"),
                "SET_NX": (b"SET", b"k", b"v", b"NX"),
                "SET_XX": (b"SET", b"k", b"v", b"XX"),
                "SET_KEEPTTL": (b"SET", b"k", b"v", b"KEEPTTL"),
                "GETEX": (b"GETEX", b"k"),
                "GETEX_EX": (b"GETEX", b"k", b"EX", b"60"),
                "GETEX_PX": (b"GETEX", b"k", b"PX", b"60000"),
                "GETEX_EXAT": (b"GETEX", b"k", b"EXAT", seconds),
                "GETEX_PXAT": (b"GETEX", b"k", b"PXAT", milliseconds),
                "GETEX_PERSIST": (b"GETEX", b"k", b"PERSIST"),
            }
            # Ensure persistence/retention cases actually exercise existing metadata.
            if name in ("PERSIST", "SET_KEEPTTL", "GETEX_PERSIST"):
                await compare((b"PEXPIREAT", b"k", milliseconds))
            started = time.monotonic_ns()
            await compare(commands[name])
            await compare((b"GET", b"k"))
            local_ttl = await exchange(local, output, (b"PTTL", b"k"))
            oracle_ttl = await exchange(reference, oracle, (b"PTTL", b"k"))
            assert isinstance(local_ttl, Integer) and isinstance(oracle_ttl, Integer)
            if local_ttl.payload < 0 or oracle_ttl.payload < 0:
                assert local_ttl == oracle_ttl
            else:
                # Bracket independent calls: do not require equal running clocks.
                elapsed_ms = (time.monotonic_ns() - started) // 1_000_000
                assert abs(local_ttl.payload - oracle_ttl.payload) <= elapsed_ms + 2
            await compare((b"PEXPIRE", b"k", b"0"))
            assert await compare((b"PTTL", b"k")) == Integer(-2)
            assert await compare((b"GET", b"k")) == Bulk(None)
            assert await compare((b"SET", b"k", b"v")) == Simple(b"OK")
            await compare((b"GETEX", b"k", b"PX", b"0"))
            await compare((b"SET", b"k", b"new", b"NX", b"XX"))
            await compare((b"EXPIRE", b"k"))
            await compare((b"SET", b"k", b"new", b"EX", b"0"))
            await compare((b"GET", b"k"))
        finally:
            output.close()
            oracle.close()
            await output.wait_closed()
            await oracle.wait_closed()
