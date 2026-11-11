"""Live local smoke for comparison plumbing; this is not reference acceptance."""
from __future__ import annotations

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server


@pytest.mark.asyncio
async def test_comparison_pipeline_retains_binary_types_and_closes_tasks() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        payload = b"\x00\r\n\xff"
        assert await connection.pipeline([
            (b"SET", b"k", payload), (b"GET", b"k"), (b"GET", b"missing"),
            (b"INCR", b"k"), (b"PTTL", b"k"),
        ]) == [Simple(b"OK"), Bulk(payload), Bulk(None),
               Error(b"ERR value is not an integer or out of range"), Integer(-1)]
    assert server.expiration_task.done() and server.client_task_count == 0
