from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Bulk, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.persistence]


async def test_unexpected_allocation_failure_fail_stops_and_preserves_acknowledged_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'journal.aof'), aof_fsync='always')
    server = await start_server(config)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'SET', b'safe', b'value') == Simple(b'OK')
        def fail(*args, **kwargs):
            raise MemoryError('injected mutation-path allocator failure')

        monkeypatch.setattr(server.executor.store, 'put', fail)
        client.writer.write(b'*3\r\n$3\r\nSET\r\n$3\r\nbad\r\n$1\r\nx\r\n')
        await client.writer.drain()
        assert await asyncio.wait_for(client.reader.read(), 5) == b''
        assert server.executor.failed and not server.is_serving()
        assert server.metrics()['allocation_failures'] == 1
        assert server.executor.store.get_string(b'safe') == b'value'
    assert server.metrics()['connections_active'] == 0
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'safe') == Bulk(b'value')
        assert await client.call(b'GET', b'bad') == Bulk(None)
