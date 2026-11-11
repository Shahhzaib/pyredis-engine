from __future__ import annotations

from pathlib import Path

import pytest
from _support.comparison import peer
from _support.recovery import Partition, config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence.errors import CapacityError
from pyredis_engine.protocol import Array, Bulk, Integer
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_collection_indexes_rebuild_after_compaction_and_restart(tmp_path: Path) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always", snapshot_enabled=True
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"HSET", b"h", b"\xff", b"v", b"\x00", b"x") == Integer(2)
        assert await client.call(b"SADD", b"s", b"\xff", b"\x00") == Integer(2)
        assert server.persistence is not None and (await server.persistence.compact()).activated
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"HSCAN", b"h", b"0") == Array(
            (Bulk(b"0"), Array((Bulk(b"\x00"), Bulk(b"x"), Bulk(b"\xff"), Bulk(b"v"))))
        )
        assert await client.call(b"SSCAN", b"s", b"0") == Array(
            (Bulk(b"0"), Array((Bulk(b"\x00"), Bulk(b"\xff"))))
        )


async def test_collection_indexes_survive_full_and_partial_replica_sync(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / "primary"))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b"HSET", b"h", b"a", b"v") == Integer(1)
        assert await writer.call(b"SADD", b"s", b"a") == Integer(1)
        replica = await start_server(
            config(
                tmp_path / "replica",
                replica_host="127.0.0.1",
                replica_port=primary.sockets[0].getsockname()[1],
            )
        )
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            state = node(replica)
            await state.wait_ready()
            assert await reader.call(b"SSCAN", b"s", b"0") == Array(
                (Bulk(b"0"), Array((Bulk(b"a"),)))
            )
            async with Partition(state):
                assert await writer.call(b"HSET", b"h", b"b", b"v2") == Integer(1)
                assert await writer.call(b"SADD", b"s", b"b") == Integer(1)
            await state.wait_offset(node(primary).offset)
            assert state.partial_syncs == 1
            assert await reader.call(b"HSCAN", b"h", b"0") == Array(
                (Bulk(b"0"), Array((Bulk(b"a"), Bulk(b"v"), Bulk(b"b"), Bulk(b"v2"))))
            )
            assert await reader.call(b"SSCAN", b"s", b"0") == Array(
                (Bulk(b"0"), Array((Bulk(b"a"), Bulk(b"b"))))
            )


async def test_legacy_budget_recovery_fails_closed_and_succeeds_with_headroom(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy-compatible-journal"
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync="always"))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"HSET", b"h", b"a", b"1", b"b", b"2") == Integer(2)
    before = path.read_bytes()
    old_charge = 192 + 1 + 4 + 64 * 2
    with pytest.raises(CapacityError):
        await start_server(ServerConfig(port=0, aof_path=str(path), maxmemory_bytes=old_charge))
    assert path.read_bytes() == before
    reopened = await start_server(
        ServerConfig(port=0, aof_path=str(path), maxmemory_bytes=old_charge + 16)
    )
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"HSCAN", b"h", b"0") == Array(
            (Bulk(b"0"), Array((Bulk(b"a"), Bulk(b"1"), Bulk(b"b"), Bulk(b"2"))))
        )
