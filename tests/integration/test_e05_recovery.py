from __future__ import annotations

from pathlib import Path

import pytest
from _support.comparison import peer
from _support.recovery import Partition, config, node
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_stream_journal_and_snapshot_recover_empty_last_id(tmp_path: Path) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always", snapshot_enabled=True
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XADD", b"s", b"7-1", b"\x00", b"\xff") == Bulk(b"7-1")
        assert await client.call(b"XADD", b"s", b"8-0", b"f", b"v") == Bulk(b"8-0")
        assert await client.call(b"XDEL", b"s", b"7-1", b"8-0") == Integer(2)
        assert server.persistence is not None
        assert (await server.persistence.compact()).activated
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"TYPE", b"s") == Simple(b"stream")
        assert await client.call(b"XLEN", b"s") == Integer(0)
        assert await client.call(b"XADD", b"s", b"9-0", b"f", b"v") == Bulk(b"9-0")
        assert await client.call(b"XRANGE", b"s", b"-", b"+") == Array(
            (Array((Bulk(b"9-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)
        )


@pytest.mark.parametrize("evict", [False, True])
async def test_stream_replication_reconnect_and_exact_effects(tmp_path: Path, evict: bool) -> None:
    primary = await start_server(
        config(tmp_path / "primary", replication_backlog_bytes=1 if evict else 1024 * 1024)
    )
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
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
            assert await reader.call(b"XRANGE", b"s", b"-", b"+") == Array((
                Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),))
            async with Partition(state):
                replies = await writer.pipeline(
                    [
                        (b"MULTI",),
                        (b"XADD", b"s", b"2-0", b"f", b"v2"),
                        (b"XDEL", b"s", b"1-0"),
                        (b"EXEC",),
                    ]
                )
                assert replies[-1] == Array((Bulk(b"2-0"), Integer(1)))
                assert primary.persistence is not None
                assert (await primary.persistence.compact()).activated
            await state.wait_offset(node(primary).offset)
            assert state.full_syncs == (2 if evict else 1)
            assert state.partial_syncs == (0 if evict else 1)
            assert await reader.call(b"XRANGE", b"s", b"-", b"+") == Array(
                (Array((Bulk(b"2-0"), Array((Bulk(b"f"), Bulk(b"v2"))))),)
            )


async def test_replica_blocked_xread_wakes_after_effect_and_fails_on_disconnect(
    tmp_path: Path,
) -> None:
    import asyncio

    primary = await start_server(config(tmp_path / "primary"))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
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
            pending = asyncio.create_task(
                reader.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"$")
            )
            async with asyncio.timeout(5):
                while (
                    not replica.executor.waiters.count
                    or not replica.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
            assert await asyncio.wait_for(pending, 5) == Array(
                (
                    Array(
                        (
                            Bulk(b"s"),
                            Array((Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)),
                        )
                    ),
                )
            )
            assert state.offset == node(primary).offset
            pending = asyncio.create_task(
                reader.call(b"XREAD", b"BLOCK", b"0", b"STREAMS", b"s", b"$")
            )
            async with asyncio.timeout(5):
                while (
                    not replica.executor.waiters.count
                    or not replica.executor.waiters.pending[0].started
                ):
                    await asyncio.sleep(0)
            async with Partition(state):
                result = await asyncio.wait_for(pending, 5)
                assert isinstance(result, Error) and result.payload.startswith(b"MASTERDOWN")


async def test_stream_journal_failure_does_not_acknowledge_or_replay_partial_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    settings = ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    server = await start_server(settings)
    assert server.persistence is not None

    def fail(payload: bytes) -> None:
        raise OSError("injected stream journal failure")

    monkeypatch.setattr(server.persistence.file, "append", fail)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        try:
            response = await asyncio.wait_for(client.call(b"XADD", b"s", b"1-0", b"f", b"v"), 5)
            assert isinstance(response, Error)
        except (ReferenceError, OSError):
            pass
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"TYPE", b"s") == Simple(b"none")
