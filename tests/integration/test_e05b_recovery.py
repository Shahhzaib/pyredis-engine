from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import Partition, config, node
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_group_pending_survives_trim_compaction_and_restart(tmp_path: Path) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always", snapshot_enabled=True
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(
            await client.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"), Array
        )
        assert await client.call(b"XADD", b"s", b"2-0", b"f", b"v2") == Bulk(b"2-0")
        assert await client.call(b"XTRIM", b"s", b"MAXLEN", b"1") == Integer(1)
        assert server.persistence is not None and (await server.persistence.compact()).activated
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XPENDING", b"s", b"g") == Array(
            (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"c"), Bulk(b"1"))),)))
        )
        assert await client.call(
            b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b"0"
        ) == Array((Array((Bulk(b"s"), Array((Array((Bulk(b"1-0"), Array(None))),)))),))
        assert await client.call(b"XACK", b"s", b"g", b"1-0") == Integer(1)


async def test_group_pending_replicates_across_partial_resync(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / "primary"))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
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
            async with Partition(state):
                assert isinstance(
                    await writer.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"),
                    Array,
                )
            await state.wait_offset(node(primary).offset)
            assert state.partial_syncs == 1
            assert await reader.call(b"XPENDING", b"s", b"g") == Array(
                (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"c"), Bulk(b"1"))),)))
            )


async def test_group_access_denied_and_block_timeout_creates_no_pending(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / "acl", ["on", "+xreadgroup", "~allowed:*"])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await app.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        assert await admin.call(
            b"XGROUP", b"CREATE", b"allowed:s", b"g", b"0", b"MKSTREAM"
        ) == Simple(b"OK")
        denial = await app.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"private:s", b">")
        assert isinstance(denial, Error) and denial.payload.startswith(b"NOPERM")
        for group, consumer in ((b"STREAMS", b"c"), (b"g", b"STREAMS")):
            denial = await app.call(
                b"XREADGROUP", b"GROUP", group, consumer, b"STREAMS", b"private:s", b">"
            )
            assert isinstance(denial, Error) and denial.payload.startswith(b"NOPERM")
        reply = await app.call(
            b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"10", b"STREAMS", b"allowed:s", b">"
        )
        assert reply == Array(None)
        assert await admin.call(b"XPENDING", b"allowed:s", b"g") == Array(
            (Integer(0), Bulk(None), Bulk(None), Array(None))
        )


async def test_concurrent_group_consumers_receive_distinct_entries() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as admin:
        port = server.sockets[0].getsockname()[1]
        for number in range(1, 21):
            identity = f"{number}-0".encode()
            assert await admin.call(b"XADD", b"s", identity, b"f", b"v") == Bulk(identity)
        assert await admin.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")

        async def consume(number: int) -> bytes:
            async with peer(port) as client:
                response = await client.call(
                    b"XREADGROUP",
                    b"GROUP",
                    b"g",
                    str(number).encode(),
                    b"COUNT",
                    b"1",
                    b"STREAMS",
                    b"s",
                    b">",
                )
                assert isinstance(response, Array) and response.payload is not None
                stream = response.payload[0]
                assert isinstance(stream, Array) and stream.payload is not None
                entries = stream.payload[1]
                assert isinstance(entries, Array) and entries.payload is not None
                entry = entries.payload[0]
                assert isinstance(entry, Array) and entry.payload is not None
                identity = entry.payload[0]
                assert isinstance(identity, Bulk) and identity.payload is not None
                return identity.payload

        identities = await asyncio.gather(*(consume(number) for number in range(20)))
        assert len(set(identities)) == 20
        summary = await admin.call(b"XPENDING", b"s", b"g")
        assert isinstance(summary, Array) and summary.payload is not None
        assert summary.payload[0] == Integer(20)


async def test_group_pending_in_initial_full_replica_snapshot(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / "primary"))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(
            await writer.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"), Array
        )
        replica = await start_server(
            config(
                tmp_path / "replica",
                replica_host="127.0.0.1",
                replica_port=primary.sockets[0].getsockname()[1],
            )
        )
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            await node(replica).wait_ready()
            assert await reader.call(b"XPENDING", b"s", b"g") == await writer.call(
                b"XPENDING", b"s", b"g"
            )


async def test_failed_group_delivery_journal_does_not_acknowledge_or_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    server = await start_server(settings)
    assert server.persistence is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")

        def fail(payload: bytes) -> None:
            raise OSError("injected group journal failure")

        monkeypatch.setattr(server.persistence.file, "append", fail)
        try:
            response = await asyncio.wait_for(
                client.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"), 5
            )
            assert isinstance(response, Error)
        except (ReferenceError, OSError):
            pass
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XPENDING", b"s", b"g") == Array(
            (Integer(0), Bulk(None), Bulk(None), Array(None))
        )
        assert isinstance(
            await client.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"), Array
        )


async def test_claim_and_consumer_removal_survive_compaction_and_restart(tmp_path: Path) -> None:
    settings = ServerConfig(
        port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always", snapshot_enabled=True
    )
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(
            await client.call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array
        )
        assert await client.call(b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"JUSTID") == Array(
            (Bulk(b"1-0"),)
        )
        assert server.persistence is not None and (await server.persistence.compact()).activated
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XPENDING", b"s", b"g") == Array(
            (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"b"), Bulk(b"1"))),)))
        )
        assert await client.call(b"XGROUP", b"DELCONSUMER", b"s", b"g", b"b") == Integer(1)
        assert await client.call(b"XPENDING", b"s", b"g") == Array(
            (Integer(0), Bulk(None), Bulk(None), Array(None))
        )


async def test_claim_replicates_after_disconnection(tmp_path: Path) -> None:
    primary = await start_server(config(tmp_path / "primary"))
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        assert await writer.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await writer.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(
            await writer.call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array
        )
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
            async with Partition(state):
                assert await writer.call(
                    b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"JUSTID"
                ) == Array((Bulk(b"1-0"),))
            await state.wait_offset(node(primary).offset)
            assert state.partial_syncs == 1
            assert await reader.call(b"XPENDING", b"s", b"g") == await writer.call(
                b"XPENDING", b"s", b"g"
            )


async def test_claim_commands_enforce_key_acl(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / "acl", ["on", "+xclaim", "+xautoclaim", "+xpending", "~allowed:*"])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await app.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        assert await admin.call(b"XADD", b"private:s", b"1-0", b"f", b"secret") == Bulk(b"1-0")
        assert await admin.call(b"XGROUP", b"CREATE", b"private:s", b"g", b"0") == Simple(b"OK")
        assert isinstance(
            await admin.call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"private:s", b">"),
            Array,
        )
        for command in (
            (b"XCLAIM", b"private:s", b"g", b"b", b"0", b"1-0"),
            (b"XAUTOCLAIM", b"private:s", b"g", b"b", b"0", b"0-0"),
            (b"XPENDING", b"private:s", b"g", b"-", b"+", b"10"),
        ):
            result = await app.call(*command)
            assert isinstance(result, Error) and result.payload.startswith(b"NOPERM")
        assert await admin.call(b"XPENDING", b"private:s", b"g") == Array(
            (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"a"), Bulk(b"1"))),)))
        )


async def test_failed_claim_journal_does_not_replay_new_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = ServerConfig(port=0, aof_path=str(tmp_path / "journal"), aof_fsync="always")
    server = await start_server(settings)
    assert server.persistence is not None
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert await client.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(
            await client.call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array
        )

        def fail(payload: bytes) -> None:
            raise OSError("injected claim journal failure")

        monkeypatch.setattr(server.persistence.file, "append", fail)
        try:
            reply = await asyncio.wait_for(
                client.call(b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"JUSTID"), 5
            )
            assert isinstance(reply, Error)
        except (ReferenceError, OSError):
            pass
    reopened = await start_server(settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"XPENDING", b"s", b"g") == Array(
            (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"a"), Bulk(b"1"))),)))
        )
