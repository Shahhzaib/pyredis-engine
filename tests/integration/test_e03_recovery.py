from __future__ import annotations

from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import peer
from _support.recovery import config, node

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]
OK = Simple(b"OK")


async def test_getdel_reply_limit_does_not_commit(tmp_path: Path) -> None:
    server = await start_server(
        ServerConfig(
            port=0, max_reply_bytes=128, aof_path=str(tmp_path / "aof"), aof_fsync="always"
        )
    )
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        assert await client.call(b"SET", b"k", b"x" * 125) == OK
        assert server.persistence is not None
        sequence = server.persistence.file.sequence
        assert await client.call(b"GETDEL", b"k") == Error(
            b"ERR reply exceeds configured maximum size"
        )
        assert await client.call(b"STRLEN", b"k") == Integer(125)
        assert server.persistence.file.sequence == sequence


async def test_acl_denial_has_no_journal_effect(tmp_path: Path) -> None:
    server = await start_server(
        ServerConfig(
            port=0,
            aof_path=str(tmp_path / "aof"),
            aof_fsync="always",
            acl_file=bootstrap(tmp_path / "acl", ["on", "+getdel", "~ok:*"]),
        )
    )
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == OK
        assert await app.call(b"AUTH", b"app", PASSWORD) == OK
        assert await admin.call(b"SET", b"private", b"value") == OK
        assert server.persistence is not None
        sequence = server.persistence.file.sequence
        denial = await app.call(b"GETDEL", b"private")
        assert isinstance(denial, Error) and denial.payload.startswith(b"NOPERM")
        assert await admin.call(b"GET", b"private") == Bulk(b"value")
        assert server.persistence.file.sequence == sequence


async def test_new_writes_replicate_and_recover_transaction_effects(tmp_path: Path) -> None:
    primary_settings = config(tmp_path / "primary.aof")
    primary = await start_server(primary_settings)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        replica = await start_server(
            config(
                tmp_path / "replica.aof",
                replica_host="127.0.0.1",
                replica_port=primary.sockets[0].getsockname()[1],
            )
        )
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            await node(replica).wait_ready()
            assert await writer.call(b"SET", b"delete", b"value") == OK
            result = await writer.pipeline(
                [
                    (b"MULTI",),
                    (b"SETNX", b"n", b"8"),
                    (b"DECRBY", b"n", b"3"),
                    (b"HSETNX", b"h", b"f", b"v"),
                    (b"GETDEL", b"delete"),
                    (b"EXEC",),
                ]
            )
            assert result[-1] == Array((Integer(1), Integer(5), Integer(1), Bulk(b"value")))
            await node(replica).wait_offset(node(primary).offset)
            assert await reader.call(b"GET", b"n") == Bulk(b"5")
            assert await reader.call(b"HGET", b"h", b"f") == Bulk(b"v")
            assert await reader.call(b"GET", b"delete") == Bulk(None)
    reopened = await start_server(primary_settings)
    async with reopened, peer(reopened.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b"GET", b"n") == Bulk(b"5")
        assert await reader.call(b"HGET", b"h", b"f") == Bulk(b"v")
        assert await reader.call(b"GET", b"delete") == Bulk(None)
