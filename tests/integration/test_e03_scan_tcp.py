from __future__ import annotations

from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import Peer, peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def iterate(client: Peer, *prefix: bytes, options: tuple[bytes, ...] = ()) -> list[bytes]:
    cursor = b"0"
    collected: list[bytes] = []
    for _ in range(100):
        result = await client.call(*prefix, cursor, *options)
        assert isinstance(result, Array) and result.payload is not None
        raw_cursor, items = result.payload
        assert isinstance(raw_cursor, Bulk) and raw_cursor.payload is not None
        assert isinstance(items, Array) and items.payload is not None
        for item in items.payload:
            assert isinstance(item, Bulk) and item.payload is not None
            collected.append(item.payload)
        cursor = raw_cursor.payload
        if cursor == b"0":
            return collected
    pytest.fail("cursor failed to terminate")


async def test_scan_acl_filters_keyspace_and_checks_collection_key(tmp_path: Path) -> None:
    acl = bootstrap(tmp_path / "acl", ["on", "+scan", "+hscan", "~allowed:*"])
    server = await start_server(ServerConfig(port=0, acl_file=acl))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == Simple(b"OK")
        assert await app.call(b"AUTH", b"app", PASSWORD) == Simple(b"OK")
        for key in (b"allowed:\x00", b"allowed:\xff", b"private:secret"):
            assert await admin.call(b"SET", key, b"value") == Simple(b"OK")
        assert set(await iterate(app, b"SCAN", options=(b"COUNT", b"1"))) == {
            b"allowed:\x00",
            b"allowed:\xff",
        }
        denied = await app.call(b"HSCAN", b"private:secret", b"0")
        assert isinstance(denied, Error) and denied.payload.startswith(b"NOPERM")


async def test_cursor_is_connection_owned_and_mutation_invalidates() -> None:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as first,
        peer(server.sockets[0].getsockname()[1]) as second,
    ):
        for key in (b"a", b"b", b"c"):
            await first.call(b"SET", key, b"v")
        page = await first.call(b"SCAN", b"0", b"COUNT", b"1")
        assert isinstance(page, Array) and page.payload is not None
        cursor = page.payload[0]
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        assert isinstance(await second.call(b"SCAN", cursor.payload), Error)
        await second.call(b"SET", b"other", b"v")
        assert isinstance(await first.call(b"SCAN", cursor.payload), Error)
        assert set(await iterate(first, b"SCAN", options=(b"COUNT", b"1"))) == {
            b"a",
            b"b",
            b"c",
            b"other",
        }
