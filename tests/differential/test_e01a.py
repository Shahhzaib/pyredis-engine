from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from _support.comparison import Peer, peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]
OK = Simple(b"OK")
PASSWORD = b"synthetic-e01a-reference"


@asynccontextmanager
async def pair(reference_port: int, rules: list[bytes]) -> AsyncIterator[tuple[Peer, ...]]:
    server = await start_server(ServerConfig(port=0))
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(reference_port) as oracle,
        peer(server.sockets[0].getsockname()[1]) as app,
        peer(reference_port) as ref_app,
    ):
        for connection in (admin, oracle):
            await connection.call(b"FLUSHDB")
            await connection.call(b"ACL", b"DELUSER", b"e01a")
            assert (
                await connection.call(b"ACL", b"SETUSER", b"e01a", b"on", b">" + PASSWORD, *rules)
                == OK
            )
        try:
            yield admin, oracle, app, ref_app
        finally:
            await oracle.call(b"ACL", b"DELUSER", b"e01a")


async def same(local: Peer, reference: Peer, *command: bytes) -> object:
    actual, expected = await local.call(*command), await reference.call(*command)
    assert actual == expected
    return actual


async def test_default_nopass_auth_arity_and_identity(reference_port: int) -> None:
    async with pair(reference_port, []) as (admin, oracle, _, __):
        for command in (
            (b"AUTH",),
            (b"AUTH", b"u", b"p", b"extra"),
            (b"AUTH", b"synthetic"),
            (b"AUTH", b"default", b"any"),
            (b"ACL", b"WHOAMI"),
            (b"ACL", b"USERS"),
        ):
            await same(admin, oracle, *command)


async def test_named_auth_failure_disabled_user_and_password_rotation(reference_port: int) -> None:
    async with pair(reference_port, [b"+get", b"+acl|whoami", b"~*"]) as (
        admin,
        oracle,
        app,
        ref_app,
    ):
        assert await same(app, ref_app, b"AUTH", b"e01a", PASSWORD) == OK
        for command in ((b"AUTH", b"e01a", b"wrong"), (b"ACL", b"WHOAMI")):
            await same(app, ref_app, *command)
        for command in (
            (b"ACL", b"SETUSER", b"e01a", b">replacement"),
            (b"ACL", b"SETUSER", b"e01a", b"<" + PASSWORD),
        ):
            await same(admin, oracle, *command)
        await same(app, ref_app, b"AUTH", b"e01a", PASSWORD)
        assert await same(app, ref_app, b"AUTH", b"e01a", b"replacement") == OK
        await same(admin, oracle, b"ACL", b"SETUSER", b"e01a", b"off")
        await same(app, ref_app, b"ACL", b"WHOAMI")
        await same(app, ref_app, b"AUTH", b"e01a", b"replacement")
        await same(app, ref_app, b"AUTH", b"unknown", b"wrong")


async def test_command_multikey_channel_and_administration_denials(reference_port: int) -> None:
    rules = [b"+get", b"+set", b"+mget", b"+mset", b"+subscribe", b"+publish", b"~ok:*", b"&c:*"]
    async with pair(reference_port, rules) as (_, __, app, ref_app):
        await same(app, ref_app, b"AUTH", b"e01a", PASSWORD)
        for command in (
            (b"SET", b"ok:\x00\xff", b"\r\n"),
            (b"GET", b"ok:\x00\xff"),
            (b"DEL", b"ok:k"),
            (b"GET", b"bad:k"),
            (b"MGET", b"ok:k", b"bad:k"),
            (b"MSET", b"ok:k", b"v", b"bad:k", b"x"),
            (b"PUBLISH", b"bad:c", b"message"),
            (b"SUBSCRIBE", b"c:ok", b"bad:c"),
            (b"ACL", b"USERS"),
            (b"ACL", b"SETUSER", b"evil", b">not-logged"),
        ):
            await same(app, ref_app, *command)


@pytest.mark.parametrize("kind", ["command", "key", "channel"])
async def test_exec_revocation_preserves_only_authorized_siblings(
    reference_port: int,
    kind: str,
) -> None:
    rules = [b"+set", b"+del", b"+get", b"+publish", b"+multi", b"+exec", b"~*", b"&*"]
    async with pair(reference_port, rules) as (admin, oracle, app, ref_app):
        await same(app, ref_app, b"AUTH", b"e01a", PASSWORD)
        await same(app, ref_app, b"MULTI")
        await same(app, ref_app, b"SET", b"ok:k", b"one")
        command = (
            (b"DEL", b"bad:k")
            if kind == "command"
            else (b"SET", b"bad:k", b"two")
            if kind == "key"
            else (b"PUBLISH", b"bad:c", b"message")
        )
        await same(app, ref_app, *command)
        change = (
            [b"-del"]
            if kind == "command"
            else [b"resetkeys", b"~ok:*"]
            if kind == "key"
            else [b"resetchannels", b"&c:*"]
        )
        await same(admin, oracle, b"ACL", b"SETUSER", b"e01a", *change)
        result = await same(app, ref_app, b"EXEC")
        assert isinstance(result, Array) and result.payload is not None and result.payload[0] == OK
        assert isinstance(result.payload[1], Error)
        assert await same(admin, oracle, b"GET", b"ok:k") == Bulk(b"one")
        assert await same(admin, oracle, b"GET", b"bad:k") == Bulk(None)


@pytest.mark.parametrize(
    "command", [(b"DEL", b"ok:k"), (b"GET", b"bad:k"), (b"PUBLISH", b"bad:c", b"message")]
)
async def test_acl_dryrun_returns_correct_resp_type_without_effects(
    reference_port: int,
    command: tuple[bytes, ...],
) -> None:
    async with pair(reference_port, [b"+get", b"+publish", b"~ok:*", b"&c:*"]) as (
        admin,
        oracle,
        _,
        __,
    ):
        assert isinstance(await same(admin, oracle, b"ACL", b"DRYRUN", b"e01a", *command), Bulk)


@pytest.mark.parametrize(
    "pattern,key",
    [
        (b"ok:*", b"ok:\x00\xff"),
        (b"ok:?", b"ok:\xff"),
        (b"[a-c]*", b"b\xff"),
        (b"[^a-c]*", b"b"),
        (b"\\*", b"*"),
        (b"a*b*c", b"abbbc"),
    ],
)
async def test_binary_key_pattern_compatibility(
    reference_port: int, pattern: bytes, key: bytes
) -> None:
    async with pair(reference_port, [b"+set", b"+get", b"~" + pattern]) as (_, __, app, ref_app):
        await same(app, ref_app, b"AUTH", b"e01a", PASSWORD)
        await same(app, ref_app, b"SET", key, b"\x00\xff")
        await same(app, ref_app, b"GET", key)
