from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap, verifier
from _support.comparison import peer
from _support.pubsub import ack, message, read
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, read_value, serialize_reply
from pyredis_engine.security.policy import NOAUTH, WRONGPASS
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]
OK = Simple(b"OK")
APP = [
    "on",
    "+get",
    "+set",
    "+mset",
    "+watch",
    "+multi",
    "+exec",
    "+subscribe",
    "+publish",
    "+acl|whoami",
    "~ok:*",
    "&events:*",
]


async def test_protected_pipeline_fragmented_binary_auth_and_isolated_connections(
    tmp_path: Path,
) -> None:
    server = await start_server(
        ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl.json", APP))
    )
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert (
            await app.pipeline(
                [
                    (b"PING",),
                    (b"GET", b"k"),
                    (b"MULTI",),
                    (b"INFO", b"replication"),
                    (b"PYREDIS.SYNC", b"2"),
                    (b"ACL", b"WHOAMI"),
                ]
            )
            == [NOAUTH] * 6
        )
        frame = serialize_reply(Array(tuple(Bulk(value) for value in (b"AUTH", b"app", PASSWORD))))
        for offset in range(0, len(frame), 3):
            app.writer.write(frame[offset : offset + 3])
            await app.writer.drain()
        assert await read_value(app.reader, ServerConfig()) == OK
        assert await admin.call(b"GET", b"ok:k") == NOAUTH
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == OK
        assert await app.call(b"SET", b"ok:\x00\xff", b"\x00\r\n") == OK
        assert await app.call(b"GET", b"ok:\x00\xff") == Bulk(b"\x00\r\n")
        denied = await app.call(b"ACL", b"SETUSER", b"evil", b">synthetic-secret")
        assert isinstance(denied, Error) and denied.payload.startswith(b"NOPERM")
        assert b"synthetic-secret" not in denied.payload
        assert await admin.call(b"ACL", b"USERS") == Array(
            (Bulk(b"admin"), Bulk(b"app"), Bulk(b"default"))
        )
        private = await admin.call(b"PYREDIS.SYNC", b"2")
        assert isinstance(private, Error) and private.payload.startswith(b"NOPERM")


async def test_acl_denials_never_enter_journal_and_recovery_keeps_only_authorized_effects(
    tmp_path: Path,
) -> None:
    rules = ["on", "+set", "+get", "+incr", "+multi", "+exec", "~*"]
    settings = ServerConfig(
        port=0,
        acl_file=bootstrap(tmp_path / "acl.json", rules),
        aof_path=str(tmp_path / "journal.aof"),
        aof_fsync="always",
        snapshot_enabled=True,
        aof_max_record_bytes=1024,
    )
    server = await start_server(settings)
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == OK
        assert await app.call(b"AUTH", b"app", PASSWORD) == OK
        owner = server.persistence
        assert owner is not None
        before = owner.file.sequence
        await app.call(b"MULTI")
        await app.call(b"INCR", b"ok:count")
        await app.call(b"SET", b"forbidden", b"x" * 8000)
        await admin.call(b"ACL", b"SETUSER", b"app", b"resetkeys", b"~ok:*")
        assert owner.file.sequence == before  # ACL changes are node-local security state
        reply = await app.call(b"EXEC")
        assert isinstance(reply, Array) and reply.payload is not None
        assert reply.payload[0] == Integer(1)
        assert isinstance(reply.payload[1], Error) and b"keys" in reply.payload[1].payload
        assert owner.file.sequence == before + 1  # denied large write excluded from preflight/log
        before = owner.file.sequence
        assert (await app.call(b"SET", b"forbidden", b"v")).payload.startswith(b"NOPERM")
        assert owner.file.sequence == before
        assert await admin.call(b"GET", b"forbidden") == Bulk(None)
        assert (await owner.compact()).activated
    restarted = await start_server(settings)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as admin:
        assert await admin.call(b"GET", b"ok:count") == NOAUTH
        assert await admin.call(b"AUTH", b"admin", PASSWORD) == OK
        assert await admin.call(b"GET", b"ok:count") == Bulk(b"1")
        assert await admin.call(b"GET", b"forbidden") == Bulk(None)
        # Runtime ACL changes are volatile; restart deliberately reloads the private bootstrap.
        assert await admin.call(b"ACL", b"DRYRUN", b"app", b"SET", b"forbidden", b"v") == OK
    for path in tmp_path.glob("*.aof"):
        assert PASSWORD not in path.read_bytes() and verifier().digest not in path.read_bytes()


async def test_channel_revocation_disconnects_tcp_subscriber_and_cleans_tasks(
    tmp_path: Path,
) -> None:
    server = await start_server(
        ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl.json", APP))
    )
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as app,
    ):
        await admin.call(b"AUTH", b"admin", PASSWORD)
        await app.call(b"AUTH", b"app", PASSWORD)
        assert await app.call(b"SUBSCRIBE", b"events:\x00\xff") == ack(
            b"subscribe", b"events:\x00\xff", 1
        )
        assert await admin.call(b"PUBLISH", b"events:\x00\xff", b"first") == Integer(1)
        assert await read(app) == [message(b"events:\x00\xff", b"first")]
        assert await admin.call(b"ACL", b"SETUSER", b"app", b"resetchannels", b"&other:*") == OK
        assert await asyncio.wait_for(app.reader.read(), 5) == b""
        async with asyncio.timeout(5):
            while server.client_task_count > 1:
                await asyncio.sleep(0)
        assert not server.executor.pubsub.channels
    assert not server.client_task_count


async def test_repeated_failed_auth_is_bounded_and_does_not_disclose_credentials(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl.json"))
    server = await start_server(settings)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        for _ in range(5):
            assert await client.call(b"AUTH", b"unknown", b"synthetic-never-log-this") == WRONGPASS
        assert await asyncio.wait_for(client.reader.read(), 5) == b""
        assert all(isinstance(value, int) for value in server.metrics().values())
    assert "synthetic-never-log-this" not in caplog.text
    assert verifier().export() not in caplog.text and verifier().digest.hex() not in repr(settings)


async def test_real_redis_py_resp2_auth_and_transaction_pipeline(tmp_path: Path) -> None:
    from redis.asyncio import Redis

    server = await start_server(
        ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl.json", APP))
    )
    async with server:
        client = Redis(
            host="127.0.0.1",
            port=server.sockets[0].getsockname()[1],
            username="app",
            password=PASSWORD,
            protocol=2,
            socket_timeout=5,
        )
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.set(b"ok:k", b"\x00\xff")
                pipe.get(b"ok:k")
                assert await pipe.execute() == [True, b"\x00\xff"]
            assert await client.acl_whoami() == "app"
        finally:
            await client.aclose()


async def test_channel_revocation_while_exec_waits_for_fsync_releases_no_old_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = ServerConfig(
        port=0,
        acl_file=bootstrap(tmp_path / "acl.json", APP),
        aof_path=str(tmp_path / "journal.aof"),
        aof_fsync="always",
    )
    server = await start_server(settings)
    async with (
        server,
        peer(server.sockets[0].getsockname()[1]) as writer,
        peer(server.sockets[0].getsockname()[1]) as admin,
        peer(server.sockets[0].getsockname()[1]) as subscriber,
    ):
        for connection, name in ((writer, b"admin"), (admin, b"admin"), (subscriber, b"app")):
            assert await connection.call(b"AUTH", name, PASSWORD) == OK
        await subscriber.call(b"SUBSCRIBE", b"events:one")
        owner = server.persistence
        assert owner is not None
        original = owner.file.sync
        entered, release = threading.Event(), threading.Event()

        def paused() -> None:
            entered.set()
            assert release.wait(5)
            original()

        monkeypatch.setattr(owner.file, "sync", paused)
        pending = asyncio.create_task(
            writer.pipeline(
                [
                    (b"MULTI",),
                    (b"SET", b"ok:k", b"v"),
                    (b"PUBLISH", b"events:one", b"pending"),
                    (b"EXEC",),
                ]
            )
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            assert await admin.call(b"ACL", b"SETUSER", b"app", b"resetchannels") == OK
            assert await asyncio.wait_for(subscriber.reader.read(), 5) == b""
        finally:
            release.set()
        assert (await pending)[-1] == Array((OK, Integer(1)))
        assert await writer.call(b"GET", b"ok:k") == Bulk(b"v")
        assert not server.executor.pubsub.channels


async def test_deleted_client_waiting_for_commit_lock_is_rejected_without_effects(
    tmp_path: Path,
) -> None:
    async def dormant(_: float) -> None:
        await asyncio.Event().wait()

    settings = ServerConfig(
        port=0,
        acl_file=bootstrap(tmp_path / "acl.json", APP),
        aof_path=str(tmp_path / "journal.aof"),
        aof_fsync="always",
    )
    server = await start_server(settings, expiry_wait=dormant)
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    failures: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _, context: failures.append(context))
    try:
        async with (
            server,
            peer(server.sockets[0].getsockname()[1]) as admin,
            peer(server.sockets[0].getsockname()[1]) as app,
        ):
            await admin.call(b"AUTH", b"admin", PASSWORD)
            await app.call(b"AUTH", b"app", PASSWORD)
            owner = server.persistence
            assert owner is not None
            await owner.lock.acquire()
            pending = asyncio.create_task(app.call(b"SET", b"ok:k", b"uncommitted"))
            try:
                async with asyncio.timeout(5):
                    while not owner.pending:
                        await asyncio.sleep(0)
                assert await admin.call(b"ACL", b"DELUSER", b"app") == Integer(1)
            finally:
                owner.lock.release()
            with pytest.raises((ReferenceError, OSError)):
                await pending
            async with asyncio.timeout(5):
                while owner.pending or server.client_task_count > 1:
                    await asyncio.sleep(0)
            assert await admin.call(b"GET", b"ok:k") == Bulk(None)
            assert owner.file.sequence == 0 and owner.failure is None
        assert not failures
    finally:
        loop.set_exception_handler(previous)
