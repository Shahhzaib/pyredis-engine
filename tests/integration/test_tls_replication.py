from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import ssl
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.comparison import Peer
from _support.recovery import Partition, node
from _support.recovery import config as recovery_config
from _support.redis_reference import close_writer
from _support.tls_fixtures import generate

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, read_value, serialize_reply
from pyredis_engine.server import ManagedServer, start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]
OK = Simple(b"OK")


@pytest.fixture(scope="module")
def certs(tmp_path_factory: pytest.TempPathFactory) -> Path:
    configured = os.environ.get("PYREDIS_TEST_TLS_DIR")
    if configured:
        directory = Path(configured)
    else:
        if not shutil.which("openssl"):
            pytest.fail("OpenSSL or PYREDIS_TEST_TLS_DIR is required for mandatory TLS tests")
        directory = tmp_path_factory.mktemp("e01b-certificates")
        generate(directory)
    for name in (
        "ca.pem",
        "other-ca.pem",
        "server.pem",
        "server.key",
        "client.pem",
        "client.key",
        "other-client.pem",
        "other-client.key",
    ):
        assert (directory / name).is_file()
    return directory


def client_context(
    certs: Path, *, ca: str = "ca.pem", certificate: str | None = None
) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(certs / ca))
    if certificate is not None:
        context.load_cert_chain(
            str(certs / f"{certificate}.pem"), str(certs / f"{certificate}.key")
        )
    return context


def tls_settings(certs: Path, **options: object) -> dict[str, object]:
    return {
        "tls_cert_file": str(certs / "server.pem"),
        "tls_key_file": str(certs / "server.key"),
        "tls_ca_file": str(certs / "ca.pem"),
        **options,
    }


@asynccontextmanager
async def tls_peer(
    server: ManagedServer,
    certs: Path,
    *,
    certificate: str | None = None,
    name: str = "localhost",
    ca: str = "ca.pem",
) -> AsyncIterator[Peer]:
    reader, writer = await asyncio.open_connection(
        "127.0.0.1",
        server.sockets[0].getsockname()[1],
        ssl=client_context(certs, ca=ca, certificate=certificate),
        server_hostname=name,
        ssl_handshake_timeout=5,
    )
    try:
        yield Peer(reader, writer)
    finally:
        await close_writer(writer)


def replica_options(
    certs: Path, directory: Path, primary: ManagedServer, **options: object
) -> dict[str, object]:
    password = directory / "replica.password"
    password.write_bytes(PASSWORD)
    password.chmod(0o600)
    return {
        **tls_settings(certs),
        "acl_file": bootstrap(directory / "replica-acl.json"),
        "replication_secure": True,
        "replica_host": "127.0.0.1",
        "replica_port": primary.sockets[0].getsockname()[1],
        "replica_username": "app",
        "replica_password_file": str(password),
        "replica_server_name": "localhost",
        "replica_tls_cert_file": str(certs / "client.pem"),
        "replica_tls_key_file": str(certs / "client.key"),
        **options,
    }


def primary_options(certs: Path, directory: Path, **options: object) -> dict[str, object]:
    der = ssl.PEM_cert_to_DER_cert((certs / "client.pem").read_text())
    return {
        **tls_settings(certs),
        "acl_file": bootstrap(directory / "primary-acl.json", ["on", "+pyredis.sync"]),
        "replication_secure": True,
        "tls_client_auth": "required",
        "replication_client_cert_sha256": hashlib.sha256(der).hexdigest(),
        **options,
    }


async def login(client: Peer, user: bytes = b"admin") -> None:
    assert await client.call(b"AUTH", user, PASSWORD) == OK


async def test_tls_auth_binary_pipeline_and_plaintext_rejection(
    certs: Path, tmp_path: Path
) -> None:
    settings = ServerConfig(
        port=0, acl_file=bootstrap(tmp_path / "acl.json"), **tls_settings(certs)
    )
    server = await start_server(settings)
    async with server, tls_peer(server, certs) as client:
        assert await client.call(b"PING") == Error(b"NOAUTH Authentication required.")
        await login(client)
        assert await client.pipeline(
            [(b"MULTI",), (b"SET", b"\x00\xff", b"\r\n"), (b"GET", b"\x00\xff"), (b"EXEC",)]
        ) == [OK, Simple(b"QUEUED"), Simple(b"QUEUED"), Array((OK, Bulk(b"\r\n")))]
        frame = serialize_reply(Array((Bulk(b"ECHO"), Bulk(b"\x00\xff\r\n"))))
        for offset in range(0, len(frame), 2):
            client.writer.write(frame[offset : offset + 2])
            await client.writer.drain()
        assert await read_value(client.reader, ServerConfig()) == Bulk(b"\x00\xff\r\n")
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        try:
            writer.write(b"*1\r\n$4\r\nPING\r\n")
            await writer.drain()
            try:
                assert await asyncio.wait_for(reader.read(), 5) == b""
            except TimeoutError:
                raise
            except (ConnectionError, OSError):
                pass
        finally:
            await close_writer(writer)


async def test_ca_hostname_and_required_client_certificates(
    certs: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = await start_server(
        ServerConfig(port=0, **tls_settings(certs, tls_client_auth="required"))
    )
    async with server:
        for ca, name in [("other-ca.pem", "localhost"), ("ca.pem", "wrong.example")]:
            with pytest.raises((ssl.SSLError, ConnectionError)):
                async with tls_peer(server, certs, ca=ca, name=name, certificate="client"):
                    pass
        for certificate in (None, "other-client"):
            try:
                async with tls_peer(server, certs, certificate=certificate) as client:
                    try:
                        assert await asyncio.wait_for(client.reader.read(1), 5) == b""
                    except TimeoutError:
                        raise
                    except (ConnectionError, OSError, ssl.SSLError):
                        pass
            except TimeoutError:
                raise
            except (ConnectionError, OSError, ssl.SSLError):
                pass
        async with tls_peer(server, certs, certificate="client") as client:
            assert await client.call(b"PING") == Simple(b"PONG")
    assert "Exception in callback" not in caplog.text and "client task failed" not in caplog.text


async def test_optional_client_certificates_verify_when_supplied(certs: Path) -> None:
    server = await start_server(
        ServerConfig(port=0, **tls_settings(certs, tls_client_auth="optional"))
    )
    async with server:
        for certificate in (None, "client"):
            async with tls_peer(server, certs, certificate=certificate) as client:
                assert await client.call(b"PING") == Simple(b"PONG")
        try:
            async with tls_peer(server, certs, certificate="other-client") as client:
                try:
                    assert await asyncio.wait_for(client.reader.read(1), 5) == b""
                except TimeoutError:
                    raise
                except (ConnectionError, OSError, ssl.SSLError):
                    pass
        except TimeoutError:
            raise
        except (ConnectionError, OSError, ssl.SSLError):
            pass


async def test_stalled_handshake_reserves_a_client_slot_and_expires(certs: Path) -> None:
    server = await start_server(
        ServerConfig(port=0, max_clients=1, tls_handshake_timeout=0.25, **tls_settings(certs))
    )
    async with server:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        try:
            async with asyncio.timeout(5):
                while server.client_task_count != 1:
                    await asyncio.sleep(0)
            try:
                async with tls_peer(server, certs):
                    pytest.fail("a stalled handshake bypassed the client limit")
            except TimeoutError:
                raise
            except (ConnectionError, OSError, ssl.SSLError):
                pass
            assert await asyncio.wait_for(reader.read(), 5) == b""
        finally:
            await close_writer(writer)
        async with asyncio.timeout(5):
            while server.client_task_count:
                await asyncio.sleep(0)
        async with tls_peer(server, certs) as client:
            assert await client.call(b"PING") == Simple(b"PONG")


@pytest.mark.parametrize("evict", [False, True])
async def test_secure_full_and_partial_resync_preserve_order(
    certs: Path,
    tmp_path: Path,
    evict: bool,
) -> None:
    primary = await start_server(
        recovery_config(
            tmp_path / "primary.aof",
            **primary_options(certs, tmp_path, replication_backlog_bytes=1 if evict else 65536),
        )
    )
    async with primary, tls_peer(primary, certs, certificate="client") as writer:
        await login(writer)
        await writer.call(b"SET", b"before", b"\x00\xff")
        replica = await start_server(
            recovery_config(tmp_path / "replica.aof", **replica_options(certs, tmp_path, primary))
        )
        async with replica, tls_peer(replica, certs) as reader:
            await login(reader)
            await node(replica).wait_ready()
            assert await reader.call(b"GET", b"before") == Bulk(b"\x00\xff")
            async with Partition(node(replica)):
                result = await writer.pipeline(
                    [(b"MULTI",), (b"INCR", b"a"), (b"INCR", b"b"), (b"EXEC",)]
                )
                assert result[-1] == Array((Integer(1), Integer(1)))
            await node(replica).wait_offset(node(primary).offset)
            assert node(replica).partial_syncs == (0 if evict else 1)
            assert node(replica).full_syncs == (2 if evict else 1)
            assert await reader.call(b"MGET", b"a", b"b") == Array((Bulk(b"1"), Bulk(b"1")))


async def test_secure_version_one_full_sync(certs: Path, tmp_path: Path) -> None:
    primary = await start_server(
        ServerConfig(
            port=0,
            aof_path=str(tmp_path / "p.aof"),
            aof_fsync="always",
            snapshot_enabled=True,
            replication_enabled=True,
            **primary_options(certs, tmp_path),
        )
    )
    async with primary, tls_peer(primary, certs, certificate="client") as writer:
        await login(writer)
        await writer.call(b"SET", b"before", b"v")
        replica = await start_server(
            ServerConfig(
                port=0,
                aof_path=str(tmp_path / "r.aof"),
                aof_fsync="always",
                snapshot_enabled=True,
                replication_enabled=True,
                **replica_options(
                    certs,
                    tmp_path,
                    primary,
                    replica_tls_key_file=str(certs / "client-encrypted.key"),
                    replica_tls_key_password_file=str(certs / "key-password"),
                ),
            )
        )
        async with replica, tls_peer(replica, certs) as reader:
            await login(reader)
            assert replica.replication is not None and primary.replication is not None
            await replica.replication.wait_ready()
            assert await reader.call(b"GET", b"before") == Bulk(b"v")
            await writer.call(b"INCR", b"after")
            await replica.replication.wait_offset(primary.replication.offset)
            assert await reader.call(b"GET", b"after") == Bulk(b"1")


async def test_revoked_peer_is_disconnected_and_reauthenticates(
    certs: Path, tmp_path: Path
) -> None:
    primary = await start_server(
        recovery_config(tmp_path / "primary.aof", **primary_options(certs, tmp_path))
    )
    async with primary, tls_peer(primary, certs, certificate="client") as admin:
        await login(admin)
        replica = await start_server(
            recovery_config(tmp_path / "replica.aof", **replica_options(certs, tmp_path, primary))
        )
        async with replica:
            peer_node = node(replica)
            await peer_node.wait_ready()
            entered, release = asyncio.Event(), asyncio.Event()

            async def wait_retry(_: float) -> None:
                entered.set()
                await release.wait()

            peer_node.retry_wait = wait_retry
            try:
                assert await admin.call(b"ACL", b"SETUSER", b"app", b"-pyredis.sync") == OK
                await asyncio.wait_for(entered.wait(), 5)
                await admin.call(b"SET", b"after-revoke", b"v")
                assert replica.executor.store.get_string(b"after-revoke") is None
                assert await admin.call(b"ACL", b"SETUSER", b"app", b"+pyredis.sync") == OK
            finally:
                release.set()
            await peer_node.wait_offset(node(primary).offset)
            assert replica.executor.store.get_string(b"after-revoke") == b"v"


@pytest.mark.parametrize("failure", ["password", "hostname", "ca", "client-cert", "pin"])
async def test_secure_replica_failures_never_start_sync_or_log_credentials(
    certs: Path,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    failure: str,
) -> None:
    extra = {"replication_client_cert_sha256": "00" * 32} if failure == "pin" else {}
    primary = await start_server(
        recovery_config(tmp_path / "primary.aof", **primary_options(certs, tmp_path, **extra))
    )
    async with primary:
        options = replica_options(certs, tmp_path, primary, replication_retry_attempts=1)
        if failure == "password":
            Path(str(options["replica_password_file"])).write_bytes(b"synthetic-wrong-password")
        elif failure == "hostname":
            options["replica_server_name"] = "wrong.example"
        elif failure == "ca":
            options["tls_ca_file"] = str(certs / "other-ca.pem")
        elif failure == "client-cert":
            options["replica_tls_cert_file"] = str(certs / "other-client.pem")
            options["replica_tls_key_file"] = str(certs / "other-client.key")
        replica = await start_server(recovery_config(tmp_path / "replica.aof", **options))
        async with replica:
            async with asyncio.timeout(10):
                while node(replica).phase != "down":
                    await asyncio.sleep(0)
            assert not node(primary).sessions
            assert node(replica).full_syncs == node(replica).partial_syncs == 0
            assert replica.executor.store.physical_count == 0
            assert "synthetic-wrong-password" not in caplog.text
            assert PASSWORD.decode("latin1") not in caplog.text


async def test_encrypted_key_requires_private_password_file(certs: Path, tmp_path: Path) -> None:
    encrypted = str(certs / "server-encrypted.key")
    settings = tls_settings(certs, tls_key_file=encrypted)
    with pytest.raises(ValueError, match="TLS/replication configuration") as error:
        await start_server(ServerConfig(port=0, **settings))
    assert "synthetic-key-only" not in str(error.value)
    password = tmp_path / "private-key-password"
    password.write_bytes(b"synthetic-key-only")
    password.chmod(0o600)
    config = ServerConfig(port=0, **settings, tls_key_password_file=str(password))
    assert str(password) not in repr(config)
    server = await start_server(config)
    async with server, tls_peer(server, certs) as client:
        assert await client.call(b"PING") == Simple(b"PONG")


async def test_invalid_tls_material_fails_before_listening(certs: Path, tmp_path: Path) -> None:
    bad = tmp_path / "invalid-ca.pem"
    bad.write_bytes(b"synthetic-invalid-certificate")
    for settings in (
        tls_settings(certs, tls_ca_file=str(bad)),
        tls_settings(certs, tls_key_file=str(certs / "client.key")),
    ):
        with pytest.raises(ValueError, match="TLS/replication configuration") as error:
            await start_server(ServerConfig(port=0, **settings))
        assert "synthetic-invalid-certificate" not in str(error.value)


async def test_redis_py_authenticated_tls(certs: Path, tmp_path: Path) -> None:
    from redis.asyncio import Redis

    server = await start_server(
        ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl.json"), **tls_settings(certs))
    )
    async with server:
        client = Redis(
            host="127.0.0.1",
            port=server.sockets[0].getsockname()[1],
            username="admin",
            password=PASSWORD,
            protocol=2,
            ssl=True,
            ssl_ca_certs=str(certs / "ca.pem"),
            ssl_check_hostname=True,
            socket_timeout=5,
        )
        try:
            assert await client.set(b"\x00\xff", b"\r\n")
            assert await client.get(b"\x00\xff") == b"\r\n"
        finally:
            await client.aclose()


async def test_revocation_during_snapshot_prevents_greeting_and_stream(
    certs: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = await start_server(
        recovery_config(tmp_path / "primary.aof", **primary_options(certs, tmp_path))
    )
    owner = primary.persistence
    assert owner is not None
    entered, release = asyncio.Event(), asyncio.Event()
    original = owner.export_snapshot

    async def pause(*args: object, **kwargs: object) -> object:
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(owner, "export_snapshot", pause)
    async with (
        primary,
        tls_peer(primary, certs, certificate="client") as admin,
        tls_peer(primary, certs, certificate="client") as replica,
    ):
        await login(admin)
        await login(replica, b"app")
        payload = Array((Bulk(b"PYREDIS.SYNC"), Bulk(b"1")))
        replica.writer.write(serialize_reply(payload))
        await replica.writer.drain()
        try:
            await asyncio.wait_for(entered.wait(), 5)
            assert await admin.call(b"ACL", b"SETUSER", b"app", b"-pyredis.sync") == OK
            assert await asyncio.wait_for(replica.reader.read(), 5) == b""
        finally:
            release.set()
        async with asyncio.timeout(5):
            while node(primary).sessions:
                await asyncio.sleep(0)


async def test_large_tls_snapshot_with_concurrent_transaction_and_expiration(
    certs: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = ManualClock(1000)
    primary = await start_server(
        recovery_config(tmp_path / "primary.aof", **primary_options(certs, tmp_path)), clock=clock
    )
    owner = primary.persistence
    assert owner is not None and owner.generations is not None
    entered, release = threading.Event(), threading.Event()
    original = owner.generations._stage

    def pause(stage: str) -> None:
        if stage == "replication_snapshot":
            entered.set()
            assert release.wait(10)
        original(stage)

    monkeypatch.setattr(owner.generations, "_stage", pause)
    async with primary, tls_peer(primary, certs, certificate="client") as writer:
        await login(writer)
        large = b"\x00\xff\r\n" * 65536
        assert await writer.call(b"SET", b"large", large) == OK
        replica = await start_server(
            recovery_config(tmp_path / "replica.aof", **replica_options(certs, tmp_path, primary)),
            clock=clock,
        )
        async with replica, tls_peer(replica, certs) as reader:
            await login(reader)
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                replies = await writer.pipeline(
                    [
                        (b"MULTI",),
                        (b"INCR", b"counter"),
                        (b"INCR", b"counter"),
                        (b"SET", b"expiring", b"value", b"PX", b"500"),
                        (b"EXEC",),
                    ]
                )
                assert replies[-1] == Array((Integer(1), Integer(2), OK))
            finally:
                release.set()
            await node(replica).wait_ready()
            await node(replica).wait_offset(node(primary).offset)
            assert await reader.call(b"GET", b"large") == Bulk(large)
            assert await reader.call(b"GET", b"counter") == Bulk(b"2")
            assert await reader.call(b"PTTL", b"expiring") == Integer(500)


async def test_wrong_password_or_permission_exposes_no_snapshot(
    certs: Path, tmp_path: Path
) -> None:
    primary = await start_server(
        recovery_config(tmp_path / "primary.aof", **primary_options(certs, tmp_path))
    )
    async with primary, tls_peer(primary, certs, certificate="client") as admin:
        await login(admin)
        await admin.call(b"SET", b"secret-key", b"secret-value")
        async with tls_peer(primary, certs, certificate="client") as peer_client:
            assert isinstance(await peer_client.call(b"PYREDIS.SYNC", b"1"), Error)
            assert isinstance(await peer_client.call(b"AUTH", b"app", b"wrong"), Error)
            assert isinstance(await peer_client.call(b"PYREDIS.SYNC", b"1"), Error)
        assert await admin.call(b"ACL", b"SETUSER", b"app", b"-pyredis.sync") == OK
        async with tls_peer(primary, certs, certificate="client") as peer_client:
            await login(peer_client, b"app")
            denied = await peer_client.call(b"PYREDIS.SYNC", b"1")
            assert isinstance(denied, Error) and denied.payload.startswith(b"NOPERM")
        assert not node(primary).sessions


async def test_plaintext_upstream_never_receives_replica_credentials(
    certs: Path, tmp_path: Path
) -> None:
    seen = bytearray()
    done = asyncio.Event()

    async def observe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            seen.extend(await asyncio.wait_for(reader.read(4096), 5))
        finally:
            writer.close()
            done.set()

    fake = await asyncio.start_server(observe, "127.0.0.1", 0)
    password = tmp_path / "password"
    password.write_bytes(PASSWORD)
    password.chmod(0o600)
    options = {
        **tls_settings(certs),
        "acl_file": bootstrap(tmp_path / "acl.json"),
        "replication_secure": True,
        "replica_host": "127.0.0.1",
        "replica_port": fake.sockets[0].getsockname()[1],
        "replica_username": "app",
        "replica_password_file": str(password),
        "replica_server_name": "localhost",
        "replication_retry_attempts": 1,
    }
    replica = await start_server(recovery_config(tmp_path / "replica.aof", **options))
    try:
        async with replica:
            await asyncio.wait_for(done.wait(), 5)
            async with asyncio.timeout(5):
                while node(replica).phase != "down":
                    await asyncio.sleep(0)
            assert seen and seen[0] == 0x16 and PASSWORD not in seen and b"AUTH" not in seen
    finally:
        fake.close()
        await fake.wait_closed()


async def test_missing_secure_context_never_opens_a_plaintext_connection(
    certs: Path, tmp_path: Path
) -> None:
    accepted = asyncio.Event()

    async def observe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.set()
        writer.close()

    fake = await asyncio.start_server(observe, "127.0.0.1", 0)
    password = tmp_path / "password"
    password.write_bytes(PASSWORD)
    password.chmod(0o600)
    options = {
        **tls_settings(certs),
        "acl_file": bootstrap(tmp_path / "acl.json"),
        "replication_secure": True,
        "replica_host": "127.0.0.1",
        "replica_port": fake.sockets[0].getsockname()[1],
        "replica_username": "app",
        "replica_password_file": str(password),
        "replica_server_name": "localhost",
        "replication_retry_attempts": 1,
    }
    replica = await start_server(recovery_config(tmp_path / "replica.aof", **options))
    node(replica).tls.upstream = None  # Inject a missing context before the supervisor runs.
    try:
        async with replica:
            async with asyncio.timeout(5):
                while node(replica).phase != "down":
                    await asyncio.sleep(0)
            assert not accepted.is_set()
    finally:
        fake.close()
        await fake.wait_closed()


@pytest.mark.parametrize("kind", ["missing", "empty", "oversized"])
async def test_invalid_replication_password_file_fails_before_listen(
    certs: Path, tmp_path: Path, kind: str
) -> None:
    password = tmp_path / "secret-marker"
    if kind != "missing":
        password.write_bytes(b"" if kind == "empty" else b"x" * 1025)
        password.chmod(0o600)
    options = {
        **tls_settings(certs),
        "acl_file": bootstrap(tmp_path / "acl.json"),
        "replication_secure": True,
        "replica_host": "127.0.0.1",
        "replica_port": 12345,
        "replica_username": "app",
        "replica_password_file": str(password),
        "replica_server_name": "localhost",
    }
    with pytest.raises(ValueError) as failure:
        await start_server(recovery_config(tmp_path / "replica.aof", **options))
    assert "secret-marker" not in str(failure.value)


async def test_shutdown_cancels_pending_tls_handshake(certs: Path) -> None:
    server = await start_server(
        ServerConfig(port=0, max_clients=1, tls_handshake_timeout=30, **tls_settings(certs))
    )
    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
    try:
        async with asyncio.timeout(5):
            while not server._tasks:
                await asyncio.sleep(0)
        server.close()
        await asyncio.wait_for(server.wait_closed(), 5)
        try:
            assert await asyncio.wait_for(reader.read(), 5) == b""
        except TimeoutError:
            raise
        except (ConnectionError, OSError):
            pass
        assert not server._tasks
    finally:
        server.close()
        await server.wait_closed()
        await close_writer(writer)


async def test_upstream_error_text_cannot_leak_credentials_to_logs(
    certs: Path,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    received = asyncio.Event()
    handler_errors: list[BaseException] = []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(certs / "server.pem"), str(certs / "server.key"))

    async def reject(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await read_value(reader, ServerConfig())
            assert isinstance(request, Array) and request.payload is not None
            assert request.payload[0] == Bulk(b"AUTH")
            writer.write(b"-ERR synthetic-upstream-secret-marker\r\n")
            await writer.drain()
            assert await asyncio.wait_for(reader.read(1), 5) == b""
        except (AssertionError, TimeoutError, OSError) as error:
            handler_errors.append(error)
        finally:
            received.set()
            writer.close()

    fake = await asyncio.start_server(reject, "127.0.0.1", 0, ssl=context)
    password = tmp_path / "password"
    password.write_bytes(PASSWORD)
    password.chmod(0o600)
    options = {
        **tls_settings(certs),
        "acl_file": bootstrap(tmp_path / "acl.json"),
        "replication_secure": True,
        "replica_host": "127.0.0.1",
        "replica_port": fake.sockets[0].getsockname()[1],
        "replica_username": "app",
        "replica_password_file": str(password),
        "replica_server_name": "localhost",
        "replication_retry_attempts": 1,
    }
    replica = await start_server(recovery_config(tmp_path / "replica.aof", **options))
    try:
        async with replica:
            await asyncio.wait_for(received.wait(), 10)
            assert not handler_errors
            async with asyncio.timeout(5):
                while node(replica).phase != "down":
                    await asyncio.sleep(0)
            assert node(replica).last_error == "upstream authentication failed"
            assert "synthetic-upstream-secret-marker" not in caplog.text
    finally:
        fake.close()
        await fake.wait_closed()
