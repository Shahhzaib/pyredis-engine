from __future__ import annotations

import asyncio
import json
import os
import threading
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap, verifier

from pyredis_engine.commands.registry import Registry
from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.persistence.effects import Capture
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.security.credentials import PasswordHash
from pyredis_engine.security.permissions import MatchBudget, Pattern
from pyredis_engine.security.policy import NOAUTH, WRONGPASS, AccessControl
from pyredis_engine.security.worker import SecurityBusy, SecurityWorker

OK = Simple(b"OK")


def test_local_default_has_an_explicit_identity() -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute([b"ACL", b"WHOAMI"]).reply == Bulk(b"default")
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_protected_authentication_forms_isolation_failure_and_disabled_users(
    tmp_path: Path,
) -> None:
    engine = Executor(ServerConfig(acl_file=bootstrap(tmp_path / "acl.json")))
    admin, other = engine.new_client(), engine.new_client()
    try:
        assert engine.execute([b"PING"], admin).reply == NOAUTH
        assert engine.execute([b"GET", b"k"], admin).reply == NOAUTH
        assert (await engine.security_command([b"AUTH", b"admin", PASSWORD], admin)).reply == OK
        assert engine.execute([b"ACL", b"WHOAMI"], admin).reply == Bulk(b"admin")
        assert engine.execute([b"PING"], other).reply == NOAUTH
        assert (
            await engine.security_command([b"AUTH", b"unknown", PASSWORD], other)
        ).reply == WRONGPASS
        assert (
            await engine.security_command([b"AUTH", b"admin", b"wrong"], admin)
        ).reply == WRONGPASS
        assert engine.execute([b"ACL", b"WHOAMI"], admin).reply == Bulk(b"admin")
        assert (
            engine.execute([b"ACL", b"SETUSER", b"admin", b"off"], admin).reply != OK
        )  # last admin
        assert engine.execute([b"ACL", b"SETUSER", b"u", b"on", b"nopass"], admin).reply != OK
        assert (
            await engine.security_command(
                [b"ACL", b"SETUSER", b"default", b"on", b">synthetic-default", b"+get", b"~*"],
                admin,
            )
        ).reply == OK
        assert (await engine.security_command([b"AUTH", b"synthetic-default"], other)).reply == OK
        assert engine.execute([b"ACL", b"SETUSER", b"default", b"off"], admin).reply == OK
        # Redis off affects new authentication, not the existing connection identity.
        assert engine.execute([b"GET", b"k"], other).reply == Bulk(None)
        assert (
            await engine.security_command([b"AUTH", b"synthetic-default"], other)
        ).reply == WRONGPASS
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_commands_all_keys_and_channels_are_checked_before_any_access() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    client = engine.new_client()
    try:
        engine.execute([b"SET", b"forbidden", b"value", b"PX", b"1"])
        engine.execute(
            [
                b"ACL",
                b"SETUSER",
                b"app",
                b"on",
                b"nopass",
                b"+get",
                b"+set",
                b"+mget",
                b"+mset",
                b"+del",
                b"+exists",
                b"+watch",
                b"+subscribe",
                b"+publish",
                b"+acl|whoami",
                b"~ok:*",
                b"&events:*",
            ]
        )
        assert (await engine.security_command([b"AUTH", b"app", b"anything"], client)).reply == OK
        clock.advance(2)
        before = (
            engine.store.physical_count,
            engine.store.memory_bytes,
            engine.store.mutation_revision,
        )
        denied = [
            [b"GET", b"forbidden"],
            [b"SET", b"forbidden", b"new"],
            [b"MGET", b"ok:one", b"forbidden"],
            [b"MSET", b"ok:one", b"v", b"forbidden", b"x"],
            [b"DEL", b"ok:one", b"forbidden"],
            [b"EXISTS", b"ok:one", b"forbidden"],
            [b"WATCH", b"ok:one", b"forbidden"],
            [b"SUBSCRIBE", b"events:ok", b"other"],
            [b"PUBLISH", b"other", b"message"],
            [b"FLUSHDB"],
            [b"ACL", b"USERS"],
        ]
        with Capture(engine.store) as captured:
            for command in denied:
                result = engine.execute(command, client).reply
                assert isinstance(result, Error) and result.payload.startswith(b"NOPERM"), command[
                    0
                ]
        assert not captured.group().entries
        assert (
            engine.store.physical_count,
            engine.store.memory_bytes,
            engine.store.mutation_revision,
        ) == before
        assert engine.execute([b"GET", b"ok:one"]).reply == Bulk(None)
        assert not client.watched and not engine.pubsub.channels
        assert engine.execute([b"SET", b"ok:\x00\xff", b"\r\n"], client).reply == OK
        assert engine.execute([b"GET", b"ok:\x00\xff"], client).reply == Bulk(b"\r\n")
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_queue_time_denial_poison_and_exec_rechecks_without_unauthorized_effects() -> None:
    engine = Executor(ServerConfig())
    app = engine.new_client()
    try:
        engine.execute(
            [
                b"ACL",
                b"SETUSER",
                b"app",
                b"on",
                b"nopass",
                b"+set",
                b"+get",
                b"+multi",
                b"+exec",
                b"~*",
            ]
        )
        await engine.security_command([b"AUTH", b"app", b"any"], app)
        engine.execute([b"MULTI"], app)
        assert isinstance(engine.execute([b"DEL", b"k"], app).reply, Error)
        assert engine.execute([b"EXEC"], app).reply == Error(
            b"EXECABORT Transaction discarded because of previous errors."
        )
        engine.execute([b"MULTI"], app)
        engine.execute([b"SET", b"ok:k", b"v"], app)
        engine.execute([b"SET", b"bad:k", b"v"], app)
        engine.execute([b"ACL", b"SETUSER", b"app", b"resetkeys", b"~ok:*"])
        result = engine.execute([b"EXEC"], app).reply
        assert isinstance(result, Array) and result.payload is not None
        assert result.payload[0] == OK
        assert isinstance(result.payload[1], Error) and b"keys" in result.payload[1].payload
        assert engine.execute([b"GET", b"bad:k"]).reply == Bulk(None)
        assert engine.execute([b"GET", b"ok:k"]).reply == Bulk(b"v")
        assert not app.multi and not app.watched
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_credentials_and_admin_controls_never_enter_transaction_queues() -> None:
    engine = Executor(ServerConfig())
    try:
        for command in (
            [b"AUTH", b"synthetic-secret"],
            [b"ACL", b"SETUSER", b"u", b">synthetic-secret"],
        ):
            engine.execute([b"MULTI"])
            result = await engine.security_command(command, engine._default_client)
            assert isinstance(result.reply, Error)
            assert not engine._default_client.queue
            assert b"synthetic-secret" not in result.reply.payload
            assert isinstance(engine.execute([b"EXEC"]).reply, Error)
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_password_rotation_removal_and_existing_identity_semantics() -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        assert (
            await engine.security_command(
                [b"ACL", b"SETUSER", b"u", b"on", b">old", b"+get", b"+acl|whoami", b"~*"],
                engine._default_client,
            )
        ).reply == OK
        assert (await engine.security_command([b"AUTH", b"u", b"old"], client)).reply == OK
        assert (
            await engine.security_command(
                [b"ACL", b"SETUSER", b"u", b">new"], engine._default_client
            )
        ).reply == OK
        assert (
            await engine.security_command(
                [b"ACL", b"SETUSER", b"u", b"<old"], engine._default_client
            )
        ).reply == OK
        assert engine.execute([b"GET", b"k"], client).reply == Bulk(None)
        assert (await engine.security_command([b"AUTH", b"u", b"old"], client)).reply == WRONGPASS
        assert (await engine.security_command([b"AUTH", b"u", b"new"], client)).reply == OK
        assert engine.execute([b"ACL", b"SETUSER", b"u", b"off"]).reply == OK
        assert engine.execute([b"ACL", b"WHOAMI"], client).reply == Bulk(b"u")
        assert (await engine.security_command([b"AUTH", b"u", b"new"], client)).reply == WRONGPASS
        assert engine.execute([b"ACL", b"DELUSER", b"u"]).reply == Integer(1)
        assert client.closed
        assert engine.execute([b"ACL", b"SETUSER", b"u", b"on", b"nopass", b"+@all"]).reply == OK
        assert (
            engine.access.current(client) is None
        )  # deletion/recreation cannot revive old identity
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_channel_revocation_closes_subscriber_and_drops_queued_messages() -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        engine.execute([b"ACL", b"SETUSER", b"u", b"on", b"nopass", b"+subscribe", b"&*"])
        await engine.security_command([b"AUTH", b"u", b"any"], client)
        engine.execute([b"SUBSCRIBE", b"events:\xff"], client)
        engine.execute([b"PUBLISH", b"events:\xff", b"queued"])
        assert client.subscriber is not None and client.subscriber.output.pending_bytes
        assert engine.execute([b"ACL", b"SETUSER", b"u", b"resetchannels", b"&other:*"]).reply == OK
        assert client.closed and not engine.pubsub.channels
        assert (
            not client.subscriber.output.pending_bytes
            and not client.subscriber.output.pending_frames
        )
    finally:
        engine.close()


@pytest.mark.parametrize(
    "rule",
    [
        b"+@read",
        b"%R~*",
        b"#" + b"0" * 64,
        b"!hash",
        b"( +get )",
        b"clearselectors",
        b"~[",
        b">" + b"x" * 1025,
    ],
)
@pytest.mark.asyncio
async def test_invalid_or_unsupported_rules_are_atomic_and_redacted(rule: bytes) -> None:
    engine = Executor(ServerConfig())
    try:
        before = dict(engine.access.users)
        result = await engine.security_command(
            [b"ACL", b"SETUSER", b"u", b"+@all", rule], engine._default_client
        )
        assert isinstance(result.reply, Error)
        assert engine.access.users == before
        assert rule not in result.reply.payload
    finally:
        engine.close()


@pytest.mark.parametrize(
    "command",
    [
        [b"AUTH"],
        [b"AUTH", b"u", b"p", b"extra"],
        [b"ACL"],
        [b"ACL", b"GETUSER", b"default"],
        [b"ACL", b"SAVE"],
    ],
)
@pytest.mark.asyncio
async def test_malformed_security_requests_return_bounded_errors(command: list[bytes]) -> None:
    engine = Executor(ServerConfig())
    try:
        result = await engine.security_command(command, engine._default_client)
        assert isinstance(result.reply, Error) and len(result.reply.payload) < 128
        if command[0] == b"AUTH" and len(command) > 3:
            assert result.reply == Error(b"ERR syntax error")
    finally:
        engine.close()


@pytest.mark.parametrize(
    "pattern,value,allowed",
    [
        (b"ok:*", b"ok:\x00\xff\r\n", True),
        (b"?", b"\xff", True),
        (b"?", b"", False),
        (b"[a-c]?[^^]", b"b\x00x", True),
        (b"[^a-c]*", b"b", False),
        (b"a*b*c", b"abbbc", True),
        (b"a*b*c", b"abbbd", False),
        (b"\\*", b"*", True),
        (b"[z-a]", b"m", True),
        (b"[a-\\z]", b"m", True),
    ],
)
def test_binary_glob_semantics(pattern: bytes, value: bytes, allowed: bool) -> None:
    assert Pattern.compile(pattern).matches(value, MatchBudget()) is allowed


def test_pattern_work_and_unknown_metadata_fail_closed() -> None:
    assert not Pattern.compile(b"*?*").matches(b"x" * 100, MatchBudget(10))
    with pytest.raises(RuntimeError, match="key specifications"):
        AccessControl(Registry(()), None)


def test_password_verifiers_are_salted_constant_time_compared_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hmac

    original = hmac.compare_digest
    compared: list[tuple[int, int]] = []

    def constant_compare(left: bytes, right: bytes) -> bool:
        compared.append((len(left), len(right)))
        return original(left, right)

    monkeypatch.setattr(hmac, "compare_digest", constant_compare)
    first, second = verifier(), PasswordHash.create(PASSWORD)
    assert first.salt != second.salt and first.digest != second.digest
    assert first.verify(PASSWORD) and not first.verify(b"wrong")
    assert compared == [(32, 32), (32, 32)]
    assert PasswordHash.parse(first.export()) == first
    assert first.digest.hex() not in repr(first) and first.salt.hex() not in repr(first)


@pytest.mark.parametrize(
    "change",
    [
        "version",
        "unknown",
        "missing-admin",
        "nopass",
        "verifier",
        "plaintext",
        "duplicate",
        "too-large",
    ],
)
def test_invalid_protected_configuration_fails_closed_without_disclosure(
    tmp_path: Path,
    change: str,
) -> None:
    path = Path(bootstrap(tmp_path / "acl.json"))
    content = json.loads(path.read_text())
    if change == "version":
        content["version"] = True
    elif change == "unknown":
        content["secret-setting"] = "synthetic-config-secret"
    elif change == "missing-admin":
        content["users"][0]["rules"] = ["on", "+get"]
    elif change == "nopass":
        content["users"][0]["rules"].append("nopass")
    elif change == "verifier":
        content["users"][0]["passwords"] = ["synthetic-config-secret"]
    elif change == "plaintext":
        content["users"][0]["rules"].append(">synthetic-config-secret")
    elif change == "duplicate":
        content["users"].append(content["users"][0])
    if change == "too-large":
        path.write_bytes(b" " * 65537)
    else:
        path.write_text(json.dumps(content))
    with pytest.raises(ValueError, match="invalid or unsafe ACL configuration") as error:
        Executor(ServerConfig(acl_file=str(path)))
    assert "synthetic-config-secret" not in str(error.value)


def test_protected_network_configuration_never_enables_remote_or_replication(
    tmp_path: Path,
) -> None:
    acl = bootstrap(tmp_path / "acl.json")
    with pytest.raises(ValueError, match="non-loopback"):
        ServerConfig(acl_file=acl, host="0.0.0.0")
    with pytest.raises(ValueError, match="E01B"):
        ServerConfig(acl_file=acl, replication_enabled=True)
    with pytest.raises(ValueError, match="ACL file"):
        ServerConfig(acl_file="")


@pytest.mark.asyncio
async def test_bounded_worker_cancellation_and_shutdown_keep_ownership() -> None:
    worker = SecurityWorker()
    entered, release = threading.Event(), threading.Event()

    def operation() -> int:
        entered.set()
        assert release.wait(5)
        return 1

    tasks = [asyncio.create_task(worker.run(operation)) for _ in range(4)]
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        with pytest.raises(SecurityBusy):
            await worker.run(operation)
        tasks[0].cancel()
        await asyncio.sleep(0)
        assert worker.pending == 4 and not tasks[0].done()
    finally:
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        worker.close()
    assert isinstance(results[0], asyncio.CancelledError)
    assert results[1:] == [1, 1, 1] and worker.pending == 0


@pytest.mark.asyncio
async def test_deleted_session_cannot_reauthenticate_after_pending_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    entered, release = threading.Event(), threading.Event()
    engine.execute([b"ACL", b"SETUSER", b"u", b"on", b"nopass", b"+get"])
    await engine.security_command([b"AUTH", b"u", b"any"], client)
    original = engine.access.authentication

    def blocked(*args: object) -> object:
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(engine.access, "authentication", blocked)
    pending = asyncio.create_task(engine.security_command([b"AUTH", b"u", b"any"], client))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert engine.execute([b"ACL", b"DELUSER", b"u"]).reply == Integer(1)
    finally:
        release.set()
    try:
        result = await pending
        assert result.close and result.reply == NOAUTH
        assert client.closed and client.username is None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_policy_snapshot_cannot_overwrite_a_concurrent_administrator_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Executor(ServerConfig())
    admin = engine.new_client()
    engine.execute([b"ACL", b"SETUSER", b"u"])
    entered, release = threading.Event(), threading.Event()
    original = PasswordHash.create

    def blocked(cls: type[PasswordHash], password: bytes) -> PasswordHash:
        entered.set()
        assert release.wait(5)
        return original(password)

    monkeypatch.setattr(PasswordHash, "create", classmethod(blocked))
    pending = asyncio.create_task(
        engine.security_command([b"ACL", b"SETUSER", b"u", b">new"], admin)
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert engine.execute([b"ACL", b"SETUSER", b"u", b"+get"]).reply == OK
    finally:
        release.set()
    try:
        result = await pending
        assert result.reply == Error(b"ERR ACL policy changed; retry the request")
        assert b"GET" in engine.access.users[b"u"].commands
        assert not engine.access.users[b"u"].passwords
    finally:
        engine.close()


def test_bootstrap_generator_never_echoes_credentials_or_overwrites_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from pyredis_engine.security import credentials

    path = tmp_path / "generated.json"
    monkeypatch.setattr("sys.argv", ["acl-bootstrap", str(path)])
    monkeypatch.setattr("getpass.getpass", lambda prompt: "synthetic-interactive-secret")
    credentials.main()
    contents = path.read_bytes()
    assert b"synthetic-interactive-secret" not in contents
    output = capsys.readouterr()
    assert "synthetic-interactive-secret" not in output.out + output.err
    if os.name == "posix":
        assert path.stat().st_mode & 0o077 == 0
    engine = Executor(ServerConfig(acl_file=str(path)))
    engine.close()
    with pytest.raises(SystemExit) as exit_code:
        credentials.main()
    assert exit_code.value.code == 1 and path.read_bytes() == contents


def test_duplicate_fields_and_public_acl_file_are_rejected(tmp_path: Path) -> None:
    path = Path(bootstrap(tmp_path / "acl.json"))
    original = path.read_text()
    path.write_text(original.replace('"version": 1', '"version": 1, "version": 1'))
    with pytest.raises(ValueError, match="unsafe ACL configuration"):
        Executor(ServerConfig(acl_file=str(path)))
    path.write_text(original)
    if os.name == "posix":
        path.chmod(0o644)
        with pytest.raises(ValueError, match="unsafe ACL configuration"):
            Executor(ServerConfig(acl_file=str(path)))


def test_bootstrap_refuses_an_echoing_password_prompt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import getpass
    import warnings

    from pyredis_engine.security import credentials

    path = tmp_path / "uncreated.json"
    monkeypatch.setattr("sys.argv", ["acl-bootstrap", str(path)])

    def echoing(prompt: str) -> str:
        warnings.warn("no private terminal", getpass.GetPassWarning, stacklevel=1)
        return "synthetic-secret"

    monkeypatch.setattr("getpass.getpass", echoing)
    with pytest.raises(SystemExit) as error:
        credentials.main()
    assert error.value.code == 1 and not path.exists()


@pytest.mark.asyncio
async def test_revoked_watch_permission_cannot_expire_an_inaccessible_key() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    client = engine.new_client()
    try:
        engine.execute([b"SET", b"private", b"v", b"PX", b"1"])
        engine.execute(
            [
                b"ACL",
                b"SETUSER",
                b"u",
                b"on",
                b"nopass",
                b"+watch",
                b"+multi",
                b"+exec",
                b"+get",
                b"~*",
            ]
        )
        await engine.security_command([b"AUTH", b"u", b"any"], client)
        engine.execute([b"WATCH", b"private"], client)
        engine.execute([b"MULTI"], client)
        engine.execute([b"GET", b"private"], client)
        clock.advance(2)
        engine.execute([b"ACL", b"SETUSER", b"u", b"resetkeys", b"~allowed:*"])
        before = engine.store.memory_bytes, engine.store.mutation_revision
        with Capture(engine.store) as captured:
            result = engine.execute([b"EXEC"], client)
        assert isinstance(result.reply, Error) and result.reply.payload.startswith(b"EXECABORT")
        assert not captured.group().entries
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
        assert engine.store.physical_count == 1 and not client.watched
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_revoked_administrator_cannot_install_prepared_password_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Executor(ServerConfig())
    actor = engine.new_client()
    engine.execute([b"ACL", b"SETUSER", b"manager", b"on", b"nopass", b"+acl|setuser"])
    await engine.security_command([b"AUTH", b"manager", b"any"], actor)
    entered, release = threading.Event(), threading.Event()
    original = engine.access.prepare_update

    def blocked(name: bytes, *args: object) -> object:
        if name == b"new-user":
            entered.set()
            assert release.wait(5)
        return original(name, *args)

    monkeypatch.setattr(engine.access, "prepare_update", blocked)
    pending = asyncio.create_task(
        engine.security_command(
            [b"ACL", b"SETUSER", b"new-user", b">synthetic-worker-password"], actor
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert engine.execute([b"ACL", b"SETUSER", b"manager", b"-acl|setuser"]).reply == OK
    finally:
        release.set()
    try:
        result = await pending
        assert isinstance(result.reply, Error) and result.reply.payload.startswith(b"NOPERM")
        assert b"new-user" not in engine.access.users
    finally:
        engine.close()
