from __future__ import annotations

from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap

from pyredis_engine.commands.registry import result_payload
from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Simple


def test_exec_preflights_selected_protocol_before_committing() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    client = engine.new_client()
    try:
        for number in range(6):
            assert not isinstance(engine.execute(
                [b"ZADD", b"z", b"1.25", f"m{number:02}".encode()], client).reply, Error)
        client.protocol_version = 3
        assert engine.execute([b"MULTI"], client).reply == Simple(b"OK")
        engine.execute([b"SET", b"result", b"committed"], client)
        engine.execute([b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"], client)
        result = engine.execute([b"EXEC"], client).reply
        assert isinstance(result, Error) and result.payload.startswith(b"EXECABORT")
        assert engine.execute([b"GET", b"result"], client).reply == Bulk(None)

        client.protocol_version = 2
        engine.execute([b"MULTI"], client)
        engine.execute([b"SET", b"result", b"committed"], client)
        engine.execute([b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"], client)
        assert isinstance(engine.execute([b"EXEC"], client).reply, Array)
        assert engine.execute([b"GET", b"result"], client).reply == Bulk(b"committed")
    finally:
        engine.close()


def test_hello_changes_only_the_requesting_client() -> None:
    config = ServerConfig()
    engine = Executor(config)
    first, second = engine.new_client(), engine.new_client()
    try:
        reply = engine.execute([b"HELLO", b"3"], first)
        assert first.protocol_version == 3
        assert second.protocol_version == 2
        assert result_payload(reply, protocol=3, config=config).startswith(b"%7\r\n")
        assert engine.execute([b"HELLO", b"4"], first).reply == Error(
            b"NOPROTO unsupported protocol version")
        assert first.protocol_version == 3
        engine.execute([b"HELLO", b"2"], first)
        assert first.protocol_version == 2
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_hello_auth_oversize_preserves_identity_protocol_and_name(tmp_path: Path) -> None:
    engine = Executor(ServerConfig(acl_file=bootstrap(tmp_path / 'acl.json'), max_reply_bytes=128))
    client = engine.new_client()
    client.client_name = b'old'
    try:
        result = await engine.hello_command(
            [b'HELLO', b'3', b'AUTH', b'admin', PASSWORD, b'SETNAME', b'new'], client)
        assert result.reply == Error(b'ERR reply exceeds configured maximum size')
        assert client.username is None
        assert client.identity is None
        assert client.protocol_version == 2
        assert client.client_name == b'old'
        assert engine.execute([b'PING'], client).reply == Error(b'NOAUTH Authentication required.')
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_hello_failure_preserves_existing_auth_and_watch(tmp_path: Path) -> None:
    engine = Executor(ServerConfig(acl_file=bootstrap(tmp_path / 'acl.json'), max_reply_bytes=128))
    client = engine.new_client()
    try:
        auth = await engine.security_command([b'AUTH', b'admin', PASSWORD], client)
        assert auth.reply == Simple(b'OK')
        assert engine.execute([b'WATCH', b'key'], client).reply == Simple(b'OK')
        identity = client.identity
        result = await engine.hello_command([b'HELLO', b'3', b'AUTH', b'admin', PASSWORD], client)
        assert isinstance(result.reply, Error)
        assert client.username == b'admin' and client.identity is identity
        assert client.watched == {b'key'}
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_hello_acl_denial_does_not_switch_identity(tmp_path: Path) -> None:
    engine = Executor(ServerConfig(acl_file=bootstrap(tmp_path / 'acl.json')))
    client = engine.new_client()
    try:
        auth = await engine.security_command([b'AUTH', b'admin', PASSWORD], client)
        assert auth.reply == Simple(b'OK')
        update = await engine.security_command(
            [b'ACL', b'SETUSER', b'limited', b'on', b'>pass', b'+ping'], client)
        assert update.reply == Simple(b'OK')
        identity = client.identity
        result = await engine.hello_command([b'HELLO', b'3', b'AUTH', b'limited', b'pass'], client)
        assert isinstance(result.reply, Error) and result.reply.payload.startswith(b'NOPERM')
        assert client.username == b'admin' and client.identity is identity
        assert client.protocol_version == 2
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_hello_auth_exact_reply_limit(tmp_path: Path) -> None:
    acl_file = bootstrap(tmp_path / 'acl.json')
    config = ServerConfig(acl_file=acl_file)
    probe = Executor(config)
    try:
        client = probe.new_client()
        reply = await probe.hello_command([b'HELLO', b'3', b'AUTH', b'admin', PASSWORD], client)
        size = len(result_payload(reply, protocol=3, config=config))
    finally:
        probe.close()
    assert size > 128
    for limit in (size - 1, size):
        engine = Executor(ServerConfig(acl_file=acl_file, max_reply_bytes=limit))
        client = engine.new_client()
        try:
            reply = await engine.hello_command(
                [b'HELLO', b'3', b'AUTH', b'admin', PASSWORD], client)
            if limit < size:
                assert isinstance(reply.reply, Error)
                assert client.username is None and client.protocol_version == 2
            else:
                assert isinstance(reply.reply, Array)
                assert client.username == b'admin' and client.protocol_version == 3
        finally:
            engine.close()
