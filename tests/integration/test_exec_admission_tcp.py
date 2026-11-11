from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.recovery import config, node
from _support.redis_reference import ReferenceError

from pyredis_engine import server as network
from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.persistence.coordinator import Persistence
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

OVERFLOW = Error(b'EXECABORT Transaction discarded because reply exceeds configured maximum size')


async def test_pipelined_rejection_does_not_desynchronize_next_transaction() -> None:
    server = await start_server(ServerConfig(port=0, max_reply_bytes=128))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        replies = await client.pipeline([
            (b'MULTI',), (b'SET', b'\x00\xff', b'x' * 112), (b'GET', b'\x00\xff'), (b'EXEC',),
            (b'GET', b'\x00\xff'), (b'PING',), (b'MULTI',), (b'INCR', b'count'),
            (b'EXEC',), (b'GET', b'count'),
        ])
        assert replies == [Simple(b'OK'), Simple(b'QUEUED'), Simple(b'QUEUED'), OVERFLOW,
                           Bulk(None), Simple(b'PONG'), Simple(b'OK'), Simple(b'QUEUED'),
                           Array((Integer(1),)), Bulk(b'1')]


async def test_admitted_large_binary_reply_uses_existing_chunked_output() -> None:
    payload = b'\x00\r\n\xff' * 65536
    server = await start_server(ServerConfig(port=0, max_output_bytes=1024))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        replies = await client.pipeline([(b'MULTI',), (b'SET', b'k', payload),
                                         (b'GET', b'k'), (b'EXEC',)])
        assert replies[-1] == Array((Simple(b'OK'), Bulk(payload)))
        assert await client.call(b'GET', b'k') == Bulk(payload)


async def test_transport_failure_after_commit_is_not_a_safe_reply_rejection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = ServerConfig(port=0, aof_path=str(tmp_path / 'journal.aof'), aof_fsync='always')
    server = await start_server(settings)
    original = network._safe_send
    injected = False

    async def fail_reply(writer: asyncio.StreamWriter, payload: bytes,
                         limits: ServerConfig) -> None:
        nonlocal injected
        if payload == serialize_reply(Array((Integer(1),))):
            injected = True
            writer.transport.abort()
            raise ConnectionResetError('injected loss of committed EXEC response')
        await original(writer, payload, limits)

    monkeypatch.setattr(network, '_safe_send', fail_reply)
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        await client.call(b'MULTI')
        await client.call(b'INCR', b'count')
        with pytest.raises((ReferenceError, OSError)):
            await client.call(b'EXEC')
        assert injected and server.persistence is not None
        assert server.persistence.file.sequence == server.persistence.file.synced_sequence == 1
    restarted = await start_server(settings)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as client:
        assert await client.call(b'GET', b'count') == Bulk(b'1')


async def test_rejected_effects_never_enter_journal_or_replication_and_retry_commits_once(
    tmp_path: Path,
) -> None:
    primary_config = config(tmp_path / 'primary.aof', max_reply_bytes=128)
    primary = await start_server(primary_config)
    async with primary, peer(primary.sockets[0].getsockname()[1]) as writer:
        await writer.call(b'SET', b'large', b'x' * 100)
        replica = await start_server(config(tmp_path / 'replica.aof', max_reply_bytes=128,
            replica_host='127.0.0.1', replica_port=primary.sockets[0].getsockname()[1]))
        async with replica, peer(replica.sockets[0].getsockname()[1]) as reader:
            await node(replica).wait_ready()
            owner = primary.persistence
            assert owner is not None
            before = owner.file.sequence, node(primary).offset
            replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'count'),
                (b'GET', b'large'), (b'GET', b'large'), (b'EXEC',)])
            assert replies[-1] == OVERFLOW
            assert (owner.file.sequence, node(primary).offset) == before
            assert await reader.call(b'GET', b'count') == Bulk(None)
            replies = await writer.pipeline([(b'MULTI',), (b'INCR', b'count'), (b'EXEC',)])
            assert replies[-1] == Array((Integer(1),))
            assert owner.file.sequence == before[0] + 1
            await node(replica).wait_offset(node(primary).offset)
            assert await reader.call(b'GET', b'count') == Bulk(b'1')
    restarted = await start_server(primary_config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as reader:
        assert await reader.call(b'GET', b'count') == Bulk(b'1')


@pytest.mark.parametrize('operation', ['append', 'sync'])
async def test_admitted_exec_disk_failure_is_fail_stop_not_reply_abort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    settings = ServerConfig(aof_path=str(tmp_path / 'journal.aof'), aof_fsync='always')
    executor = Executor(settings)
    owner = await Persistence.open(executor, settings)
    client = executor.new_client()
    try:
        assert (await owner.execute([b'SET', b'safe', b'old'], client)).reply == Simple(b'OK')
        await owner.execute([b'MULTI'], client)
        await owner.execute([b'INCR', b'count'], client)

        def fail(*args: object) -> None:
            raise OSError('injected EXEC commit failure')

        monkeypatch.setattr(owner.file, operation, fail)
        result = await owner.execute([b'EXEC'], client)
        assert result.close and isinstance(result.reply, Error)
        assert result.reply.payload.startswith(b'MISCONF') and result.reply != OVERFLOW
        assert owner.failure is not None
    finally:
        await owner.close()
        executor.close_client(client)
        executor.close()
    restored = Executor(settings)
    recovery = await Persistence.open(restored, settings)
    try:
        assert restored.execute([b'GET', b'safe']).reply == Bulk(b'old')
        # A complete, unacknowledged group can survive a failed fsync.
        assert restored.execute([b'GET', b'count']).reply == Bulk(None if operation == 'append'
                                                               else b'1')
    finally:
        await recovery.close()
        restored.close()
