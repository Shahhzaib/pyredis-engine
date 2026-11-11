from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.pubsub import ack, message, read

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.persistence]


@pytest.mark.parametrize('failed', [False, True])
async def test_mixed_exec_publication_waits_for_commit_and_is_never_replayed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed: bool,
) -> None:
    config = ServerConfig(port=0, aof_path=str(tmp_path / 'journal.aof'), aof_fsync='always')
    server = await start_server(config)
    owner = server.persistence
    assert owner is not None
    entered, release = threading.Event(), threading.Event()
    original = owner.file.sync

    def barrier() -> None:
        entered.set()
        assert release.wait(10)
        if failed:
            raise OSError('injected Pub/Sub commit failure')
        original()

    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as subscriber:
            assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
            target = next(iter(server.executor.pubsub.channels[b'c']))
            client = server.executor.new_client()
            try:
                for command in ([b'MULTI'], [b'SET', b'k', b'v'], [b'PUBLISH', b'c', b'once']):
                    await owner.execute(command, client)
                monkeypatch.setattr(owner.file, 'sync', barrier)
                pending = asyncio.create_task(owner.execute([b'EXEC'], client))
                try:
                    assert await asyncio.to_thread(entered.wait, 5)
                    assert not pending.done()
                    assert not target.output.frames and not target.output.inflight
                    assert not subscriber.reader._buffer  # no bytes escaped the durability barrier
                finally:
                    release.set()
                result = await pending
                if failed:
                    assert result.close and isinstance(result.reply, Error)
                    assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''
                else:
                    assert result.reply == Array((Simple(b'OK'), Integer(1)))
                    assert await read(subscriber) == [message(b'c', b'once')]
                    assert owner.file.sequence == owner.file.synced_sequence == 1
            finally:
                release.set()
                server.executor.close_client(client)
                monkeypatch.setattr(owner.file, 'sync', original)
    restarted = await start_server(config)
    async with restarted, peer(restarted.sockets[0].getsockname()[1]) as subscriber:
        assert not restarted.executor.pubsub.channels
        # Failed commits may leave a full unacknowledged group, but no transient publications.
        assert await subscriber.call(b'GET', b'k') == Bulk(b'v')
        assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
        assert await subscriber.call(b'PING') == Array((Bulk(b'pong'), Bulk(b'')))
