from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer
from _support.pubsub import ack, message, read

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Integer, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio]


@pytest.mark.parametrize('limit', ['bytes', 'frames'])
async def test_blocked_subscriber_inflight_counts_toward_bound_and_fast_client_continues(
    monkeypatch: pytest.MonkeyPatch, limit: str,
) -> None:
    from pyredis_engine import server as server_module

    payload = b'payload'
    size = len(serialize_reply(message(b'c', payload)))
    config = ServerConfig(port=0, pubsub_queue_bytes=size * 2 if limit == 'bytes' else 1024,
                          pubsub_queue_frames=2 if limit == 'frames' else 1024)
    server = await start_server(config)
    entered, release = asyncio.Event(), asyncio.Event()
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as slow, peer(port) as fast, peer(port) as publisher:
            for subscriber in (slow, fast):
                assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
            slow_port = slow.writer.get_extra_info('sockname')[1]
            targets = list(server.executor.pubsub.channels[b'c'])
            slow_state = next(
                target for target in targets
                if target.on_close.__self__.get_extra_info('peername')[1] == slow_port)
            original = server_module._safe_send

            async def pause(
                writer: asyncio.StreamWriter, data: bytes, limits: ServerConfig,
            ) -> None:
                if writer.get_extra_info('peername')[1] == slow_port:
                    entered.set()
                    await release.wait()
                await original(writer, data, limits)

            monkeypatch.setattr(server_module, '_safe_send', pause)
            try:
                assert await publisher.call(b'PUBLISH', b'c', payload) == Integer(2)
                await asyncio.wait_for(entered.wait(), 5)
                assert slow_state.output.pending_bytes == size
                assert await read(fast) == [message(b'c', payload)]
                for _ in range(2):
                    assert await publisher.call(b'PUBLISH', b'c', payload) == Integer(2)
                    assert await read(fast) == [message(b'c', payload)]
                assert await asyncio.wait_for(slow.reader.read(), 5) == b''
                assert await publisher.call(b'PUBLISH', b'c', payload) == Integer(1)
                assert await read(fast) == [message(b'c', payload)]
                async with asyncio.timeout(5):
                    while server.client_task_count != 2:
                        await asyncio.sleep(0)
                assert slow_state.closed
                assert slow_state.output.pending_bytes == slow_state.output.pending_frames == 0
                assert slow_state not in server.executor.pubsub.channels[b'c']
            finally:
                release.set()
    assert not server.executor.pubsub.channels
    assert not [task for task in asyncio.all_tasks() if task.get_name() == 'pyredis-pubsub-writer']


@pytest.mark.parametrize('reason', ['timeout', 'socket', 'cancel', 'bug'])
async def test_writer_failures_and_cancellation_cleanup_without_hiding_bugs(
    monkeypatch: pytest.MonkeyPatch, reason: str,
) -> None:
    from pyredis_engine import server as server_module

    server = await start_server(ServerConfig(port=0))
    loop = asyncio.get_running_loop()
    original_handler = loop.get_exception_handler()
    reports: list[dict] = []
    loop.set_exception_handler(lambda _, context: reports.append(context))
    try:
        async with server:
            port = server.sockets[0].getsockname()[1]
            async with peer(port) as subscriber, peer(port) as publisher:
                await subscriber.call(b'SUBSCRIBE', b'c')
                target = next(iter(server.executor.pubsub.channels[b'c']))
                original = server_module._safe_send

                async def fail(writer, data, config):
                    if data == serialize_reply(message(b'c', b'm')):
                        if reason == 'cancel':
                            task = next(task for task in asyncio.all_tasks()
                                        if task.get_name() == 'pyredis-pubsub-writer')
                            task.cancel()
                            await asyncio.Event().wait()
                        if reason == 'bug':
                            raise RuntimeError('visible Pub/Sub writer bug')
                        if reason == 'timeout':
                            raise TimeoutError('injected subscriber deadline')
                        raise ConnectionResetError('injected subscriber reset')
                    await original(writer, data, config)

                monkeypatch.setattr(server_module, '_safe_send', fail)
                assert await publisher.call(b'PUBLISH', b'c', b'm') == Integer(1)
                assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''
                async with asyncio.timeout(5):
                    while server.client_task_count != 1:
                        await asyncio.sleep(0)
                assert target.closed and not target.channels and not server.executor.pubsub.channels
                assert target.output.pending_bytes == target.output.pending_frames == 0
                assert await publisher.call(b'PUBLISH', b'c', b'after') == Integer(0)
            if reason == 'bug':
                assert len(reports) == 1
                assert isinstance(reports[0]['exception'], RuntimeError)
                assert str(reports[0]['exception']) == 'visible Pub/Sub writer bug'
            else:
                assert not reports
    finally:
        loop.set_exception_handler(original_handler)
