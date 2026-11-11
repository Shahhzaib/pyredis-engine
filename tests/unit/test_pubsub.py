from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, serialize_reply
from pyredis_engine.pubsub.output import Outbox


def ack(kind: bytes, channel: bytes | None, count: int) -> Array:
    return Array((Bulk(kind), Bulk(channel), Integer(count)))


def test_duplicate_subscriptions_binary_publication_and_unsubscribe_counts() -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        result = engine.execute([b'SUBSCRIBE', b'\xff\x00', b'\xff\x00', b''], client)
        assert result.reply == ack(b'subscribe', b'\xff\x00', 1)
        assert result.extra_replies == (ack(b'subscribe', b'\xff\x00', 1),
                                        ack(b'subscribe', b'', 2))
        assert engine.execute([b'PUBLISH', b'\xff\x00', b'\r\n\x00']).reply == Integer(1)
        result = engine.execute([b'UNSUBSCRIBE', b'\xff\x00', b'\xff\x00'], client)
        assert result.reply == ack(b'unsubscribe', b'\xff\x00', 1)
        assert result.extra_replies == (ack(b'unsubscribe', b'\xff\x00', 1),)
    finally:
        engine.close_client(client)
        engine.close()
    assert not engine.pubsub.channels


@pytest.mark.parametrize('limit', ['count', 'bytes', 'reply'])
def test_subscription_limits_reject_atomically(limit: str) -> None:
    config = ServerConfig(max_subscriptions=1 if limit == 'count' else 1024,
                          max_subscription_bytes=1 if limit == 'bytes' else 1024,
                          max_reply_bytes=128 if limit == 'reply' else 1024)
    engine = Executor(config)
    client = engine.new_client()
    try:
        channels = [b'a', b'b'] if limit != 'reply' else [b'x' * 100, b'y' * 100]
        result = engine.execute([b'SUBSCRIBE', *channels], client)
        assert isinstance(result.reply, Error)
        assert not engine.pubsub.channels
        assert client.subscriber is not None and not client.subscriber.channels
        assert not client.subscriber.output.active
    finally:
        engine.close_client(client)
        engine.close()


def test_publication_recipient_snapshot_and_explicit_commit_release() -> None:
    engine = Executor(ServerConfig())
    subscriber = engine.new_client()
    try:
        engine.execute([b'SUBSCRIBE', b'c'], subscriber)
        endpoint = subscriber.subscriber
        assert endpoint is not None
        while endpoint.output.frames:
            endpoint.output.finish(endpoint.output.frames.popleft())
        publisher = engine.new_client()
        try:
            staged = engine.execute([b'PUBLISH', b'c', b'm'], publisher, defer_pubsub=True)
            assert staged.reply == Integer(1) and not endpoint.output.frames
            engine.complete(staged, publisher)
            assert len(endpoint.output.frames) == 1
            assert endpoint.output.frames[0].data == serialize_reply(
                Array((Bulk(b'message'), Bulk(b'c'), Bulk(b'm'))))
        finally:
            engine.close_client(publisher)
    finally:
        engine.close_client(subscriber)
        engine.close()


@pytest.mark.parametrize('termination', ['discard', 'queue_error', 'watch_abort'])
def test_unexecuted_transactions_leave_no_subscriptions_or_publications(termination: str) -> None:
    engine = Executor(ServerConfig())
    try:
        if termination == 'watch_abort':
            engine.execute([b'WATCH', b'k'])
            client = engine.new_client()
            try:
                engine.execute([b'SET', b'k', b'changed'], client)
            finally:
                engine.close_client(client)
        engine.execute([b'MULTI'])
        engine.execute([b'SUBSCRIBE', b'c'])
        engine.execute([b'PUBLISH', b'c', b'm'])
        if termination == 'discard':
            assert engine.execute([b'DISCARD']).reply == Simple(b'OK')
        else:
            if termination == 'queue_error':
                engine.execute([b'PUBLISH', b'c'])
            result = engine.execute([b'EXEC']).reply
            if termination == 'watch_abort':
                assert result == Array(None)
            else:
                assert isinstance(result, Error)
        assert not engine.pubsub.channels
    finally:
        engine.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('limit', ['bytes', 'frames'])
async def test_outbox_counts_inflight_data_and_releases_every_waiter(limit: str) -> None:
    config = replace(ServerConfig(), pubsub_queue_bytes=5 if limit == 'bytes' else 100,
                     pubsub_queue_frames=1 if limit == 'frames' else 10)
    output = Outbox(config)
    first = output.enqueue(b'abcde')
    assert first is not None and await output.get() is first
    assert output.pending_bytes == 5 and output.pending_frames == 1
    assert output.enqueue(b'x') is None and output.failed
    assert not first.done.is_set()  # the writer still owns this frame
    output.finish(first)
    assert first.done.is_set() and not first.data
    assert output.pending_bytes == output.pending_frames == 0
    assert await output.get() is None


@pytest.mark.asyncio
async def test_failed_outbox_wakes_queued_and_idle_consumers() -> None:
    output = Outbox(ServerConfig())
    queued = output.enqueue(b'payload')
    assert queued is not None
    output.fail()
    assert queued.done.is_set() and not queued.data
    assert output.pending_bytes == output.pending_frames == 0
    idle = Outbox(ServerConfig())
    waiting = asyncio.create_task(idle.get())
    await asyncio.sleep(0)
    idle.fail()
    assert await waiting is None


@pytest.mark.parametrize('field', ['max_subscriptions', 'max_subscription_bytes',
                                  'pubsub_queue_bytes', 'pubsub_queue_frames'])
@pytest.mark.parametrize('value', [0, True, 1.5])
def test_invalid_pubsub_configuration(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(ServerConfig(), **{field: value})


def test_oversized_publication_and_unsubscribe_confirmation_do_not_mutate_state() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    subscriber = engine.new_client()
    try:
        assert engine.execute([b'SUBSCRIBE', b'c'], subscriber).reply == ack(b'subscribe', b'c', 1)
        target = subscriber.subscriber
        assert target is not None
        queued = target.output.pending_frames
        assert isinstance(engine.execute([b'PUBLISH', b'c', b'x' * 128]).reply, Error)
        assert target.output.pending_frames == queued
        result = engine.execute([b'UNSUBSCRIBE', b'c', b'x' * 128], subscriber)
        assert isinstance(result.reply, Error)
        assert list(target.channels) == [b'c'] and engine.pubsub.channels[b'c'] == {target}
    finally:
        engine.close_client(subscriber)
        engine.close()
