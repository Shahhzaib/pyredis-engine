from __future__ import annotations

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, serialize_reply


def ack(kind: bytes, pattern: bytes | None, count: int) -> Array:
    return Array((Bulk(kind), Bulk(pattern), Integer(count)))


def test_patterns_deliver_with_combined_counts_and_rollback() -> None:
    engine = Executor(ServerConfig())
    listener = engine.new_client()
    try:
        assert engine.execute([b"SUBSCRIBE", b"a:1"], listener).reply == ack(
            b"subscribe", b"a:1", 1
        )
        assert engine.execute([b"PSUBSCRIBE", b"a:*", b"a:*"], listener).reply == ack(
            b"psubscribe", b"a:*", 2
        )
        subscriber = listener.subscriber
        assert subscriber is not None
        while subscriber.output.frames:
            subscriber.output.finish(subscriber.output.frames.popleft())
        assert engine.execute([b"PUBLISH", b"a:1", b"\x00\xff"]).reply == Integer(2)
        frames = [frame.data for frame in subscriber.output.frames]
        assert frames == [
            serialize_reply(Array((Bulk(b"message"), Bulk(b"a:1"), Bulk(b"\x00\xff")))),
            serialize_reply(
                Array((Bulk(b"pmessage"), Bulk(b"a:*"), Bulk(b"a:1"), Bulk(b"\x00\xff")))
            ),
        ]
        assert engine.execute([b"PUNSUBSCRIBE", b"a:*"], listener).reply == ack(
            b"punsubscribe", b"a:*", 1
        )
        assert engine.execute([b"UNSUBSCRIBE", b"a:1"], listener).reply == ack(
            b"unsubscribe", b"a:1", 0
        )
        assert engine.execute([b"PING"], listener).reply == Simple(b"PONG")
    finally:
        engine.close()


def test_pattern_limits_and_transaction_reply_abort_are_atomic() -> None:
    engine = Executor(ServerConfig(max_subscriptions=2, max_reply_bytes=128))
    client = engine.new_client()
    try:
        assert isinstance(engine.execute([b"PSUBSCRIBE", b"a", b"b", b"c"], client).reply, Error)
        assert not engine.pubsub.patterns
        assert engine.execute([b"MULTI"], client).reply == Simple(b"OK")
        assert engine.execute([b"PSUBSCRIBE", b"a:*"], client).reply == Simple(b"QUEUED")
        assert engine.execute([b"ECHO", b"x" * 120], client).reply == Simple(b"QUEUED")
        assert isinstance(engine.execute([b"EXEC"], client).reply, Error)
        assert not engine.pubsub.patterns
        assert client.subscriber is not None and not client.subscriber.patterns
    finally:
        engine.close()


def test_publication_fanout_preflight_prevents_partial_delivery() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128, pubsub_queue_bytes=128))
    subscriber = engine.new_client()
    try:
        assert engine.execute([b"PSUBSCRIBE", b"a*", b"?*"], subscriber).reply == ack(
            b"psubscribe", b"a*", 1
        )
        target = subscriber.subscriber
        assert target is not None
        while target.output.frames:
            target.output.finish(target.output.frames.popleft())
        result = engine.execute([b"PUBLISH", b"abc", b"x" * 75])
        assert isinstance(result.reply, Error)
        assert not target.output.frames
    finally:
        engine.close()


def test_global_pattern_cap_releases_capacity_after_unsubscribe() -> None:
    engine = Executor(ServerConfig(max_subscriptions=1))
    first, second = engine.new_client(), engine.new_client()
    try:
        assert engine.execute([b"PSUBSCRIBE", b"a*"], first).reply == ack(b"psubscribe", b"a*", 1)
        assert isinstance(engine.execute([b"PSUBSCRIBE", b"b*"], second).reply, Error)
        assert engine.execute([b"PUNSUBSCRIBE"], first).reply == ack(b"punsubscribe", b"a*", 0)
        assert engine.execute([b"PSUBSCRIBE", b"b*"], second).reply == ack(b"psubscribe", b"b*", 1)
    finally:
        engine.close()
