from __future__ import annotations

import pytest

from pyredis_engine.commands.registry import CommandResult
from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import Store
from pyredis_engine.engine.transactions import ClientState
from pyredis_engine.persistence.effects import Capture
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, serialize_reply

OVERFLOW = Error(b'EXECABORT Transaction discarded because reply exceeds configured maximum size')


def test_exec_overflow_rejects_before_any_effect() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    engine.execute([b'SET', b'large', b'x' * 80])
    before = engine.store.memory_bytes, engine.store.mutation_revision
    engine.execute([b'MULTI'])
    for command in ([b'INCR', b'counter'], [b'GET', b'large'], [b'GET', b'large'],
                    [b'INCR', b'after']):
        engine.execute(command)
    assert engine.execute([b'EXEC']).reply == OVERFLOW
    assert engine.execute([b'GET', b'counter']).reply == Bulk(None)
    assert engine.execute([b'GET', b'after']).reply == Bulk(None)
    assert (engine.store.memory_bytes, engine.store.mutation_revision) == before


@pytest.mark.parametrize('size,accepted', [(110, True), (111, True), (112, False)])
def test_exact_boundary_uses_replies_after_earlier_writes(size: int, accepted: bool) -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(max_reply_bytes=128), clock=clock)
    engine.execute([b'SET', b'k', b'original', b'PX', b'500'])
    watcher = engine.new_client()
    engine.execute([b'WATCH', b'k'], watcher)
    before = engine.store.mutation_revision, engine.store.memory_bytes
    engine.execute([b'WATCH', b'k'])
    engine.execute([b'MULTI'])
    engine.execute([b'SET', b'k', b'x' * size])
    engine.execute([b'GET', b'k'])
    result = engine.execute([b'EXEC'])
    expected = Array((Simple(b'OK'), Bulk(b'x' * size)))
    assert len(serialize_reply(expected)) == size + 17
    assert result.reply == (expected if accepted else OVERFLOW)
    assert not engine._default_client.multi and not engine._default_client.watched
    assert engine.execute([b'GET', b'k']).reply == Bulk(b'x' * size if accepted else b'original')
    assert engine.execute([b'PTTL', b'k']).reply == Integer(-1 if accepted else 500)
    if not accepted:
        assert (engine.store.mutation_revision, engine.store.memory_bytes) == before
    engine.execute([b'MULTI'], watcher)
    assert engine.execute([b'EXEC'], watcher).reply == Array(None if accepted else ())


def test_large_single_result_is_not_replaced_after_a_write() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    engine.execute([b'SET', b'large', b'x' * 4096])
    for command in ([b'MULTI'], [b'INCR', b'counter'], [b'GET', b'large']):
        engine.execute(command)
    assert engine.execute([b'EXEC']).reply == OVERFLOW
    assert engine.execute([b'GET', b'counter']).reply == Bulk(None)


@pytest.mark.parametrize('setup,mutations,read,expected', [
    ([b'HSET', b'k', b'f', b'old'], [[b'HSET', b'k', b'f', b'new'],
                                   [b'HDEL', b'k', b'f']], [b'HGET', b'k', b'f'], Bulk(b'old')),
    ([b'RPUSH', b'k', b'old'], [[b'LPOP', b'k'], [b'RPUSH', b'k', b'new']],
     [b'LRANGE', b'k', b'0', b'-1'], Array((Bulk(b'old'),))),
    ([b'SADD', b'k', b'old'], [[b'SREM', b'k', b'old'], [b'SADD', b'k', b'new']],
     [b'SMEMBERS', b'k'], Array((Bulk(b'old'),))),
    ([b'ZADD', b'k', b'1', b'old'], [[b'ZADD', b'k', b'2', b'old'],
                                  [b'ZREM', b'k', b'old']], [b'ZSCORE', b'k', b'old'], Bulk(b'1')),
])
def test_collection_preimages_and_indexes_survive_rejection(
    setup: list[bytes], mutations: list[list[bytes]], read: list[bytes], expected: object,
) -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128), clock=ManualClock(1000))
    engine.execute(setup)
    engine.execute([b'PEXPIRE', b'k', b'500'])
    original = engine.store.physical_entry(b'k')
    before = engine.store.memory_bytes, engine.store.mutation_revision
    engine.execute([b'MULTI'])
    for command in [*mutations, [b'ECHO', b'x' * 128]]:
        engine.execute(command)
    assert engine.execute([b'EXEC']).reply == OVERFLOW
    assert engine.store.physical_entry(b'k') == original
    assert engine.store.physical_entry(b'k')[0] is original[0]
    assert engine.execute(read).reply == expected
    assert engine.execute([b'PTTL', b'k']).reply == Integer(500)
    assert (engine.store.memory_bytes, engine.store.mutation_revision) == before


@pytest.mark.parametrize('operation', [[b'PERSIST', b'k'], [b'PEXPIRE', b'k', b'0'],
                                    [b'GETEX', b'k', b'PX', b'900'], [b'FLUSHDB']])
def test_rejection_restores_deadlines_clear_accounting_and_observers(
    operation: list[bytes],
) -> None:
    changes: list[tuple[bytes, int]] = []
    store = Store(clock=ManualClock(1000), on_change=lambda k, r: changes.append((k, r)))
    engine = Executor(ServerConfig(max_reply_bytes=128), store=store)
    engine.execute([b'SET', b'k', b'old', b'PX', b'500'])
    engine.execute([b'SET', b'untouched', b'value'])
    before = store.memory_bytes, store.mutation_revision, list(changes)
    engine.execute([b'MULTI'])
    for command in [operation, [b'SET', b'new', b'value'], [b'ECHO', b'x' * 128]]:
        engine.execute(command)
    with Capture(store) as captured:
        assert engine.execute([b'EXEC']).reply == OVERFLOW
    assert not captured.group().clear and not captured.group().entries
    assert engine.execute([b'GET', b'k']).reply == Bulk(b'old')
    assert engine.execute([b'GET', b'untouched']).reply == Bulk(b'value')
    assert engine.execute([b'GET', b'new']).reply == Bulk(None)
    assert engine.execute([b'PTTL', b'k']).reply == Integer(500)
    assert (store.memory_bytes, store.mutation_revision, changes) == before
    assert store.expiration_count == 1


def test_admitted_batch_preserves_clear_events_and_each_observable_write() -> None:
    engine = Executor(ServerConfig())
    engine.execute([b'SET', b'k', b'old'])
    watcher = engine.new_client()
    engine.execute([b'WATCH', b'k'], watcher)
    before = engine.store.mutation_revision
    engine.execute([b'MULTI'])
    for command in ([b'SET', b'k', b'temporary'], [b'FLUSHDB'],
                    [b'SET', b'k', b'old'], [b'INCR', b'after']):
        engine.execute(command)
    with Capture(engine.store) as captured:
        result = engine.execute([b'EXEC'])
    assert result.reply == Array((Simple(b'OK'), Simple(b'OK'), Simple(b'OK'), Integer(1)))
    assert engine.store.mutation_revision == before + 4
    group = captured.group()
    assert group.clear and {entry.key for entry in group.entries} == {b'k', b'after'}
    engine.execute([b'MULTI'], watcher)
    assert engine.execute([b'EXEC'], watcher).reply == Array(None)


@pytest.mark.parametrize('phase', ['dispatch', 'result'])
def test_allocation_failure_before_admission_discards_provisional_effects(
    monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    engine = Executor(ServerConfig())
    engine.execute([b'SET', b'k', b'old'])
    before = engine.store.memory_bytes, engine.store.mutation_revision
    engine.execute([b'MULTI'])
    engine.execute([b'SET', b'k', b'new'])
    engine.execute([b'INCR', b'counter'])
    if phase == 'dispatch':
        original = engine._transactions.dispatch

        def fail(args: list[bytes], client: ClientState) -> CommandResult:
            if args[0] == b'INCR':
                raise MemoryError('injected admission allocation')
            return original(args, client)

        monkeypatch.setattr(engine._transactions, 'dispatch', fail)
    else:
        def fail_result(*args: object, **kwargs: object) -> None:
            raise MemoryError('injected result allocation')

        monkeypatch.setattr('pyredis_engine.engine.transactions.Array', fail_result)
    with Capture(engine.store) as captured, pytest.raises(MemoryError):
        engine.execute([b'EXEC'])
    assert engine.failed and not captured.group().entries
    assert engine.store.get_string(b'k') == b'old'
    assert engine.store.get_string(b'counter') is None
    assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
    assert engine.store._exec_stage is None


def test_overflow_discards_subscriptions_and_publications() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    external = engine.new_client()
    engine.execute([b'SUBSCRIBE', b'c'], external)
    target = external.subscriber
    actor = engine._default_client.subscriber
    assert target is not None and actor is not None
    before = target.output.pending_bytes, target.output.pending_frames
    engine.execute([b'MULTI'])
    for command in ([b'SUBSCRIBE', b'c'], [b'PUBLISH', b'c', b'not committed'],
                    [b'UNSUBSCRIBE', b'c'], [b'ECHO', b'x' * 128]):
        engine.execute(command)
    assert engine.execute([b'EXEC']).reply == OVERFLOW
    assert not actor.channels and not actor.channel_bytes and not actor.output.active
    assert engine.pubsub.channels == {b'c': {target}}
    assert (target.output.pending_bytes, target.output.pending_frames) == before
    assert engine.execute([b'PING']).reply == Simple(b'PONG')


def test_expiration_inside_rejected_batch_is_not_published_early() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(max_reply_bytes=128), clock=clock)
    engine.execute([b'SET', b'expiring', b'v', b'PX', b'1'])
    clock.advance(2)
    before = engine.store.memory_bytes, engine.store.mutation_revision
    for command in ([b'MULTI'], [b'GET', b'expiring'], [b'ECHO', b'x' * 128]):
        engine.execute(command)
    with Capture(engine.store) as captured:
        assert engine.execute([b'EXEC']).reply == OVERFLOW
    assert not captured.group().entries
    assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
    assert engine.expire_batch() == 1
    assert engine.execute([b'GET', b'expiring']).reply == Bulk(None)
