from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple


def test_budget_rejection_preserves_value_and_revision_and_delete_releases_capacity() -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=200))
    try:
        assert engine.execute([b'SET', b'k', b'1234567']).reply == Simple(b'OK')
        assert engine.store.memory_bytes == 200
        revision = engine.store.mutation_revision
        assert isinstance(engine.execute([b'APPEND', b'k', b'x']).reply, Error)
        assert engine.execute([b'GET', b'k']).reply == Bulk(b'1234567')
        assert engine.store.mutation_revision == revision
        engine.execute([b'DEL', b'k'])
        assert engine.store.memory_bytes == 0
        assert engine.execute([b'SET', b'new', b'']).reply == Simple(b'OK')
    finally:
        engine.close()


def test_mset_whole_batch_preflight_and_replacement_credit() -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=400))
    try:
        assert engine.execute([b'MSET', b'a', b'1234567', b'b', b'1234567']).reply == Simple(b'OK')
        before = engine.store.mutation_revision
        assert isinstance(engine.execute([b'MSET', b'a', b'changed', b'c', b'']).reply, Error)
        assert engine.store.mutation_revision == before
        assert engine.execute([b'MGET', b'a', b'b', b'c']).reply == Array(
            (Bulk(b'1234567'), Bulk(b'1234567'), Bulk(None)))
        assert engine.execute([b'MSET', b'a', b'x' * 14, b'b', b'']).reply == Simple(b'OK')
        assert engine.store.memory_bytes == 400
        engine.execute([b'MSET', b'a', b'transient' * 20, b'a', b''])
        assert engine.store.memory_bytes == 386
    finally:
        engine.close()


def test_expiry_and_clear_release_charges_and_exec_runtime_oom_does_not_rollback() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(maxmemory_bytes=200), clock=clock)
    try:
        engine.execute([b'SET', b'k', b'1234567', b'PX', b'1'])
        assert engine.store.memory_bytes == 200
        clock.advance(2)
        engine.expire_batch()
        assert engine.store.memory_bytes == 0
        for command in ([b'MULTI'], [b'SET', b'a', b''], [b'SET', b'b', b''], [b'DEL', b'a']):
            engine.execute(command)
        result = engine.execute([b'EXEC']).reply
        assert isinstance(result, Array) and result.payload is not None
        assert result.payload[0] == Simple(b'OK')
        assert isinstance(result.payload[1], Error)
        assert result.payload[2] == Integer(1)
        assert engine.store.memory_bytes == 0
        engine.execute([b'SET', b'k', b''])
        engine.execute([b'FLUSHDB'])
        assert engine.store.memory_bytes == 0 and not engine.store._memory_sizes
    finally:
        engine.close()


@pytest.mark.parametrize('module_name,constructor,command,args', [
    ('hashes', 'HashValue', b'HSET', [b'a', b'b']),
    ('lists', 'ListValue', b'RPUSH', [b'a']),
    ('sets', 'SetValue', b'SADD', [b'a']),
    ('sorted_sets', 'SortedSetValue', b'ZADD', [b'1', b'a']),
])
def test_private_collection_allocation_failure_leaves_original_unchanged(
    monkeypatch: pytest.MonkeyPatch, module_name: str, constructor: str,
    command: bytes, args: list[bytes],
) -> None:
    from importlib import import_module

    engine = Executor(ServerConfig())
    try:
        engine.execute([command, b'k', *args])
        value, deadline = engine.store.physical_entry(b'k')
        before = engine.store.memory_bytes, engine.store.mutation_revision

        def fail(*args, **kwargs):
            raise MemoryError('injected private allocation')

        module = import_module('pyredis_engine.commands.' + module_name)
        monkeypatch.setattr(module, constructor, fail)
        more = {b'HSET': [b'c', b'd'], b'ZADD': [b'2', b'c']}.get(command, [b'c'])
        assert isinstance(engine.execute([command, b'k', *more]).reply, Error)
        assert engine.store.physical_entry(b'k') == (value, deadline)
        assert before == (engine.store.memory_bytes, engine.store.mutation_revision)
        assert engine.store.allocation_rejections == 1
    finally:
        engine.close()


def test_restored_group_and_lower_budget_fail_before_partial_replacement() -> None:
    from pyredis_engine.engine.store import StringValue
    from pyredis_engine.persistence.codec import Entry, Group
    from pyredis_engine.persistence.errors import CapacityError

    engine = Executor(ServerConfig(maxmemory_bytes=400))
    try:
        engine.execute([b'MSET', b'a', b'1234567', b'b', b'1234567'])
        before = engine.store.mutation_revision
        with pytest.raises(CapacityError):
            engine.restore_group(Group(True, (Entry(b'too-large', StringValue(b'x' * 300), None),)))
        assert engine.store.mutation_revision == before
        assert engine.store.memory_bytes == 400
        engine.restore_group(Group(False, (Entry(b'a', StringValue(b'x' * 14), None),
                                          Entry(b'b', StringValue(b''), None))))
        assert engine.store.memory_bytes == 400
        assert engine.execute([b'GET', b'a']).reply == Bulk(b'x' * 14)
    finally:
        engine.close()


@pytest.mark.parametrize('name,args', [
    (b'HSET', [b'f', b'v']), (b'RPUSH', [b'v']), (b'SADD', [b'v']),
    (b'ZADD', [b'1', b'v']),
])
def test_collection_budget_preflight_is_atomic(name: bytes, args: list[bytes]) -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=330))
    try:
        engine.execute([name, b'k', *args])
        before = engine.store.memory_bytes, engine.store.mutation_revision
        value, deadline = engine.store.physical_entry(b'k')
        result = engine.execute([name, b'k', *([b'long' * 100, b'v'] if name == b'HSET'
            else [b'1', b'long' * 100] if name == b'ZADD' else [b'long' * 100])])
        assert isinstance(result.reply, Error) and result.reply.payload.startswith(b'OOM ')
        assert before == (engine.store.memory_bytes, engine.store.mutation_revision)
        assert engine.store.physical_entry(b'k') == (value, deadline)
    finally:
        engine.close()
