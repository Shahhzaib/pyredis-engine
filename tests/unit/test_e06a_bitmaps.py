from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple


def test_bitmap_offsets_ranges_and_bitop() -> None:
    engine = Executor(ServerConfig())
    try:
        def run(*args: bytes):
            return engine.execute(list(args)).reply

        assert run(b'SETBIT', b'k', b'0', b'1') == Integer(0)
        assert run(b'SETBIT', b'k', b'15', b'1') == Integer(0)
        assert run(b'GET', b'k') == Bulk(b'\x80\x01')
        assert run(b'GETBIT', b'k', b'0') == Integer(1)
        assert run(b'GETBIT', b'k', b'16') == Integer(0)
        assert run(b'BITCOUNT', b'k') == Integer(2)
        assert run(b'BITCOUNT', b'k', b'1', b'7', b'BIT') == Integer(0)
        assert run(b'BITPOS', b'k', b'1', b'1') == Integer(15)
        assert run(b'BITPOS', b'k', b'0') == Integer(1)
        assert run(b'BITOP', b'NOT', b'k', b'k') == Integer(2)
        assert run(b'GET', b'k') == Bulk(b'\x7f\xfe')
    finally:
        engine.close()


def test_bitfield_atomic_sequential_overflow() -> None:
    engine = Executor(ServerConfig())
    try:
        def run(*args: bytes):
            return engine.execute(list(args)).reply

        assert run(b'BITFIELD', b'b', b'SET', b'i8', b'0', b'127',
                   b'OVERFLOW', b'FAIL', b'INCRBY', b'i8', b'0', b'1',
                   b'GET', b'i8', b'0') == Array((Integer(0), Bulk(None), Integer(127)))
        assert run(b'BITFIELD', b'b', b'OVERFLOW', b'WRAP',
                   b'INCRBY', b'i8', b'0', b'1') == Array((Integer(-128),))
        assert run(b'BITFIELD_RO', b'b', b'GET', b'i8', b'#0') == Array((Integer(-128),))
        assert isinstance(run(b'BITFIELD_RO', b'b', b'SET', b'i8', b'0', b'2'), Error)
        assert run(b'GET', b'b') == Bulk(b'\x80')
    finally:
        engine.close()


def test_bitmap_limits_and_wrongtype_preserve_state() -> None:
    engine = Executor(ServerConfig(max_bulk_bytes=64))
    try:
        def run(*args: bytes):
            return engine.execute(list(args)).reply

        assert run(b'SET', b'k', b'old') == Simple(b'OK')
        assert isinstance(run(b'SETBIT', b'k', b'512', b'1'), Error)
        assert run(b'GET', b'k') == Bulk(b'old')
        assert run(b'HSET', b'h', b'f', b'v') == Integer(1)
        assert isinstance(run(b'BITOP', b'OR', b'k', b'h'), Error)
        assert run(b'GET', b'k') == Bulk(b'old')
    finally:
        engine.close()


def test_bitfield_rejects_later_growth_without_publishing_earlier_write() -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=200))
    try:
        engine.execute([b'SET', b'k', b'1234567'])
        before = engine.store.memory_bytes, engine.store.mutation_revision
        result = engine.execute([b'BITFIELD', b'k', b'SET', b'u8', b'0', b'3',
                                 b'SET', b'u8', b'64', b'4']).reply
        assert isinstance(result, Error)
        assert engine.execute([b'GET', b'k']).reply == Bulk(b'1234567')
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
    finally:
        engine.close()


@pytest.mark.parametrize('args', [
    [b'SETBIT', b'k', b'2', b'1'],
    [b'BITFIELD', b'k', b'SET', b'u8', b'0', b'4'],
    [b'BITOP', b'NOT', b'k', b'k'],
])
def test_private_allocation_failure_preserves_value(
    args: list[bytes], monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyredis_engine.commands import bitmaps

    engine = Executor(ServerConfig())
    try:
        engine.execute([b'SET', b'k', b'old', b'PX', b'60000'])
        before = (engine.store.physical_entry(b'k'), engine.store.memory_bytes,
                  engine.store.mutation_revision)

        def fail(*args: object, **kwargs: object) -> None:
            raise MemoryError('injected allocation failure')

        monkeypatch.setattr(bitmaps, 'StringValue', fail)
        assert isinstance(engine.execute(args).reply, Error)
        assert (engine.store.physical_entry(b'k'), engine.store.memory_bytes,
                engine.store.mutation_revision) == before
    finally:
        engine.close()


@pytest.mark.parametrize('version', [2, 3])
def test_exec_reply_overflow_rolls_back_bitmap_writes(version: int) -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    client = engine.new_client()
    client.protocol_version = version
    try:
        engine.execute([b'MULTI'], client)
        engine.execute([b'SETBIT', b'k', b'0', b'1'], client)
        reads = [part for _ in range(29) for part in (b'GET', b'u1', b'0')]
        engine.execute([b'BITFIELD_RO', b'k', *reads], client)
        result = engine.execute([b'EXEC'], client).reply
        assert isinstance(result, Error) and result.payload.startswith(b'EXECABORT')
        assert engine.execute([b'GET', b'k'], client).reply == Bulk(None)
    finally:
        engine.close()


def test_watch_detects_bitmap_mutation() -> None:
    engine = Executor(ServerConfig())
    watcher, writer = engine.new_client(), engine.new_client()
    try:
        assert engine.execute([b'WATCH', b'k'], watcher).reply == Simple(b'OK')
        engine.execute([b'SETBIT', b'k', b'0', b'1'], writer)
        engine.execute([b'MULTI'], watcher)
        engine.execute([b'GETBIT', b'k', b'0'], watcher)
        assert engine.execute([b'EXEC'], watcher).reply == Array(None)
    finally:
        engine.close()


def test_bitop_work_limit_rejects_without_changing_destination() -> None:
    engine = Executor(ServerConfig(max_collection_bytes=32))
    try:
        assert engine.execute([b'SET', b's', b'x' * 17]).reply == Simple(b'OK')
        assert engine.execute([b'SET', b'd', b'old']).reply == Simple(b'OK')
        revision = engine.store.mutation_revision
        assert isinstance(engine.execute([b'BITOP', b'OR', b'd', b's', b's']).reply, Error)
        assert engine.execute([b'GET', b'd']).reply == Bulk(b'old')
        assert engine.store.mutation_revision == revision
    finally:
        engine.close()
