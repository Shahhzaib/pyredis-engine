from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.ops.memory import AllocationRejected
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple


def test_multi_stream_delivery_and_missing_group_are_atomic() -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    try:
        for key in (b"a", b"b"):
            assert engine.execute([b"XADD", key, b"1-0", b"f", key]).reply == Bulk(b"1-0")
            assert engine.execute([b"XGROUP", b"CREATE", key, b"g", b"0"]).reply == Simple(b"OK")
        before = engine.store.mutation_revision
        result = engine.execute(
            [b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"a", b"missing", b">", b">"]
        ).reply
        assert isinstance(result, Error) and result.payload.startswith(b"NOGROUP")
        assert engine.store.mutation_revision == before
        assert engine.execute([b"XPENDING", b"a", b"g"]).reply.payload[0] == Integer(0)
        result = engine.execute(
            [
                b"XREADGROUP",
                b"GROUP",
                b"g",
                b"c",
                b"COUNT",
                b"1",
                b"STREAMS",
                b"a",
                b"b",
                b">",
                b">",
            ]
        ).reply
        assert result == Array(
            (
                Array(
                    (Bulk(b"a"), Array((Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"a"))))),)))
                ),
                Array(
                    (Bulk(b"b"), Array((Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"b"))))),)))
                ),
            )
        )
        assert engine.execute([b"XPENDING", b"a", b"g"]).reply.payload[0] == Integer(1)
        assert engine.execute([b"XPENDING", b"b", b"g"]).reply.payload[0] == Integer(1)
    finally:
        engine.close()


def test_transactional_group_read_is_nonblocking_and_rolls_back_overflow() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128), clock=ManualClock(1000))
    try:
        assert engine.execute([b"XADD", b"s", b"1-0", b"f", b"v"]).reply == Bulk(b"1-0")
        assert engine.execute([b"XGROUP", b"CREATE", b"s", b"g", b"0"]).reply == Simple(b"OK")
        assert engine.execute([b"MULTI"]).reply == Simple(b"OK")
        assert engine.execute(
            [b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"s", b">"]
        ).reply == Simple(b"QUEUED")
        assert engine.execute([b"ECHO", b"x" * 120]).reply == Simple(b"QUEUED")
        result = engine.execute([b"EXEC"]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"EXECABORT")
        assert engine.execute([b"XPENDING", b"s", b"g"]).reply.payload[0] == Integer(0)
    finally:
        engine.close()


def test_multi_stream_failed_second_write_rolls_back_first(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    try:
        for key in (b"a", b"b"):
            engine.execute([b"XADD", key, b"1-0", b"f", b"v"])
            engine.execute([b"XGROUP", b"CREATE", key, b"g", b"0"])
        original = engine.store.put

        def fail_second(key, value, **kwargs):
            if key == b"b":
                raise AllocationRejected
            return original(key, value, **kwargs)

        monkeypatch.setattr(engine.store, "put", fail_second)
        result = engine.execute(
            [b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"a", b"b", b">", b">"]
        ).reply
        assert isinstance(result, Error) and result.payload.startswith(b"OOM")
        assert engine.execute([b"XPENDING", b"a", b"g"]).reply.payload[0] == Integer(0)
        assert engine.execute([b"XPENDING", b"b", b"g"]).reply.payload[0] == Integer(0)
    finally:
        engine.close()


def test_multi_stream_memory_admission_rejects_all_group_updates() -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    try:
        for key in (b"a", b"b"):
            engine.execute([b"XADD", key, b"1-0", b"f", b"v"])
            engine.execute([b"XGROUP", b"CREATE", key, b"g", b"0"])
        engine.store.maxmemory_bytes = engine.store.memory_bytes + 300
        result = engine.execute(
            [b"XREADGROUP", b"GROUP", b"g", b"consumer", b"STREAMS", b"a", b"b", b">", b">"]
        ).reply
        assert isinstance(result, Error) and result.payload.startswith(b"OOM")
        for key in (b"a", b"b"):
            assert engine.execute([b"XPENDING", key, b"g"]).reply.payload[0] == Integer(0)
    finally:
        engine.close()


def test_unexpected_second_write_allocation_failure_restores_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    try:
        for key in (b"a", b"b"):
            engine.execute([b"XADD", key, b"1-0", b"f", b"v"])
            engine.execute([b"XGROUP", b"CREATE", key, b"g", b"0"])
        original = engine.store.put

        def fail_second(key, value, **kwargs):
            if key == b"b":
                raise MemoryError("injected failure")
            return original(key, value, **kwargs)

        monkeypatch.setattr(engine.store, "put", fail_second)
        with pytest.raises(MemoryError):
            engine.execute(
                [b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"a", b"b", b">", b">"]
            )
        assert engine.failed
        for key in (b"a", b"b"):
            value, _ = engine.store.physical_entry(key)
            assert value is not None and not value.groups[0].pending
    finally:
        engine.close()
