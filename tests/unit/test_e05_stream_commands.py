from __future__ import annotations

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.ops.memory import OOM
from pyredis_engine.protocol import Array, Bulk, Error, Integer


def test_stream_ids_ranges_deletion_and_empty_metadata() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        assert call(b"XADD", b"s", b"*", b"\x00", b"\xff", b"\x00", b"v") == Bulk(b"1000-0")
        assert call(b"XADD", b"s", b"*", b"f", b"v") == Bulk(b"1000-1")
        clock.advance(-1)
        assert call(b"XADD", b"s", b"*", b"f", b"v") == Bulk(b"1000-2")
        assert isinstance(call(b"XADD", b"s", b"1000-1", b"f", b"v"), Error)
        assert call(b"XLEN", b"s") == Integer(3)
        assert call(b"XREAD", b"STREAMS", b"s", b"$") == Array(None)
        first = Array(
            (Bulk(b"1000-0"), Array((Bulk(b"\x00"), Bulk(b"\xff"), Bulk(b"\x00"), Bulk(b"v"))))
        )
        assert call(b"XRANGE", b"s", b"-", b"+", b"COUNT", b"1") == Array((first,))
        assert call(b"XDEL", b"s", b"1000-0", b"1000-1", b"1000-2") == Integer(3)
        assert call(b"TYPE", b"s").payload == b"stream"
        assert call(b"XADD", b"s", b"*", b"f", b"v") == Bulk(b"1000-3")
        assert call(b"XTRIM", b"s", b"MAXLEN", b"=", b"0") == Integer(1)
        assert call(b"XLEN", b"s") == Integer(0)
    finally:
        engine.close()


def test_xadd_allocation_failure_and_exec_overflow_restore_stream(monkeypatch) -> None:
    from pyredis_engine.commands import streams

    engine = Executor(ServerConfig(max_reply_bytes=128), clock=ManualClock(1000))
    try:
        assert engine.execute([b"XADD", b"s", b"1-0", b"f", b"v"]).reply == Bulk(b"1-0")
        revision = engine.store.mutation_revision
        with monkeypatch.context() as patch:

            def fail(*args, **kwargs):
                raise MemoryError

            patch.setattr(streams, "StreamEntry", fail)
            assert engine.execute([b"XADD", b"s", b"2-0", b"f", b"v"]).reply == Error(OOM)
        assert engine.store.mutation_revision == revision
        assert engine.execute([b"MULTI"]).reply.payload == b"OK"
        engine.execute([b"XADD", b"s", b"2-0", b"f", b"v"])
        engine.execute([b"ECHO", b"x" * 120])
        assert engine.execute([b"EXEC"]).reply.payload.startswith(b"EXECABORT")
        assert engine.execute([b"XLEN", b"s"]).reply == Integer(1)
        assert engine.execute([b"XADD", b"s", b"2-0", b"f", b"v"]).reply == Bulk(b"2-0")
    finally:
        engine.close()


def test_stream_accounting_rejects_growth_without_changing_existing_entry() -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=420))
    try:
        assert engine.execute([b"XADD", b"s", b"1-0", b"f", b"v"]).reply == Bulk(b"1-0")
        assert engine.execute([b"XADD", b"s", b"2-0", b"f", b"v"]).reply == Error(OOM)
        assert engine.execute([b"XLEN", b"s"]).reply == Integer(1)
        assert engine.execute([b"XRANGE", b"s", b"-", b"+"]).reply == Array(
            (Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)
        )
    finally:
        engine.close()


def test_stream_id_uint64_overflow_is_rejected_without_mutation() -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1))
    try:
        maximum = b"18446744073709551615-18446744073709551615"
        assert engine.execute([b"XADD", b"s", maximum, b"f", b"v"]).reply == Bulk(maximum)
        revision = engine.store.mutation_revision
        assert isinstance(engine.execute([b"XADD", b"s", b"*", b"f", b"v"]).reply, Error)
        assert engine.store.mutation_revision == revision
        assert engine.execute([b"XLEN", b"s"]).reply == Integer(1)
    finally:
        engine.close()


def test_stream_expiration_and_watch_invalidation() -> None:
    from pyredis_engine.protocol import Simple

    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    watcher = engine.new_client()
    try:
        assert engine.execute([b"XADD", b"s", b"1-0", b"f", b"v"]).reply == Bulk(b"1-0")
        assert engine.execute([b"PEXPIRE", b"s", b"50"]).reply == Integer(1)
        assert engine.execute([b"WATCH", b"s"], watcher).reply == Simple(b"OK")
        assert engine.execute([b"MULTI"], watcher).reply == Simple(b"OK")
        assert engine.execute([b"XADD", b"s", b"2-0", b"f", b"v"]).reply == Bulk(b"2-0")
        assert engine.execute([b"PTTL", b"s"]).reply == Integer(50)
        assert engine.execute([b"EXEC"], watcher).reply == Array(None)
        clock.advance(51)
        assert engine.execute([b"XLEN", b"s"]).reply == Integer(0)
        assert engine.execute([b"TYPE", b"s"]).reply == Simple(b"none")
    finally:
        engine.close()
