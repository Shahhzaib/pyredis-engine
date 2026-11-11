from __future__ import annotations

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.collections import PendingEntry, StreamGroup, StreamValue
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple


def test_group_delivery_pending_ack_and_deleted_entry() -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        assert call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        reply = call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">")
        assert reply == Array(
            (Array((Bulk(b"s"), Array((Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)))),)
        )
        assert call(b"XPENDING", b"s", b"g") == Array(
            (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"c"), Bulk(b"1"))),)))
        )
        assert call(b"XDEL", b"s", b"1-0") == Integer(1)
        assert call(b"XACK", b"s", b"g", b"1-0") == Integer(1)
        assert call(b"XACK", b"s", b"g", b"1-0") == Integer(0)
    finally:
        engine.close()


def test_consumer_administration_and_extended_pending() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM") == Simple(b"OK")
        assert call(b"XGROUP", b"CREATECONSUMER", b"s", b"g", b"c") == Integer(1)
        assert call(b"XGROUP", b"CREATECONSUMER", b"s", b"g", b"c") == Integer(0)
        assert call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert call(b"XADD", b"s", b"2-0", b"f", b"v") == Bulk(b"2-0")
        assert isinstance(call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"), Array)
        clock.advance(25)
        assert call(b"XPENDING", b"s", b"g", b"IDLE", b"25", b"(1-0", b"+", b"10", b"c") == Array(
            (Array((Bulk(b"2-0"), Bulk(b"c"), Integer(25), Integer(1))),)
        )
        assert call(b"XGROUP", b"DELCONSUMER", b"s", b"g", b"c") == Integer(2)
        assert call(b"XPENDING", b"s", b"g") == Array(
            (Integer(0), Bulk(None), Bulk(None), Array(None))
        )
    finally:
        engine.close()


def test_claim_idle_ownership_counts_and_deleted_entries() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        assert call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert call(b"XADD", b"s", b"2-0", b"f", b"w") == Bulk(b"2-0")
        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array)
        clock.advance(20)
        assert call(b"XCLAIM", b"s", b"g", b"b", b"21", b"1-0") == Array(())
        assert call(b"XCLAIM", b"s", b"g", b"b", b"20", b"1-0", b"JUSTID") == Array((Bulk(b"1-0"),))
        assert call(b"XPENDING", b"s", b"g", b"-", b"+", b"10", b"b") == Array(
            (Array((Bulk(b"1-0"), Bulk(b"b"), Integer(0), Integer(1))),)
        )
        assert call(b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0") == Array(
            (Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)
        )
        assert call(b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"1-0") == Array(
            (
                Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),
                Array((Bulk(b"1-0"), Array((Bulk(b"f"), Bulk(b"v"))))),
            )
        )
        assert call(b"XDEL", b"s", b"2-0") == Integer(1)
        assert call(b"XCLAIM", b"s", b"g", b"b", b"0", b"2-0") == Array(())
        assert call(b"XPENDING", b"s", b"g", b"-", b"+", b"10") == Array(
            (Array((Bulk(b"1-0"), Bulk(b"b"), Integer(0), Integer(4))),)
        )
    finally:
        engine.close()


def test_claim_oom_and_reply_limit_leave_pending_ownership_unchanged() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=256), clock=ManualClock(1000))
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        assert call(b"XADD", b"s", b"1-0", b"f", b"v" * 100) == Bulk(b"1-0")
        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array)
        before = call(b"XPENDING", b"s", b"g")
        reply = call(b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"1-0", b"1-0")
        assert isinstance(reply, Error) and b"reply exceeds" in reply.payload
        assert call(b"XPENDING", b"s", b"g") == before
        engine.store.maxmemory_bytes = engine.store.memory_bytes
        reply = call(b"XCLAIM", b"s", b"g", b"b", b"0", b"1-0", b"JUSTID")
        assert isinstance(reply, Error) and reply.payload.startswith(b"OOM")
        assert call(b"XPENDING", b"s", b"g") == before
    finally:
        engine.close()


def test_autoclaim_cursor_deleted_entries_and_justid_counts() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        for identity in (b"1-0", b"2-0", b"3-0"):
            assert call(b"XADD", b"s", identity, b"f", b"v") == Bulk(identity)
        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array)
        assert call(b"XDEL", b"s", b"1-0") == Integer(1)
        clock.advance(20)
        assert call(
            b"XAUTOCLAIM", b"s", b"g", b"b", b"0", b"0-0", b"COUNT", b"1", b"JUSTID"
        ) == Array((Bulk(b"2-0"), Array(()), Array((Bulk(b"1-0"),))))
        assert call(
            b"XAUTOCLAIM", b"s", b"g", b"b", b"20", b"2-0", b"COUNT", b"1", b"JUSTID"
        ) == Array((Bulk(b"3-0"), Array((Bulk(b"2-0"),)), Array(())))
        assert call(b"XPENDING", b"s", b"g", b"2-0", b"2-0", b"1") == Array(
            (Array((Bulk(b"2-0"), Bulk(b"b"), Integer(0), Integer(1))),)
        )
    finally:
        engine.close()


def test_autoclaim_skip_budget_and_reply_rejection_are_atomic() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=256), clock=ManualClock(1000))
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        for number in range(1, 12):
            identity = f"{number}-0".encode()
            assert call(b"XADD", b"s", identity, b"f", b"v" * 100) == Bulk(identity)
        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        for _ in range(11):
            assert isinstance(
                call(b"XREADGROUP", b"GROUP", b"g", b"a", b"COUNT", b"1", b"STREAMS", b"s", b">"),
                Array,
            )
        assert call(b"XAUTOCLAIM", b"s", b"g", b"b", b"1", b"0-0", b"COUNT", b"1") == Array(
            (Bulk(b"11-0"), Array(()), Array(()))
        )
        before = call(b"XPENDING", b"s", b"g")
        result = call(b"XAUTOCLAIM", b"s", b"g", b"b", b"0", b"0-0", b"COUNT", b"3")
        assert isinstance(result, Error) and b"reply exceeds" in result.payload
        assert call(b"XPENDING", b"s", b"g") == before
    finally:
        engine.close()


def test_extended_pending_rejects_excessive_scan_without_state_change() -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    try:
        pending = tuple(PendingEntry((number, 0), b"a", 1000, 1) for number in range(1, 4100))
        value = StreamValue(
            last_id=(4099, 0), groups=(StreamGroup(b"g", (0, 0), (b"a",), pending),)
        )
        engine.store.put(b"s", value)
        before = engine.store.mutation_revision
        result = engine.execute([b"XPENDING", b"s", b"g", b"-", b"+", b"1", b"other"]).reply
        assert isinstance(result, Error) and b"work limit" in result.payload
        assert engine.store.mutation_revision == before
    finally:
        engine.close()


def test_autoclaim_exec_reply_overflow_rolls_back_owner() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128), clock=ManualClock(1000))
    try:

        def call(*args: bytes):
            return engine.execute(list(args)).reply

        assert call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
        assert call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
        assert isinstance(call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"), Array)
        before = call(b"XPENDING", b"s", b"g")
        assert call(b"MULTI") == Simple(b"OK")
        assert call(b"XAUTOCLAIM", b"s", b"g", b"b", b"0", b"0-0", b"JUSTID") == Simple(b"QUEUED")
        assert call(b"ECHO", b"x" * 120) == Simple(b"QUEUED")
        result = call(b"EXEC")
        assert isinstance(result, Error) and result.payload.startswith(b"EXECABORT")
        assert call(b"XPENDING", b"s", b"g") == before
    finally:
        engine.close()


def test_group_metadata_limit_rejects_without_modifying_stream() -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=420))
    try:
        assert engine.execute([b"XADD", b"s", b"1-0", b"f", b"v"]).reply == Bulk(b"1-0")
        result = engine.execute([b"XGROUP", b"CREATE", b"s", b"g", b"0"]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"OOM")
        assert engine.execute([b"XLEN", b"s"]).reply == Integer(1)
        result = engine.execute([b"XPENDING", b"s", b"g"]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"NOGROUP")
    finally:
        engine.close()


def test_group_read_in_multi_is_queued_and_nonblocking() -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute(
            [b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM"]
        ).reply == Simple(b"OK")
        assert engine.execute([b"MULTI"]).reply == Simple(b"OK")
        result = engine.execute(
            [b"XREADGROUP", b"GROUP", b"g", b"c", b"BLOCK", b"0", b"STREAMS", b"s", b">"]
        ).reply
        assert result == Simple(b"QUEUED")
        result = engine.execute([b"EXEC"]).reply
        assert result == Array((Array(None),))
    finally:
        engine.close()


def test_group_transactions_preserve_pending_on_exec_reply_overflow() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    try:
        assert engine.execute([b"XADD", b"s", b"1-0", b"f", b"v"]).reply == Bulk(b"1-0")
        assert engine.execute([b"MULTI"]).reply == Simple(b"OK")
        assert engine.execute([b"XGROUP", b"CREATE", b"s", b"g", b"0"]).reply == Simple(b"QUEUED")
        assert engine.execute([b"EXEC"]).reply == Array((Simple(b"OK"),))
        assert isinstance(
            engine.execute([b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"]).reply,
            Array,
        )
        assert engine.execute([b"MULTI"]).reply == Simple(b"OK")
        assert engine.execute([b"XACK", b"s", b"g", b"1-0"]).reply == Simple(b"QUEUED")
        assert engine.execute([b"ECHO", b"x" * 120]).reply == Simple(b"QUEUED")
        result = engine.execute([b"EXEC"]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"EXECABORT")
        pending = engine.execute([b"XPENDING", b"s", b"g"]).reply
        assert isinstance(pending, Array) and pending.payload is not None
        assert pending.payload[0] == Integer(1)
    finally:
        engine.close()
