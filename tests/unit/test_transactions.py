from __future__ import annotations

import pytest
from _support.transaction_cases import ABORT, OK, QUEUED, TRACES, WATCH_CASES

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer


@pytest.mark.parametrize("name", TRACES)
def test_transaction_contract(name: str) -> None:
    executor = Executor(ServerConfig(), clock=ManualClock(1000))
    for command, expected in TRACES[name]:
        assert executor.execute(list(command)).reply == expected, command


@pytest.mark.parametrize("setup,mutation,conflict", WATCH_CASES)
def test_watch_observable_mutations(
    setup: tuple[bytes, ...], mutation: tuple[bytes, ...], conflict: bool,
) -> None:
    executor = Executor(ServerConfig())
    executor.execute(list(setup))
    executor.execute([b"WATCH", b"k"])
    executor.execute(list(mutation))
    executor.execute([b"MULTI"])
    assert executor.execute([b"EXEC"]).reply == Array(None if conflict else ())


def test_client_isolation_and_cleanup() -> None:
    engine = Executor(ServerConfig())
    first, second = engine.new_client(), engine.new_client()
    assert engine.execute([b"WATCH", b"k"], first).reply == OK
    assert engine.execute([b"MULTI"], first).reply == OK
    assert engine.execute([b"SET", b"k", b"uncommitted"], first).reply == QUEUED
    assert engine.execute([b"GET", b"k"], second).reply == Bulk(None)
    assert engine.execute([b"SET", b"k", b"other"], second).reply == OK
    assert engine.execute([b"EXEC"], first).reply == Array(None)
    assert engine.watched_key_count == 0
    engine.execute([b"WATCH", b"missing"], first)
    engine.execute([b"MULTI"], first)
    engine.execute([b"SET", b"new", b"value"], first)
    engine.close_client(first)
    engine.close_client(first)
    assert engine.watched_key_count == 0
    assert first.queue == [] and first.watched == set()
    assert engine.execute([b"GET", b"new"], second).reply == Bulk(None)


@pytest.mark.parametrize("active", [False, True])
def test_watched_expiry_and_expired_registration(active: bool) -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    engine.execute([b"SET", b"k", b"v", b"PX", b"1"])
    engine.execute([b"WATCH", b"k"])
    engine.execute([b"MULTI"])
    engine.execute([b"SET", b"other", b"v"])
    clock.advance(2)
    if active:
        assert engine.expire_batch() == 1
    assert engine.execute([b"EXEC"]).reply == Array(None)
    assert engine.execute([b"GET", b"other"]).reply == Bulk(None)
    assert engine.watched_key_count == 0
    engine.execute([b"WATCH", b"k"])
    engine.execute([b"MULTI"])
    assert engine.execute([b"EXEC"]).reply == Array(())


@pytest.mark.parametrize("config", [
    ServerConfig(max_transaction_commands=1), ServerConfig(max_transaction_bytes=10),
])
def test_queue_limits_abort_without_effects(config: ServerConfig) -> None:
    engine = Executor(config)
    engine.execute([b"MULTI"])
    results = [engine.execute([b"SET", b"key", b"value"]).reply for _ in range(3)]
    assert any(isinstance(reply, Error) for reply in results)
    assert engine.execute([b"EXEC"]).reply == ABORT
    assert engine.store.physical_count == 0


def test_watch_limits_are_atomic_and_released() -> None:
    engine = Executor(ServerConfig(max_watched_keys=2, max_watch_bytes=3))
    assert engine.execute([b"WATCH", b"a", b"a"]).reply == OK
    assert isinstance(engine.execute([b"WATCH", b"b", b"cc"]).reply, Error)
    assert engine.watched_key_count == 1
    assert engine.execute([b"UNWATCH"]).reply == OK
    assert engine.watched_key_count == 0
    assert engine.execute([b"WATCH", b"cc"]).reply == OK


def test_aggregate_reply_overflow_rejects_all_effects_and_clears_state() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    engine.execute([b"SET", b"k", b"v" * 80])
    engine.execute([b"MULTI"])
    for command in ([b"GET", b"k"], [b"GET", b"k"], [b"INCR", b"count"]):
        assert engine.execute(command).reply == QUEUED
    assert engine.execute([b"EXEC"]).reply == Error(
        b"EXECABORT Transaction discarded because reply exceeds configured maximum size")
    assert engine.execute([b"GET", b"count"]).reply == Bulk(None)
    assert engine.execute([b"EXEC"]).reply == Error(b"ERR EXEC without MULTI")
    assert engine.execute([b"DEL", b"k"]).reply == Integer(1)


def test_default_queue_boundary_and_reset() -> None:
    engine = Executor(ServerConfig())
    engine.execute([b"MULTI"])
    for _ in range(1024):
        assert engine.execute([b"INCR", b"k"]).reply == QUEUED
    assert isinstance(engine.execute([b"INCR", b"k"]).reply, Error)
    assert engine.execute([b"EXEC"]).reply == ABORT
    assert engine.store.physical_count == 0
    engine.execute([b"MULTI"])
    assert engine.execute([b"INCR", b"k"]).reply == QUEUED
    assert engine.execute([b"EXEC"]).reply == Array((Integer(1),))


def test_unknown_queue_error_clears_watches_and_does_not_execute() -> None:
    engine = Executor(ServerConfig())
    engine.execute([b"WATCH", b"k"])
    engine.execute([b"MULTI"])
    assert engine.execute([b"SET", b"k", b"v"]).reply == QUEUED
    assert engine.execute([b"UNSUPPORTED"]).reply == Error(b"ERR unknown command")
    assert engine.execute([b"EXEC"]).reply == ABORT
    assert engine.watched_key_count == engine.store.physical_count == 0


def test_watch_exact_expiry_boundary_and_same_deadline_update() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    engine.execute([b"SET", b"k", b"v", b"PX", b"1"])
    engine.execute([b"WATCH", b"k"])
    clock.advance(1)
    engine.execute([b"MULTI"])
    assert engine.execute([b"EXEC"]).reply == Array(())
    engine.execute([b"WATCH", b"k"])
    engine.execute([b"PEXPIREAT", b"k", b"2000"])
    engine.execute([b"UNWATCH"])
    engine.execute([b"WATCH", b"k"])
    engine.execute([b"PEXPIREAT", b"k", b"2000"])
    engine.execute([b"MULTI"])
    assert engine.execute([b"EXEC"]).reply == Array(None)


@pytest.mark.parametrize("name", ["max_transaction_commands", "max_transaction_bytes",
                                 "max_watched_keys", "max_watch_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_transaction_limits_reject_invalid_configuration(name: str, value: object) -> None:
    with pytest.raises(ValueError, match="transaction/watch"):
        ServerConfig(**{name: value})  # type: ignore[arg-type]


def test_quit_discards_queue_and_cross_executor_state_is_rejected() -> None:
    engine, other = Executor(ServerConfig()), Executor(ServerConfig())
    client = engine.new_client()
    engine.execute([b"WATCH", b"k"], client)
    engine.execute([b"MULTI"], client)
    engine.execute([b"SET", b"k", b"v"], client)
    assert engine.execute([b"QUIT"], client).close
    assert engine.watched_key_count == engine.store.physical_count == 0
    with pytest.raises(RuntimeError, match="another executor"):
        other.execute([b"PING"], client)
    engine.close_client(client)
    with pytest.raises(RuntimeError, match="closed"):
        engine.execute([b"PING"], client)
