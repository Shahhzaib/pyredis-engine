from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import Store
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple


def run(engine: Executor, *args: bytes) -> Reply:
    return engine.execute(list(args)).reply


def setup(config: ServerConfig | None = None) -> tuple[ManualClock, Store, Executor]:
    clock = ManualClock(1_000_000)
    store = Store(clock=clock)
    return clock, store, Executor(config or ServerConfig(), store=store)


def test_millisecond_boundary_and_ttl_rounding() -> None:
    clock, store, engine = setup()
    assert run(engine, b"TTL", b"k") == Integer(-2)
    run(engine, b"SET", b"k", b"v")
    assert run(engine, b"PTTL", b"k") == Integer(-1)
    assert run(engine, b"PEXPIRE", b"k", b"1500") == Integer(1)
    assert store.deadline(b"k") == 1_001_500
    assert run(engine, b"TTL", b"k") == Integer(2)
    clock.advance(1)
    assert run(engine, b"TTL", b"k") == Integer(1)
    clock.advance(1498)
    assert run(engine, b"PTTL", b"k") == Integer(1)
    clock.advance(1)
    assert run(engine, b"PTTL", b"k") == Integer(0)
    clock.advance(1)
    assert run(engine, b"PTTL", b"k") == Integer(-2)
    assert store.physical_count == store.expiration_count == 0


@pytest.mark.parametrize("command", [
    (b"GET", b"k"), (b"MGET", b"k"), (b"EXISTS", b"k"), (b"TYPE", b"k"),
    (b"STRLEN", b"k"), (b"DEL", b"k"), (b"TTL", b"k"), (b"PERSIST", b"k"),
    (b"GETEX", b"k"), (b"DBSIZE",),
])
def test_expired_is_missing_for_every_read(command: tuple[bytes, ...]) -> None:
    clock, _, engine = setup()
    fresh = Executor(ServerConfig(), clock=clock)
    run(engine, b"SET", b"k", b"old", b"PX", b"1")
    clock.advance(2)
    assert run(engine, *command) == run(fresh, *command)


@pytest.mark.parametrize("command", [
    (b"INCR", b"k"), (b"DECR", b"k"), (b"INCRBY", b"k", b"4"),
    (b"APPEND", b"k", b"x"), (b"SET", b"k", b"new", b"NX"),
    (b"MSET", b"k", b"new"),
])
def test_expired_is_missing_for_writes(command: tuple[bytes, ...]) -> None:
    clock, _, engine = setup()
    fresh = Executor(ServerConfig(), clock=clock)
    run(engine, b"SET", b"k", b"not-number", b"PX", b"1")
    clock.advance(2)
    assert run(engine, *command) == run(fresh, *command)
    assert run(engine, b"GET", b"k") == run(fresh, b"GET", b"k")
    assert run(engine, b"PTTL", b"k") == Integer(-1)


def test_update_preserve_reset_and_persist() -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"1", b"EX", b"10")
    original = store.deadline(b"k")
    for command in [(b"APPEND", b"k", b"0"), (b"INCR", b"k"), (b"DECR", b"k"),
                    (b"INCRBY", b"k", b"2"), (b"SET", b"k", b"3", b"KEEPTTL")]:
        run(engine, *command)
        assert store.deadline(b"k") == original
    assert run(engine, b"PERSIST", b"k") == Integer(1)
    assert run(engine, b"PERSIST", b"k") == Integer(0)
    run(engine, b"PEXPIRE", b"k", b"4")
    run(engine, b"SET", b"k", b"new")
    assert run(engine, b"PTTL", b"k") == Integer(-1)
    run(engine, b"EXPIRE", b"k", b"10")
    run(engine, b"MSET", b"k", b"new")
    assert store.expiration_count == 0


def test_conditions_do_not_change_live_value_deadline_or_version() -> None:
    _, store, engine = setup()
    assert run(engine, b"SET", b"missing", b"v", b"XX") == Bulk(None)
    assert store.physical_count == 0
    run(engine, b"SET", b"k", b"old", b"PX", b"100")
    version, deadline = store.key_revision(b"k"), store.deadline(b"k")
    assert run(engine, b"SET", b"k", b"new", b"NX", b"EX", b"1") == Bulk(None)
    assert store.key_revision(b"k") == version
    assert store.deadline(b"k") == deadline
    assert run(engine, b"GET", b"k") == Bulk(b"old")


@pytest.mark.parametrize(("name", "value"), [
    (b"EXPIRE", b"0"), (b"PEXPIRE", b"-1"), (b"EXPIREAT", b"1"),
    (b"PEXPIREAT", b"1000000"),
])
def test_immediate_expiration(name: bytes, value: bytes) -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"v")
    assert run(engine, name, b"k", value) == Integer(1)
    assert run(engine, name, b"k", value) == Integer(0)
    assert store.physical_count == store.expiration_count == 0


@pytest.mark.parametrize("arguments", [
    (b"EX", b"0"), (b"PX", b"-1"), (b"PX", b"x"), (b"PX", b"01"),
    (b"EX", b"9223372036854775807"), (b"PX", b"9223372036854775807"),
    (b"NX", b"XX"), (b"KEEPTTL", b"EX", b"1"), (b"EX",), (b"GET",),
    (b"EX", b"1", b"PX", b"1"), (b"NX", b"NX", b"XX"),
])
def test_invalid_set_options_are_atomic(arguments: tuple[bytes, ...]) -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"old", b"PX", b"100")
    before = (store.key_revision(b"k"), store.deadline(b"k"))
    assert isinstance(run(engine, b"SET", b"k", b"new", *arguments), Error)
    assert (store.key_revision(b"k"), store.deadline(b"k")) == before
    assert run(engine, b"GET", b"k") == Bulk(b"old")


def test_getex_modes_and_oversize_reply_atomicity() -> None:
    _, store, engine = setup(ServerConfig(max_reply_bytes=128))
    run(engine, b"SET", b"k", b"v")
    for options, expected in [((b"EX", b"2"), 1_002_000), ((b"PX", b"3"), 1_000_003),
                              ((b"EXAT", b"1002"), 1_002_000),
                              ((b"PXAT", b"1000009"), 1_000_009)]:
        assert run(engine, b"GETEX", b"k", *options) == Bulk(b"v")
        assert store.deadline(b"k") == expected
    assert run(engine, b"GETEX", b"k", b"PERSIST") == Bulk(b"v")
    assert store.deadline(b"k") is None
    run(engine, b"SET", b"big", b"x" * 128, b"PX", b"10")
    before = (store.key_revision(b"big"), store.deadline(b"big"))
    assert run(engine, b"GETEX", b"big", b"PERSIST") == Error(
        b"ERR reply exceeds configured maximum size")
    assert (store.key_revision(b"big"), store.deadline(b"big")) == before
    assert run(engine, b"GETEX", b"k", b"PXAT", b"1") == Bulk(b"v")
    assert run(engine, b"GET", b"k") == Bulk(None)


def test_index_is_bounded_and_old_deadlines_do_not_remove_replacements() -> None:
    clock, store, engine = setup()
    run(engine, b"SET", b"k", b"v")
    for milliseconds in range(1, 2001):
        run(engine, b"PEXPIRE", b"k", str(milliseconds).encode())
        assert store.expiration_count == 1
    clock.advance(100)
    assert engine.expire_batch() == 0
    run(engine, b"SET", b"k", b"new")
    clock.advance(3000)
    assert engine.expire_batch() == 0
    assert run(engine, b"GET", b"k") == Bulk(b"new")


def test_many_keys_batch_budget_and_revision_cleanup() -> None:
    clock, store, engine = setup(ServerConfig(expiry_batch_size=7))
    for n in range(100):
        run(engine, b"SET", str(n).encode(), b"v", b"PX", b"1")
    clock.advance(2)
    assert run(engine, b"DBSIZE") == Integer(0)
    assert store.physical_count == 100
    assert engine.expire_batch() == 7
    assert store.physical_count == 93
    while engine.expire_batch():
        pass
    assert store.physical_count == store.expiration_count == store.revision_count == 0


def test_mutation_hook_and_clock_jumps_never_resurrect_expired_values() -> None:
    clock = ManualClock(1000)
    changes: list[tuple[bytes, int]] = []
    store = Store(clock=clock, on_change=lambda key, revision: changes.append((key, revision)))
    engine = Executor(ServerConfig(), store=store)
    run(engine, b"SET", b"k", b"1", b"PX", b"1")
    initial = store.key_revision(b"k")
    clock.advance(2)
    assert engine.expire_batch() == 1
    clock.advance(-100)
    assert run(engine, b"GET", b"k") == Bulk(None)
    run(engine, b"SET", b"k", b"2")
    assert store.key_revision(b"k") > initial
    run(engine, b"DEL", b"k")
    assert [key for key, _ in changes] == [b"k"] * 4
    assert [revision for _, revision in changes] == [1, 2, 3, 4]


def test_multi_key_commands_use_one_time_snapshot() -> None:
    class CountingClock:
        calls = 0

        def now_ms(self) -> int:
            self.calls += 1
            return self.calls

    clock = CountingClock()
    engine = Executor(ServerConfig(), clock=clock)
    run(engine, b"SET", b"a", b"v", b"PX", b"2")
    run(engine, b"SET", b"b", b"v", b"PX", b"2")
    before = clock.calls
    assert run(engine, b"MGET", b"a", b"b") == Array((Bulk(b"v"), Bulk(b"v")))
    assert clock.calls == before + 1


def test_repeated_options_of_same_kind_and_absolute_expiry() -> None:
    clock, store, engine = setup()
    assert run(engine, b"SET", b"k", b"v", b"NX", b"NX", b"EX", b"1", b"EX", b"2") == (
        Simple(b"OK"))
    assert store.deadline(b"k") == clock.now_ms() + 2000
    assert run(engine, b"EXPIREAT", b"k", b"1005") == Integer(1)
    assert store.deadline(b"k") == 1_005_000
    assert run(engine, b"PEXPIREAT", b"k", b"1000001") == Integer(1)
    clock.advance(2)
    assert run(engine, b"GET", b"k") == Bulk(None)


@pytest.mark.parametrize("name", [b"EXPIRE", b"PEXPIRE", b"EXPIREAT", b"PEXPIREAT",
                                  b"TTL", b"PTTL", b"PERSIST", b"GETEX"])
def test_expiration_command_arity(name: bytes) -> None:
    _, store, engine = setup()
    assert run(engine, name) == Error(
        b"ERR wrong number of arguments for '" + name.lower() + b"' command")
    assert store.physical_count == store.expiration_count == 0


@pytest.mark.parametrize("options", [
    (b"EX", b"0"), (b"PX", b"-1"), (b"EXAT", b"0"), (b"PXAT", b"-1"),
    (b"EX", b"1", b"PERSIST"), (b"KEEPTTL",), (b"PX",), (b"PERSIST", b"x"),
    (b"EX", b"9223372036854775807"), (b"PX", b"9223372036854775807"),
])
def test_invalid_getex_leaves_value_deadline_and_version(options: tuple[bytes, ...]) -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"v", b"EX", b"10")
    before = (store.key_revision(b"k"), store.deadline(b"k"))
    assert isinstance(run(engine, b"GETEX", b"k", *options), Error)
    assert (store.key_revision(b"k"), store.deadline(b"k")) == before
    assert run(engine, b"GET", b"k") == Bulk(b"v")


def test_delete_flush_and_failed_writes_leave_no_orphan_metadata() -> None:
    _, store, engine = setup(ServerConfig(max_bulk_bytes=4))
    run(engine, b"SET", b"a", b"old", b"PX", b"10")
    run(engine, b"SET", b"b", b"bad", b"PX", b"20")
    before = store.key_revision(b"b"), store.deadline(b"b")
    assert isinstance(run(engine, b"INCR", b"b"), Error)
    assert isinstance(run(engine, b"MSET", b"b", b"new", b"a", b"large"), Error)
    assert (store.key_revision(b"b"), store.deadline(b"b")) == before
    assert run(engine, b"DEL", b"a") == Integer(1)
    assert store.expiration_count == store.revision_count == 1
    assert run(engine, b"FLUSHDB") == Simple(b"OK")
    assert store.physical_count == store.expiration_count == store.revision_count == 0


def test_wrong_type_expiration_and_getex_rejection() -> None:
    class Other:
        kind = b"hash"

    _, store, engine = setup()
    other = Other()
    store.put(b"k", other)
    assert run(engine, b"EXPIRE", b"k", b"1") == Integer(1)
    before = store.key_revision(b"k"), store.deadline(b"k")
    assert run(engine, b"GETEX", b"k", b"PERSIST") == Error(
        b"WRONGTYPE Operation against a key holding the wrong kind of value")
    assert (store.key_revision(b"k"), store.deadline(b"k")) == before
    assert store.get(b"k") is other


@pytest.mark.parametrize("options", [
    {"expiry_interval": 0}, {"expiry_interval": float("nan")},
    {"expiry_interval": float("inf")}, {"expiry_batch_size": 0},
    {"expiry_batch_size": -1}, {"expiry_batch_size": float("nan")},
])
def test_invalid_sweeper_configuration(options: dict[str, int | float]) -> None:
    with pytest.raises(ValueError):
        ServerConfig(**options)  # type: ignore[arg-type]


@pytest.mark.parametrize(("remaining", "seconds"), [
    (0, 0), (1, 0), (499, 0), (500, 1), (999, 1), (1000, 1), (1499, 1), (1500, 2),
])
def test_ttl_subsecond_rounding(remaining: int, seconds: int) -> None:
    clock, _, engine = setup()
    run(engine, b"SET", b"k", b"v", b"PX", b"2000")
    clock.advance(2000 - remaining)
    assert run(engine, b"PTTL", b"k") == Integer(remaining)
    assert run(engine, b"TTL", b"k") == Integer(seconds)


def test_getex_without_option_preserves_deadline_and_revision() -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"", b"EX", b"1")
    before = store.key_revision(b"k"), store.deadline(b"k")
    assert run(engine, b"GETEX", b"k") == Bulk(b"")
    assert (store.key_revision(b"k"), store.deadline(b"k")) == before
    assert run(engine, b"GETEX", b"missing", b"EX", b"1") == Bulk(None)
    assert store.physical_count == store.expiration_count == 1


def test_lazy_expiry_notifies_once_and_cannot_leave_tombstones() -> None:
    clock = ManualClock(1000)
    changes: list[tuple[bytes, int]] = []
    store = Store(clock=clock, on_change=lambda key, revision: changes.append((key, revision)))
    engine = Executor(ServerConfig(), store=store)
    run(engine, b"SET", b"k", b"v", b"PX", b"1")
    clock.advance(2)
    assert run(engine, b"GET", b"k") == Bulk(None)
    assert engine.expire_batch() == 0
    assert changes == [(b"k", 1), (b"k", 2)]
    assert store.key_revision(b"k") == 0
    assert store.physical_count == store.expiration_count == store.revision_count == 0


@pytest.mark.parametrize("name", [b"EXPIRE", b"PEXPIRE", b"EXPIREAT", b"PEXPIREAT"])
@pytest.mark.parametrize("bad", [b"bad", b"01", b"9223372036854775808"])
def test_invalid_expire_arguments_preserve_metadata(name: bytes, bad: bytes) -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"old", b"EX", b"1")
    before = store.key_revision(b"k"), store.deadline(b"k")
    assert run(engine, name, b"k", bad) == Error(b"ERR value is not an integer or out of range")
    assert (store.key_revision(b"k"), store.deadline(b"k")) == before


@pytest.mark.parametrize("name", [b"EXPIRE", b"PEXPIRE", b"EXPIREAT", b"PEXPIREAT"])
def test_expire_overflow_and_unsupported_conditions(name: bytes) -> None:
    _, store, engine = setup()
    run(engine, b"SET", b"k", b"old", b"EX", b"1")
    before = store.key_revision(b"k"), store.deadline(b"k")
    if name != b"PEXPIREAT":  # A signed-64-bit absolute millisecond maximum is valid.
        assert run(engine, name, b"k", b"9223372036854775807") == Error(
            b"ERR invalid expire time in '" + name.lower() + b"' command")
    assert isinstance(run(engine, name, b"k", b"1", b"NX"), Error)
    assert (store.key_revision(b"k"), store.deadline(b"k")) == before
