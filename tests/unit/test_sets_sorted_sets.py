from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import Store
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple

INVALID = Error(b"ERR value is not a valid float")
WRONGTYPE = Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")


def run(engine: Executor, *args: bytes) -> Reply:
    return engine.execute(list(args)).reply


def array(*values: bytes) -> Array:
    return Array(tuple(Bulk(value) for value in values))


@pytest.fixture
def engine() -> Executor:
    return Executor(ServerConfig(), clock=ManualClock(1000))


def test_set_binary_duplicates_membership_and_final_delete(engine: Executor) -> None:
    assert run(engine, b"SMEMBERS", b"") == array()
    assert run(engine, b"sAdD", b"", b"", b"\x00\r\n\xff", b"", b"A") == Integer(3)
    assert run(engine, b"SMEMBERS", b"") == array(b"", b"\x00\r\n\xff", b"A")
    assert run(engine, b"SISMEMBER", b"", b"") == Integer(1)
    assert run(engine, b"SISMEMBER", b"", b"a") == Integer(0)
    assert run(engine, b"TYPE", b"") == Simple(b"set")
    assert run(engine, b"SCARD", b"") == Integer(3)
    run(engine, b"PEXPIRE", b"", b"100")
    before = engine.store.key_revision(b"")
    assert run(engine, b"SADD", b"", b"A", b"A") == Integer(0)
    assert run(engine, b"SREM", b"", b"missing") == Integer(0)
    assert engine.store.key_revision(b"") == before
    assert run(engine, b"SREM", b"", b"", b"", b"missing") == Integer(1)
    assert run(engine, b"PTTL", b"") == Integer(100)
    assert run(engine, b"SREM", b"", b"A", b"\x00\r\n\xff") == Integer(2)
    assert run(engine, b"TYPE", b"") == Simple(b"none")
    assert engine.store.expiration_count == engine.store.revision_count == 0


def test_set_addition_preserves_deadline_and_other_keyspace_commands(engine: Executor) -> None:
    run(engine, b"SADD", b"k", b"a")
    run(engine, b"PEXPIRE", b"k", b"20")
    revision = engine.store.key_revision(b"k")
    assert run(engine, b"SADD", b"k", b"b", b"b") == Integer(1)
    assert engine.store.key_revision(b"k") > revision
    assert run(engine, b"PTTL", b"k") == Integer(20)
    run(engine, b"ZADD", b"z", b"1", b"a")
    assert run(engine, b"MGET", b"k", b"z") == Array((Bulk(None), Bulk(None)))
    assert run(engine, b"GETEX", b"k", b"PERSIST") == WRONGTYPE
    assert run(engine, b"PTTL", b"k") == Integer(20)
    assert run(engine, b"DEL", b"k", b"z", b"k") == Integer(2)
    assert engine.store.physical_count == engine.store.expiration_count == 0


def test_zadd_updates_duplicate_pairs_ties_and_revisions(engine: Executor) -> None:
    assert run(engine, b"ZADD", b"z", b"1", b"\xff", b"1", b"\x00", b"0", b"") == Integer(3)
    assert run(engine, b"TYPE", b"z") == Simple(b"zset")
    assert run(engine, b"ZRANGE", b"z", b"0", b"-1") == array(b"", b"\x00", b"\xff")
    revision = engine.store.key_revision(b"z")
    assert run(engine, b"ZADD", b"z", b"1.0", b"\xff") == Integer(0)
    assert engine.store.key_revision(b"z") == revision
    assert run(engine, b"ZADD", b"z", b"-1", b"\xff", b"2", b"new", b"3", b"new") == Integer(1)
    assert run(engine, b"ZRANGE", b"z", b"0", b"-1", b"withscores") == array(
        b"\xff", b"-1", b"", b"0", b"\x00", b"1", b"new", b"3")
    assert run(engine, b"ZSCORE", b"z", b"new") == Bulk(b"3")
    assert run(engine, b"ZSCORE", b"z", b"missing") == Bulk(None)
    revision = engine.store.key_revision(b"z")
    run(engine, b"ZADD", b"z", b"8", b"new", b"3", b"new")
    assert engine.store.key_revision(b"z") > revision
    assert run(engine, b"ZCARD", b"z") == Integer(4)


@pytest.mark.parametrize("raw,expected", [
    (b"+inf", b"inf"), (b"-inf", b"-inf"), (b"INF", b"inf"),
    (b"1.5", b"1.5"), (b".5", b"0.5"), (b"1.", b"1"), (b"+01", b"1"),
    (b"-0", b"0"), (b"-0.0e10", b"0"), (b"1e2", b"100"),
    (b"0.1", b"0.1"), (b"9007199254740993", b"9007199254740992"),
    (b"5e-324", b"5e-324"), (b"0x1p2", b"4"),
    (b"1.7976931348623157e308", b"1.7976931348623157e+308"),
])
def test_binary64_scores(engine: Executor, raw: bytes, expected: bytes) -> None:
    assert run(engine, b"ZADD", b"z", raw, b"m") == Integer(1)
    assert run(engine, b"ZSCORE", b"z", b"m") == Bulk(expected)
    assert run(engine, b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES") == array(b"m", expected)


@pytest.mark.parametrize("raw", [
    b"", b"nan", b"NaN", b"+nan", b"-NaN", b"1e309", b"-1e999", b"1e-999",
    b"1_0", b" 1", b"1 ", b"1\x00", b"\xff", b"1e", b"--1", b"0x1p1024",
])
def test_invalid_scores_reject_entire_batch(engine: Executor, raw: bytes) -> None:
    run(engine, b"ZADD", b"z", b"1", b"old")
    run(engine, b"PEXPIRE", b"z", b"10")
    before = engine.store.key_revision(b"z"), engine.store.deadline(b"z")
    assert run(engine, b"ZADD", b"z", b"2", b"old", raw, b"new") == INVALID
    assert run(engine, b"ZADD", b"missing", b"2", b"a", raw, b"a") == INVALID
    assert run(engine, b"ZSCORE", b"z", b"old") == Bulk(b"1")
    assert run(engine, b"ZCARD", b"z") == Integer(1)
    assert run(engine, b"EXISTS", b"missing") == Integer(0)
    assert (engine.store.key_revision(b"z"), engine.store.deadline(b"z")) == before


@pytest.mark.parametrize("first,last,expected", [
    (0, 0, (b"a",)), (-2, -1, (b"b", b"c")), (-99, 99, (b"a", b"b", b"c")),
    (3, 99, ()), (2, 1, ()), (0, -4, ()), (0, -5, ()),
    (-(2**63), 2**63 - 1, (b"a", b"b", b"c")),
])
def test_rank_bounds(engine: Executor, first: int, last: int, expected: tuple[bytes, ...]) -> None:
    run(engine, b"ZADD", b"z", b"-inf", b"a", b"0", b"b", b"+inf", b"c")
    assert run(engine, b"ZRANGE", b"z", str(first).encode(), str(last).encode()) == array(*expected)


def test_hex_updates_preserve_expiry_ties_and_noop_revisions(engine: Executor) -> None:
    run(engine, b"ZADD", b"z", b"1", b"a", b"4", b"\xff")
    run(engine, b"PEXPIRE", b"z", b"100")
    revision = engine.store.key_revision(b"z")
    assert run(engine, b"ZADD", b"z", b"0x1p2", b"a", b"0x1p2", b"\x00") == Integer(1)
    assert engine.store.key_revision(b"z") > revision
    assert run(engine, b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES") == array(
        b"\x00", b"4", b"a", b"4", b"\xff", b"4")
    assert run(engine, b"PTTL", b"z") == Integer(100)
    revision = engine.store.key_revision(b"z")
    assert run(engine, b"ZADD", b"z", b"4.0", b"a") == Integer(0)
    assert engine.store.key_revision(b"z") == revision


COMMAND_ARGS = [
    (b"SADD", (b"m",)), (b"SREM", (b"m",)), (b"SISMEMBER", (b"m",)),
    (b"SMEMBERS", ()), (b"SCARD", ()), (b"ZADD", (b"1", b"m")),
    (b"ZREM", (b"m",)), (b"ZSCORE", (b"m",)), (b"ZRANGE", (b"0", b"-1")),
    (b"ZCARD", ()),
]


@pytest.mark.parametrize("name,args", COMMAND_ARGS)
@pytest.mark.parametrize("creation", [
    (b"SET", b"k", b"old"), (b"HSET", b"k", b"f", b"v"),
    (b"RPUSH", b"k", b"v"), (b"SADD", b"k", b"v"), (b"ZADD", b"k", b"1", b"v"),
])
def test_wrong_types_and_arity(
    engine: Executor, name: bytes, args: tuple[bytes, ...], creation: tuple[bytes, ...],
) -> None:
    run(engine, *creation)
    run(engine, b"PEXPIRE", b"k", b"100")
    revision = engine.store.key_revision(b"k")
    same_kind = creation[0] == (b"SADD" if name.startswith(b"S") else b"ZADD")
    if not same_kind:
        assert run(engine, name, b"k", *args) == WRONGTYPE
        assert engine.store.key_revision(b"k") == revision
        assert run(engine, b"PTTL", b"k") == Integer(100)
    assert run(engine, name) == Error(
        b"ERR wrong number of arguments for '" + name.lower() + b"' command")


@pytest.mark.parametrize("raw", [b"01", b"+1", b"-0", b"x", b"9223372036854775808"])
def test_invalid_ranks_and_options(engine: Executor, raw: bytes) -> None:
    invalid = Error(b"ERR value is not an integer or out of range")
    assert run(engine, b"ZRANGE", b"missing", raw, b"0") == invalid
    assert run(engine, b"ZRANGE", b"missing", b"0", raw) == invalid
    assert run(engine, b"ZRANGE", b"missing", b"0", b"-1", b"REV") == Error(b"ERR syntax error")
    assert isinstance(run(engine, b"ZADD", b"z", b"NX", b"1", b"m"), Error)
    assert isinstance(run(engine, b"ZADD", b"z", b"1", b"m", b"2"), Error)
    assert engine.store.physical_count == 0


@pytest.mark.parametrize("sorted_set", [False, True])
def test_expiration_mutations_overwrites_and_metadata(sorted_set: bool) -> None:
    clock = ManualClock(1000)
    changes: list[tuple[bytes, int]] = []
    store = Store(clock=clock, on_change=lambda key, version: changes.append((key, version)))
    engine = Executor(ServerConfig(), store=store)
    create = (b"ZADD", b"k", b"1", b"a", b"2", b"b") if sorted_set else (b"SADD", b"k", b"a", b"b")
    remove = b"ZREM" if sorted_set else b"SREM"
    run(engine, *create)
    run(engine, b"PEXPIRE", b"k", b"10")
    revision = store.key_revision(b"k")
    assert run(engine, remove, b"k", b"a", b"a") == Integer(1)
    assert store.key_revision(b"k") > revision
    assert run(engine, b"PTTL", b"k") == Integer(10)
    clock.advance(10)
    assert run(engine, b"EXISTS", b"k") == Integer(1)
    clock.advance(1)
    assert run(engine, b"EXISTS", b"k") == Integer(0)
    assert store.expiration_count == store.revision_count == 0
    assert changes[-1][0] == b"k"
    run(engine, *create)
    assert run(engine, b"PTTL", b"k") == Integer(-1)
    run(engine, b"PEXPIRE", b"k", b"100")
    assert run(engine, remove, b"k", b"a", b"b", b"b") == Integer(2)
    assert store.physical_count == store.expiration_count == store.revision_count == 0
    run(engine, *create)
    run(engine, b"SET", b"k", b"new")
    assert run(engine, b"GET", b"k") == Bulk(b"new")
    assert run(engine, b"PTTL", b"k") == Integer(-1)


@pytest.mark.parametrize("sorted_set", [False, True])
def test_growth_limits_are_atomic_and_release_capacity(sorted_set: bool) -> None:
    engine = Executor(ServerConfig(max_collection_items=2, max_collection_bytes=18))
    def add(*members: bytes) -> Reply:
        args = (tuple(part for member in members for part in (b"1", member))
                if sorted_set else members)
        return run(engine, b"ZADD" if sorted_set else b"SADD", b"k", *args)
    assert add(b"a", b"b", b"a") == Integer(2)
    revision = engine.store.key_revision(b"k")
    assert isinstance(add(b"c"), Error)
    assert engine.store.key_revision(b"k") == revision
    assert run(engine, b"ZREM" if sorted_set else b"SREM", b"k", b"a") == Integer(1)
    assert add(b"c") == Integer(1)
    assert run(engine, b"ZCARD" if sorted_set else b"SCARD", b"k") == Integer(2)
    large = b"x" * 19
    assert isinstance(add(large), Error)
    assert engine.store.key_revision(b"k") > revision
    run(engine, b"DEL", b"k")
    assert isinstance(add(large), Error)
    assert engine.store.physical_count == 0


@pytest.mark.parametrize("name,args", COMMAND_ARGS)
def test_expired_keys_are_missing_for_every_command(name: bytes, args: tuple[bytes, ...]) -> None:
    clock = ManualClock(100)
    engine = Executor(ServerConfig(), clock=clock)
    # An expired string must not trigger WRONGTYPE for any collection command.
    run(engine, b"SET", b"k", b"old", b"PX", b"1")
    clock.advance(2)
    reply = run(engine, name, b"k", *args)
    if name in (b"SADD", b"ZADD"):
        assert reply == Integer(1)
        assert run(engine, b"PTTL", b"k") == Integer(-1)
    elif name in (b"SMEMBERS", b"ZRANGE"):
        assert reply == array()
    elif name == b"ZSCORE":
        assert reply == Bulk(None)
    else:
        assert reply == Integer(0)
    assert engine.store.expiration_count == 0


def test_active_expiry_removes_many_unaccessed_collections() -> None:
    clock = ManualClock(0)
    engine = Executor(ServerConfig(expiry_batch_size=17), clock=clock)
    for number in range(250):
        key = str(number).encode()
        if number % 2:
            run(engine, b"SADD", key, b"a", b"b")
        else:
            run(engine, b"ZADD", key, b"1", b"a", b"2", b"b")
        run(engine, b"PEXPIRE", key, b"1")
    clock.advance(2)
    assert engine.store.physical_count == engine.store.expiration_count == 250
    removed = 0
    while engine.store.physical_count:
        batch = engine.expire_batch()
        assert 0 < batch <= 17
        removed += batch
    assert removed == 250
    assert engine.store.expiration_count == engine.store.revision_count == 0


def test_zadd_score_updates_preserve_expiry_and_bounded_indexes(engine: Executor) -> None:
    run(engine, b"ZADD", b"z", b"1", b"a", b"2", b"b")
    run(engine, b"PEXPIRE", b"z", b"100")
    for number in range(1000):
        run(engine, b"ZADD", b"z", str(number).encode(), b"a")
    value = engine.store.get_sorted_set(b"z")
    assert value is not None
    assert value.ranked == [(2.0, b"b"), (999.0, b"a")]
    assert value.scores == {b"a": 999.0, b"b": 2.0}
    assert value.byte_size == 18
    assert run(engine, b"PTTL", b"z") == Integer(100)
    assert engine.store.expiration_count == engine.store.revision_count == 1
    assert run(engine, b"ZREM", b"z", b"missing") == Integer(0)
    assert run(engine, b"ZREM", b"z", b"a", b"a", b"b") == Integer(2)
    assert engine.store.physical_count == engine.store.expiration_count == 0


@pytest.mark.parametrize("name,args", [
    (b"SADD", (b"k",)), (b"SREM", (b"k",)), (b"SISMEMBER", (b"k", b"m", b"x")),
    (b"SMEMBERS", (b"k", b"x")), (b"SCARD", (b"k", b"x")),
    (b"ZADD", (b"k", b"1", b"m", b"2")), (b"ZREM", (b"k",)),
    (b"ZSCORE", (b"k", b"m", b"x")), (b"ZRANGE", (b"k", b"0", b"1", b"WITHSCORES", b"x")),
    (b"ZCARD", (b"k", b"x")),
])
def test_extra_and_incomplete_arguments(
    engine: Executor, name: bytes, args: tuple[bytes, ...],
) -> None:
    expected = (b"ERR syntax error" if name in (b"ZADD", b"ZRANGE") else
                b"ERR wrong number of arguments for '" + name.lower() + b"' command")
    assert run(engine, name, *args) == Error(expected)
    assert engine.store.physical_count == 0


def test_withscores_reply_budget_accounts_for_scores() -> None:
    constrained = Executor(ServerConfig(max_reply_bytes=128))
    member = b"a" * 105
    # Keep the same budget boundary using a genuinely long Redis score string.
    score = b"2.2250738585072014e-308"
    run(constrained, b"ZADD", b"k", score, member)
    assert run(constrained, b"ZRANGE", b"k", b"0", b"-1") == array(member)
    revision = constrained.store.key_revision(b"k")
    assert run(constrained, b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES") == Error(
        b"ERR reply exceeds configured maximum size")
    assert constrained.store.key_revision(b"k") == revision
    assert run(constrained, b"ZSCORE", b"k", member) == Bulk(score)


@pytest.mark.parametrize("sorted_set", [False, True])
def test_payload_cap_checks_entire_batch_before_mutation(sorted_set: bool) -> None:
    engine = Executor(ServerConfig(max_collection_items=10, max_collection_bytes=18),
                      clock=ManualClock(1000))
    create = (b"ZADD", b"k", b"1", b"a") if sorted_set else (b"SADD", b"k", b"a")
    assert run(engine, *create) == Integer(1)
    run(engine, b"PEXPIRE", b"k", b"10")
    revision = engine.store.key_revision(b"k")
    args = ((b"2", b"a", b"1", b"bc") if sorted_set else (b"b", b"c" * 17))
    assert run(engine, create[0], b"k", *args) == Error(
        b"ERR collection exceeds configured maximum size")
    assert engine.store.key_revision(b"k") == revision
    assert run(engine, b"PTTL", b"k") == Integer(10)
    if sorted_set:
        assert run(engine, b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES") == array(b"a", b"1")
    else:
        assert run(engine, b"SMEMBERS", b"k") == array(b"a")


@pytest.mark.parametrize("sorted_set", [False, True])
def test_bulk_and_reply_bounds_preserve_data(sorted_set: bool) -> None:
    engine = Executor(ServerConfig(max_bulk_bytes=128, max_reply_bytes=128))
    name = b"ZADD" if sorted_set else b"SADD"
    args = (b"1", b"a" * 100, b"2", b"b" * 100) if sorted_set else (b"a" * 100, b"b" * 100)
    assert run(engine, name, b"k", *args) == Integer(2)
    before = engine.store.key_revision(b"k")
    read = (b"ZRANGE", b"k", b"0", b"-1") if sorted_set else (b"SMEMBERS", b"k")
    assert run(engine, *read) == Error(b"ERR reply exceeds configured maximum size")
    oversized = (b"1", b"c" * 129) if sorted_set else (b"c" * 129,)
    assert run(engine, name, b"k", *oversized) == Error(
        b"ERR collection element exceeds configured maximum size")
    assert engine.store.key_revision(b"k") == before
    assert run(engine, b"ZCARD" if sorted_set else b"SCARD", b"k") == Integer(2)
