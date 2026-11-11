from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple

WRONGTYPE = Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")


@pytest.fixture
def engine() -> Executor:
    return Executor(ServerConfig(), clock=ManualClock(1000))


def run(engine: Executor, *args: bytes) -> Reply:
    return engine.execute(list(args)).reply


def test_hash_binary_empty_duplicates_and_new_field_counts(engine: Executor) -> None:
    assert run(engine, b"HGET", b"", b"") == Bulk(None)
    assert run(engine, b"hSeT", b"", b"", b"\x00\r\n\xff", b"a", b"1", b"a", b"2") == Integer(2)
    assert run(engine, b"HGET", b"", b"a") == Bulk(b"2")
    assert run(engine, b"HGET", b"", b"") == Bulk(b"\x00\r\n\xff")
    assert run(engine, b"HSET", b"", b"a", b"") == Integer(0)
    assert run(engine, b"HEXISTS", b"", b"a") == Integer(1)
    assert run(engine, b"HEXISTS", b"", b"A") == Integer(0)
    assert run(engine, b"HLEN", b"") == Integer(2)
    assert run(engine, b"HGETALL", b"") == Array(
        (Bulk(b""), Bulk(b"\x00\r\n\xff"), Bulk(b"a"), Bulk(b"")))
    assert run(engine, b"TYPE", b"") == Simple(b"hash")


def test_hash_missing_and_deleting_last_field_removes_metadata(engine: Executor) -> None:
    for name, expected in [(b"HGETALL", Array(())), (b"HLEN", Integer(0))]:
        assert run(engine, name, b"missing") == expected
    assert run(engine, b"HDEL", b"missing", b"f") == Integer(0)
    assert run(engine, b"HEXISTS", b"missing", b"f") == Integer(0)
    run(engine, b"HSET", b"h", b"a", b"1", b"b", b"2")
    run(engine, b"EXPIRE", b"h", b"1")
    before = engine.store.key_revision(b"h")
    assert run(engine, b"HDEL", b"h", b"missing") == Integer(0)
    assert engine.store.key_revision(b"h") == before
    assert run(engine, b"HDEL", b"h", b"a", b"a", b"missing") == Integer(1)
    assert run(engine, b"PTTL", b"h") == Integer(1000)
    assert run(engine, b"HDEL", b"h", b"b") == Integer(1)
    assert run(engine, b"TYPE", b"h") == Simple(b"none")
    assert engine.store.expiration_count == engine.store.revision_count == 0


def test_push_direction_pop_shapes_and_last_element_deletion(engine: Executor) -> None:
    assert run(engine, b"LPUSH", b"l", b"a", b"b", b"c") == Integer(3)
    assert run(engine, b"RPUSH", b"l", b"d", b"") == Integer(5)
    assert run(engine, b"TYPE", b"l") == Simple(b"list")
    assert run(engine, b"LRANGE", b"l", b"0", b"-1") == Array(
        tuple(Bulk(value) for value in [b"c", b"b", b"a", b"d", b""]))
    assert run(engine, b"LPOP", b"l") == Bulk(b"c")
    assert run(engine, b"RPOP", b"l", b"2") == Array((Bulk(b""), Bulk(b"d")))
    assert run(engine, b"LPOP", b"l", b"0") == Array(())
    assert run(engine, b"LLEN", b"l") == Integer(2)
    run(engine, b"PEXPIRE", b"l", b"10")
    assert run(engine, b"LPOP", b"l", b"9223372036854775807") == Array((Bulk(b"b"), Bulk(b"a")))
    assert run(engine, b"LPOP", b"l") == Bulk(None)
    assert run(engine, b"RPOP", b"l", b"0") == Array(None)
    assert engine.store.physical_count == engine.store.expiration_count == 0


@pytest.mark.parametrize(("start", "stop", "expected"), [
    (0, 0, [b"a"]), (0, 1, [b"a", b"b"]), (-2, -1, [b"b", b"c"]),
    (-100, 100, [b"a", b"b", b"c"]), (3, 100, []), (2, 1, []),
    (0, -4, []), (0, -5, []), (-(2**63), 2**63 - 1, [b"a", b"b", b"c"]),
])
def test_inclusive_clamped_ranges(
    engine: Executor, start: int, stop: int, expected: list[bytes],
) -> None:
    run(engine, b"RPUSH", b"l", b"a", b"b", b"c")
    assert run(engine, b"LRANGE", b"l", str(start).encode(), str(stop).encode()) == Array(
        tuple(Bulk(value) for value in expected))
    assert run(engine, b"LLEN", b"l") == Integer(3)


@pytest.mark.parametrize("name,arguments", [
    (b"HSET", [b"f", b"v"]), (b"HGET", [b"f"]), (b"HDEL", [b"f"]),
    (b"HEXISTS", [b"f"]), (b"HGETALL", []), (b"HLEN", []),
    (b"LPUSH", [b"v"]), (b"RPUSH", [b"v"]), (b"LPOP", []),
    (b"RPOP", [b"0"]), (b"LRANGE", [b"0", b"-1"]), (b"LLEN", []),
])
def test_wrong_type_errors_preserve_value_ttl_revision(
    engine: Executor, name: bytes, arguments: list[bytes],
) -> None:
    run(engine, b"SET", b"k", b"old", b"PX", b"100")
    before = engine.store.key_revision(b"k"), engine.store.deadline(b"k")
    assert run(engine, name, b"k", *arguments) == WRONGTYPE
    assert run(engine, b"GET", b"k") == Bulk(b"old")
    assert (engine.store.key_revision(b"k"), engine.store.deadline(b"k")) == before


@pytest.mark.parametrize("name,arguments", [
    (b"HSET", []), (b"HSET", [b"h", b"f"]), (b"HSET", [b"h", b"f", b"v", b"bad"]),
    (b"HGET", [b"h"]), (b"HDEL", [b"h"]), (b"HEXISTS", [b"h"]),
    (b"HGETALL", []), (b"HGETALL", [b"h", b"extra"]), (b"HLEN", []),
    (b"LPUSH", [b"l"]), (b"RPUSH", [b"l"]), (b"LPOP", []),
    (b"RPOP", [b"l", b"1", b"extra"]), (b"LRANGE", [b"l", b"0"]), (b"LLEN", []),
])
def test_registry_arity(engine: Executor, name: bytes, arguments: list[bytes]) -> None:
    assert run(engine, name, *arguments) == Error(
        b"ERR wrong number of arguments for '" + name.lower() + b"' command")
    assert engine.store.physical_count == 0


@pytest.mark.parametrize("raw", [b"bad", b"01", b"+1", b"-0", b"1_0", b"9223372036854775808"])
def test_invalid_list_numbers_do_not_modify(engine: Executor, raw: bytes) -> None:
    run(engine, b"RPUSH", b"l", b"a", b"b")
    before = engine.store.key_revision(b"l")
    invalid = Error(b"ERR value is not an integer or out of range")
    assert run(engine, b"LPOP", b"l", raw) == Error(
        b"ERR value is out of range, must be positive")
    assert run(engine, b"LRANGE", b"l", raw, b"-1") == invalid
    assert run(engine, b"LRANGE", b"l", b"0", raw) == invalid
    assert engine.store.key_revision(b"l") == before
    assert run(engine, b"LRANGE", b"l", b"0", b"-1") == Array((Bulk(b"a"), Bulk(b"b")))


def test_negative_and_zero_pop_counts(engine: Executor) -> None:
    run(engine, b"RPUSH", b"l", b"a")
    revision = engine.store.key_revision(b"l")
    assert run(engine, b"RPOP", b"l", b"-1") == Error(
        b"ERR value is out of range, must be positive")
    assert run(engine, b"LPOP", b"l", b"0") == Array(())
    assert engine.store.key_revision(b"l") == revision


def test_mutations_preserve_expiry_then_expired_keys_recreate_empty() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    run(engine, b"HSET", b"h", b"a", b"1")
    run(engine, b"RPUSH", b"l", b"1", b"2")
    for key in (b"h", b"l"):
        run(engine, b"PEXPIRE", key, b"10")
    for command in [(b"HSET", b"h", b"a", b"2"), (b"HSET", b"h", b"b", b"3"),
                    (b"HDEL", b"h", b"b"), (b"LPUSH", b"l", b"0"),
                    (b"RPUSH", b"l", b"3"), (b"LPOP", b"l"), (b"RPOP", b"l")]:
        run(engine, *command)
    assert run(engine, b"PTTL", b"h") == run(engine, b"PTTL", b"l") == Integer(10)
    clock.advance(11)
    assert run(engine, b"HGETALL", b"h") == Array(())
    assert run(engine, b"LLEN", b"l") == Integer(0)
    assert run(engine, b"HSET", b"h", b"new", b"v") == Integer(1)
    assert run(engine, b"RPUSH", b"l", b"new") == Integer(1)
    assert run(engine, b"PTTL", b"h") == run(engine, b"PTTL", b"l") == Integer(-1)


def test_real_collection_types_interoperate_with_string_and_keyspace_commands(
    engine: Executor,
) -> None:
    run(engine, b"HSET", b"h", b"f", b"v")
    run(engine, b"LPUSH", b"l", b"v")
    for key in (b"h", b"l"):
        assert run(engine, b"GET", key) == WRONGTYPE
        assert run(engine, b"GETEX", key, b"PERSIST") == WRONGTYPE
        assert run(engine, b"STRLEN", key) == WRONGTYPE
        assert run(engine, b"APPEND", key, b"x") == WRONGTYPE
        assert run(engine, b"INCR", key) == WRONGTYPE
    assert run(engine, b"MGET", b"h", b"l") == Array((Bulk(None), Bulk(None)))
    assert run(engine, b"HGET", b"l", b"f") == WRONGTYPE
    assert run(engine, b"LRANGE", b"h", b"0", b"-1") == WRONGTYPE
    assert run(engine, b"SET", b"h", b"string") == Simple(b"OK")
    assert run(engine, b"MSET", b"l", b"string") == Simple(b"OK")
    assert run(engine, b"TYPE", b"h") == run(engine, b"TYPE", b"l") == Simple(b"string")


def test_pop_reply_rejection_is_atomic_for_both_shapes() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    run(engine, b"RPUSH", b"l", b"x" * 128, b"y")
    run(engine, b"EXPIRE", b"l", b"100")
    before = engine.store.key_revision(b"l"), engine.store.deadline(b"l")
    rejection = Error(b"ERR reply exceeds configured maximum size")
    assert run(engine, b"LPOP", b"l") == rejection
    assert run(engine, b"LPOP", b"l", b"2") == rejection
    assert (engine.store.key_revision(b"l"), engine.store.deadline(b"l")) == before
    assert run(engine, b"LLEN", b"l") == Integer(2)
    assert run(engine, b"RPOP", b"l") == Bulk(b"y")


def test_collection_limits_validate_entire_mutation() -> None:
    engine = Executor(ServerConfig(max_collection_items=2, max_collection_bytes=6))
    assert run(engine, b"HSET", b"h", b"a", b"1") == Integer(1)
    assert isinstance(run(engine, b"HSET", b"h", b"b", b"2", b"c", b"3"), Error)
    assert run(engine, b"HGETALL", b"h") == Array((Bulk(b"a"), Bulk(b"1")))
    assert isinstance(run(engine, b"HSET", b"h", b"a", b"123456"), Error)
    assert run(engine, b"HGET", b"h", b"a") == Bulk(b"1")
    assert run(engine, b"RPUSH", b"l", b"a") == Integer(1)
    revision = engine.store.key_revision(b"l")
    assert isinstance(run(engine, b"LPUSH", b"l", b"b", b"c"), Error)
    assert isinstance(run(engine, b"RPUSH", b"l", b"123456"), Error)
    assert engine.store.key_revision(b"l") == revision
    assert run(engine, b"LRANGE", b"l", b"0", b"-1") == Array((Bulk(b"a"),))


def test_hash_and_list_reply_caps_leave_live_state_usable() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    run(engine, b"HSET", b"h", b"a", b"x" * 64, b"b", b"y" * 64)
    run(engine, b"RPUSH", b"l", b"x" * 64, b"y" * 64)
    rejection = Error(b"ERR reply exceeds configured maximum size")
    assert run(engine, b"HGETALL", b"h") == rejection
    assert run(engine, b"LRANGE", b"l", b"0", b"-1") == rejection
    assert run(engine, b"HGET", b"h", b"b") == Bulk(b"y" * 64)
    assert run(engine, b"LPOP", b"l") == Bulk(b"x" * 64)
    assert run(engine, b"LRANGE", b"l", b"0", b"-1") == Array((Bulk(b"y" * 64),))


def test_right_pop_reply_cap_preserves_ttl_and_revision() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128), clock=ManualClock(1000))
    run(engine, b"RPUSH", b"l", b"small", b"x" * 128)
    run(engine, b"PEXPIRE", b"l", b"10")
    before = engine.store.key_revision(b"l"), engine.store.deadline(b"l")
    for command in [(b"RPOP", b"l"), (b"RPOP", b"l", b"2")]:
        assert run(engine, *command) == Error(b"ERR reply exceeds configured maximum size")
        assert (engine.store.key_revision(b"l"), engine.store.deadline(b"l")) == before
    assert run(engine, b"LPOP", b"l") == Bulk(b"small")


def test_missing_collection_creation_rejected_without_metadata() -> None:
    engine = Executor(ServerConfig(max_collection_items=1, max_collection_bytes=6))
    for command in [(b"HSET", b"h", b"a", b"1", b"b", b"2"),
                    (b"RPUSH", b"l", b"a", b"b")]:
        assert run(engine, *command) == Error(b"ERR collection exceeds configured maximum size")
        assert engine.store.physical_count == engine.store.revision_count == 0


def test_payload_accounting_releases_capacity_on_update_delete_and_pop() -> None:
    engine = Executor(ServerConfig(max_collection_bytes=6))
    assert run(engine, b"HSET", b"h", b"a", b"123", b"b", b"1") == Integer(2)
    assert run(engine, b"HSET", b"h", b"a", b"") == Integer(0)
    assert run(engine, b"HDEL", b"h", b"b") == Integer(1)
    assert run(engine, b"HSET", b"h", b"c", b"1234") == Integer(1)
    assert run(engine, b"RPUSH", b"l", b"123", b"456") == Integer(2)
    assert run(engine, b"LPOP", b"l") == Bulk(b"123")
    assert run(engine, b"LPUSH", b"l", b"abc") == Integer(2)


def test_active_expiration_removes_collections_and_metadata() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    run(engine, b"HSET", b"h", b"a", b"1")
    run(engine, b"RPUSH", b"l", b"1", b"2")
    run(engine, b"PEXPIRE", b"h", b"1")
    run(engine, b"PEXPIRE", b"l", b"1")
    clock.advance(2)
    assert engine.expire_batch() == 2
    assert engine.store.physical_count == engine.store.expiration_count == 0
    assert engine.store.revision_count == 0


def test_large_list_range_and_clamped_count(engine: Executor) -> None:
    for offset in range(0, 2000, 100):
        values = [str(number).encode() for number in range(offset, offset + 100)]
        assert run(engine, b"RPUSH", b"l", *values) == Integer(offset + 100)
    assert run(engine, b"LRANGE", b"l", b"-3", b"-1") == Array(
        (Bulk(b"1997"), Bulk(b"1998"), Bulk(b"1999")))
    reply = run(engine, b"RPOP", b"l", b"9223372036854775807")
    assert isinstance(reply, Array) and reply.payload is not None
    assert reply.payload == tuple(Bulk(str(number).encode()) for number in reversed(range(2000)))
    assert engine.store.physical_count == 0


@pytest.mark.parametrize("key", [b"", b"\x00\r\n\xff"])
def test_binary_list_keys_and_empty_members(engine: Executor, key: bytes) -> None:
    assert run(engine, b"LPUSH", key, b"", b"\x00\r\n\xff") == Integer(2)
    assert run(engine, b"LRANGE", key, b"0", b"-1") == Array(
        (Bulk(b"\x00\r\n\xff"), Bulk(b"")))
    assert run(engine, b"LPOP", key, b"2") == Array((Bulk(b"\x00\r\n\xff"), Bulk(b"")))
    assert run(engine, b"TYPE", key) == Simple(b"none")


@pytest.mark.parametrize("command", [
    (b"HSET", b"h", b"large", b"v"), (b"HSET", b"h", b"f", b"large"),
    (b"RPUSH", b"l", b"large"), (b"LPUSH", b"l", b"ok", b"large"),
])
def test_oversized_collection_payload_does_not_create_key(command: tuple[bytes, ...]) -> None:
    engine = Executor(ServerConfig(max_bulk_bytes=4))
    assert run(engine, *command) == Error(b"ERR collection element exceeds configured maximum size")
    assert engine.store.physical_count == engine.store.revision_count == 0


@pytest.mark.parametrize("name", ["max_collection_items", "max_collection_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, float("nan"), float("inf")])
def test_invalid_collection_limits(name: str, value: int | float) -> None:
    with pytest.raises(ValueError, match="collection limits"):
        ServerConfig(**{name: value})  # type: ignore[arg-type]
