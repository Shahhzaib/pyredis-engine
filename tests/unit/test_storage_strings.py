from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import Store, StringValue
from pyredis_engine.protocol import (
    Array,
    Bulk,
    Error,
    Integer,
    Reply,
    Simple,
    reply_size,
    serialize_reply,
)


@pytest.fixture
def engine() -> Executor:
    return Executor(ServerConfig())


def run(engine: Executor, *args: bytes) -> Reply:
    return engine.execute(list(args)).reply


def test_binary_store_missing_empty_case_and_overwrite(engine: Executor) -> None:
    assert run(engine, b"GET", b"") == Bulk(None)
    assert run(engine, b"sEt", b"", b"\x00\r\n\xff") == Simple(b"OK")
    assert run(engine, b"GET", b"") == Bulk(b"\x00\r\n\xff")
    assert run(engine, b"STRLEN", b"") == Integer(4)
    assert run(engine, b"SET", b"Key", b"") == Simple(b"OK")
    assert run(engine, b"GET", b"Key") == Bulk(b"")
    assert run(engine, b"GET", b"key") == Bulk(None)
    assert run(engine, b"TYPE", b"Key") == Simple(b"string")
    assert run(engine, b"TYPE", b"missing") == Simple(b"none")
    assert run(engine, b"SET", b"Key", b"new") == Simple(b"OK")
    assert run(engine, b"DBSIZE") == Integer(2)


def test_keyspace_duplicates_and_clear(engine: Executor) -> None:
    assert run(engine, b"MSET", b"a", b"1", b"b", b"2", b"a", b"3") == Simple(b"OK")
    assert run(engine, b"MGET", b"b", b"missing", b"a", b"a") == Array(
        (Bulk(b"2"), Bulk(None), Bulk(b"3"), Bulk(b"3")))
    assert run(engine, b"EXISTS", b"a", b"a", b"b", b"missing") == Integer(3)
    assert run(engine, b"DEL", b"a", b"a", b"missing") == Integer(1)
    assert run(engine, b"DBSIZE") == Integer(1)
    assert run(engine, b"FLUSHDB") == Simple(b"OK")
    assert run(engine, b"DBSIZE") == Integer(0)
    assert run(engine, b"FLUSHDB") == Simple(b"OK")


def test_append_and_missing_lengths(engine: Executor) -> None:
    assert run(engine, b"STRLEN", b"missing") == Integer(0)
    assert run(engine, b"APPEND", b"k", b"") == Integer(0)
    assert run(engine, b"EXISTS", b"k") == Integer(1)
    assert run(engine, b"APPEND", b"k", b"\x00\r\n\xff") == Integer(4)
    assert run(engine, b"APPEND", b"k", b"abc") == Integer(7)
    assert run(engine, b"GET", b"k") == Bulk(b"\x00\r\n\xffabc")


def test_integer_operations_and_extremes(engine: Executor) -> None:
    assert run(engine, b"INCR", b"n") == Integer(1)
    assert run(engine, b"DECR", b"n") == Integer(0)
    assert run(engine, b"DECR", b"missing") == Integer(-1)
    assert run(engine, b"INCRBY", b"n", b"-2") == Integer(-2)
    assert run(engine, b"INCRBY", b"min", b"-9223372036854775808") == Integer(-(2**63))
    assert run(engine, b"GET", b"min") == Bulk(b"-9223372036854775808")
    assert run(engine, b"SET", b"max", b"9223372036854775807") == Simple(b"OK")
    overflow = Error(b"ERR increment or decrement would overflow")
    assert run(engine, b"INCR", b"max") == overflow
    assert run(engine, b"DECR", b"min") == overflow
    assert run(engine, b"INCRBY", b"min", b"-1") == overflow
    assert run(engine, b"GET", b"max") == Bulk(b"9223372036854775807")
    assert run(engine, b"GET", b"min") == Bulk(b"-9223372036854775808")


@pytest.mark.parametrize("bad", [b"", b"+1", b"01", b"00", b"-0", b"-01", b" 1", b"1 ",
                                 b"1.0", b"1_0", b"\xff", b"\x001", b"-",
                                 b"9223372036854775808", b"-9223372036854775809",
                                 b"9" * 5000])
def test_invalid_integers_preserve_state(engine: Executor, bad: bytes) -> None:
    invalid = Error(b"ERR value is not an integer or out of range")
    run(engine, b"SET", b"k", bad)
    assert run(engine, b"INCR", b"k") == invalid
    assert run(engine, b"DECR", b"k") == invalid
    assert run(engine, b"GET", b"k") == Bulk(bad)
    assert run(engine, b"INCRBY", b"new", bad) == invalid
    assert run(engine, b"GET", b"new") == Bulk(None)


@pytest.mark.parametrize(("name", "bad_args"), [
    (b"SET", []), (b"SET", [b"k"]), (b"GET", []), (b"GET", [b"a", b"b"]),
    (b"DEL", []), (b"EXISTS", []), (b"TYPE", []), (b"TYPE", [b"a", b"b"]),
    (b"DBSIZE", [b"x"]), (b"INCR", []), (b"INCR", [b"a", b"b"]),
    (b"DECR", []), (b"DECR", [b"a", b"b"]), (b"INCRBY", [b"k"]),
    (b"INCRBY", [b"k", b"1", b"x"]), (b"APPEND", [b"k"]),
    (b"STRLEN", []), (b"STRLEN", [b"a", b"b"]), (b"MGET", []),
    (b"MSET", []), (b"MSET", [b"k"]), (b"MSET", [b"a", b"v", b"b"]),
])
def test_registry_arity(engine: Executor, name: bytes, bad_args: list[bytes]) -> None:
    assert run(engine, name, *bad_args) == Error(
        b"ERR wrong number of arguments for '" + name.lower() + b"' command")
    assert run(engine, b"DBSIZE") == Integer(0)


def test_options_and_future_commands_fail_without_mutating(engine: Executor) -> None:
    run(engine, b"SET", b"k", b"old")
    assert run(engine, b"SET", b"k", b"new", b"NX") == Bulk(None)
    assert run(engine, b"SET", b"k", b"new", b"GET") == Error(b"ERR syntax error")
    assert run(engine, b"FLUSHDB", b"ASYNC") == Error(b"ERR syntax error")
    assert run(engine, b"GET", b"k") == Bulk(b"old")
    # M11 registers these commands; empty calls now fail Redis-compatible arity validation.
    for name in (b"SUBSCRIBE", b"PUBLISH"):
        assert run(engine, name) == Error(
            b"ERR wrong number of arguments for '" + name.lower() + b"' command")
    for name in (b"\xff", b""):
        assert run(engine, name) == Error(b"ERR unknown command")
    assert run(engine, b"PSUBSCRIBE") == Error(
        b"ERR wrong number of arguments for 'psubscribe' command")
    assert run(engine, b"SELECT") == Error(b"ERR wrong number of arguments for 'select' command")
    assert run(engine, b"SELECT", b"0") == Simple(b"OK")
    assert run(engine, b"SELECT", b"1") == Error(b"ERR DB index is out of range")
    assert run(engine, b"GET", b"k") == Bulk(b"old")


@dataclass(frozen=True)
class OtherValue:
    kind: bytes = b"hash"


@pytest.mark.parametrize("name", [b"GET", b"STRLEN", b"APPEND", b"INCR", b"DECR", b"INCRBY"])
def test_wrong_type_is_unchanged(name: bytes) -> None:
    store = Store()
    other = OtherValue()
    store.put(b"wrong", other)
    engine = Executor(ServerConfig(), store=store)
    args = [b"x"] if name == b"APPEND" else [b"1"] if name == b"INCRBY" else []
    assert run(engine, name, b"wrong", *args) == Error(
        b"WRONGTYPE Operation against a key holding the wrong kind of value")
    assert store.get(b"wrong") is other
    assert run(engine, b"TYPE", b"wrong") == Simple(b"hash")
    assert run(engine, b"MGET", b"wrong", b"missing") == Array((Bulk(None), Bulk(None)))
    assert run(engine, b"SET", b"wrong", b"new") == Simple(b"OK")
    assert store.get(b"wrong") == StringValue(b"new")


def test_store_isolation_and_failed_mset_is_atomic() -> None:
    first, second = Executor(ServerConfig(max_bulk_bytes=4)), Executor(ServerConfig())
    run(first, b"MSET", b"a", b"old", b"b", b"old")
    assert isinstance(run(first, b"MSET", b"a", b"new", b"b", b"large"), Error)
    assert run(first, b"MGET", b"a", b"b") == Array((Bulk(b"old"), Bulk(b"old")))
    assert isinstance(run(first, b"APPEND", b"a", b"!!"), Error)
    assert run(first, b"GET", b"a") == Bulk(b"old")
    assert run(second, b"DBSIZE") == Integer(0)


def test_reply_amplification_is_rejected_before_encoding(engine: Executor) -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    run(engine, b"SET", b"big", b"x" * 64)
    assert run(engine, b"MGET", b"big", b"big") == Error(
        b"ERR reply exceeds configured maximum size")
    assert run(engine, b"GET", b"big") == Bulk(b"x" * 64)


@pytest.mark.parametrize("reply", [Simple(b"OK"), Error(b"ERR x"), Integer(-(2**63)),
                                   Bulk(None), Bulk(b"\x00\r\n\xff"), Array(None), Array(()),
                                   Array((Bulk(b""), Array((Integer(0), Error(b"ERR")))))])
def test_reply_size_matches_serialization(reply: Reply) -> None:
    assert reply_size(reply) == len(serialize_reply(reply))


def test_executor_rejects_cross_thread_use(engine: Executor) -> None:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(engine.execute, [b"SET", b"k", b"v"])
        with pytest.raises(RuntimeError, match="owning event-loop thread"):
            future.result()
    assert run(engine, b"DBSIZE") == Integer(0)


def test_invalid_reply_configuration() -> None:
    with pytest.raises(ValueError, match="reply limit"):
        ServerConfig(max_reply_bytes=127)


def test_mset_overwrites_other_types_and_del_removes_any_type() -> None:
    store = Store()
    store.put(b"a", OtherValue())
    store.put(b"b", OtherValue())
    engine = Executor(ServerConfig(), store=store)
    assert run(engine, b"MSET", b"a", b"v") == Simple(b"OK")
    assert run(engine, b"GET", b"a") == Bulk(b"v")
    assert run(engine, b"DEL", b"a", b"b", b"b") == Integer(2)
    assert run(engine, b"DBSIZE") == Integer(0)
