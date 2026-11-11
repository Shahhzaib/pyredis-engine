"""Exact expectations established by independent Redis 7.4.12 raw RESP traces."""
from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply


def run(engine: Executor, *command: bytes) -> Reply:
    return engine.execute(list(command)).reply


@pytest.mark.parametrize("earlier", [b"bad", b"0"])
def test_getex_only_validates_final_repeated_timeout(earlier: bytes) -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    run(engine, b"SET", b"k", b"old")
    assert run(engine, b"GETEX", b"k", b"PX", earlier, b"PX", b"60000") == Bulk(b"old")
    assert run(engine, b"PTTL", b"k") == Integer(60000)


@pytest.mark.parametrize("name", [b"LPOP", b"RPOP"])
def test_counted_missing_pop_preserves_resp_null_type(name: bytes) -> None:
    engine = Executor(ServerConfig())
    assert run(engine, name, b"missing") == Bulk(None)
    for count in (b"0", b"1", b"9223372036854775807"):
        assert run(engine, name, b"missing", count) == Array(None)
    assert engine.store.physical_count == engine.store.revision_count == 0


@pytest.mark.parametrize("name", [b"LPOP", b"RPOP"])
@pytest.mark.parametrize("raw", [b"bad", b"-1", b"+1", b"01", b"-0", b"9223372036854775808"])
def test_invalid_pop_count_is_atomic(name: bytes, raw: bytes) -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    run(engine, b"RPUSH", b"k", b"a", b"b")
    run(engine, b"PEXPIRE", b"k", b"100")
    before = engine.store.key_revision(b"k"), engine.store.deadline(b"k")
    expected = Error(b"ERR value is out of range, must be positive")
    assert run(engine, name, b"k", raw) == expected
    assert run(engine, b"LRANGE", b"k", b"0", b"-1") == Array((Bulk(b"a"), Bulk(b"b")))
    assert (engine.store.key_revision(b"k"), engine.store.deadline(b"k")) == before
    assert run(engine, name, b"missing", raw) == expected


@pytest.mark.parametrize("kind", ["missing", "string", "hash"])
@pytest.mark.parametrize("option,raw,message", [
    (b"PX", b"0", b"ERR invalid expire time in 'getex' command"),
    (b"PX", b"bad", b"ERR value is not an integer or out of range"),
    (b"PX", b"9223372036854775808", b"ERR value is not an integer or out of range"),
    (b"EX", b"9223372036854775807", b"ERR invalid expire time in 'getex' command"),
])
def test_getex_type_and_missing_precede_numeric_expiration_validation(
    kind: str, option: bytes, raw: bytes, message: bytes,
) -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1000))
    if kind == "string":
        run(engine, b"SET", b"k", b"old")
    elif kind == "hash":
        run(engine, b"HSET", b"k", b"f", b"v")
    if kind != "missing":
        run(engine, b"PEXPIRE", b"k", b"100")
    before = engine.store.key_revision(b"k"), engine.store.deadline(b"k")
    expected: Reply = Bulk(None) if kind == "missing" else Error(
        b"WRONGTYPE Operation against a key holding the wrong kind of value"
        if kind == "hash" else message)
    assert run(engine, b"GETEX", b"k", option, raw) == expected
    assert (engine.store.key_revision(b"k"), engine.store.deadline(b"k")) == before
    if kind == "string":
        assert run(engine, b"GET", b"k") == Bulk(b"old")
    elif kind == "hash":
        assert run(engine, b"HGETALL", b"k") == Array((Bulk(b"f"), Bulk(b"v")))
    else:
        assert engine.store.physical_count == 0
    for invalid in ((b"PX",), (b"BOGUS",), (b"PX", b"1", b"PERSIST")):
        assert run(engine, b"GETEX", b"k", *invalid) == Error(b"ERR syntax error")
    assert (engine.store.key_revision(b"k"), engine.store.deadline(b"k")) == before
