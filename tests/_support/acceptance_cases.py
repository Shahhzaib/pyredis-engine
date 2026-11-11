"""Shared expected traces; local rehearsal is distinct from reference acceptance."""
from __future__ import annotations

from dataclasses import dataclass

from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple

OK = Simple(b"OK")
INVALID_FLOAT = Error(b"ERR value is not a valid float")
INVALID_INT = Error(b"ERR value is not an integer or out of range")


@dataclass(frozen=True)
class TTLRange:
    maximum: int


Expected = Reply | TTLRange
Step = tuple[tuple[bytes, ...], Expected]


TRACES: dict[str, tuple[Step, ...]] = {
    "TTL_STATES": (
        ((b"TTL", b"missing"), Integer(-2)), ((b"PTTL", b"missing"), Integer(-2)),
        ((b"SET", b"k", b"v"), OK), ((b"TTL", b"k"), Integer(-1)),
        ((b"PTTL", b"k"), Integer(-1)), ((b"PEXPIRE", b"k", b"60000"), Integer(1)),
        ((b"TTL", b"k"), TTLRange(60)), ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"PERSIST", b"k"), Integer(1)), ((b"PERSIST", b"k"), Integer(0)),
        ((b"TTL", b"k"), Integer(-1)), ((b"PERSIST", b"missing"), Integer(0)),
    ),
    "EXPIRE_MISSING_AND_PAST": tuple(
        step
        for name, invalid in ((b"EXPIRE", b"-1"), (b"PEXPIRE", b"0"),
                              (b"EXPIREAT", b"1"), (b"PEXPIREAT", b"1"))
        for step in (
            ((name, b"missing", b"1"), Integer(0)),
            ((b"SET", b"k", b"old"), OK), ((name, b"k", invalid), Integer(1)),
            ((b"PTTL", b"k"), Integer(-2)), ((b"GET", b"k"), Bulk(None)),
        )
    ),
    "SET_NX_XX": (
        ((b"SET", b"k", b"old", b"NX", b"PX", b"60000"), OK),
        ((b"SET", b"k", b"bad", b"NX", b"EX", b"1"), Bulk(None)),
        ((b"GET", b"k"), Bulk(b"old")), ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"SET", b"missing", b"bad", b"XX", b"PX", b"1"), Bulk(None)),
        ((b"EXISTS", b"missing"), Integer(0)),
        ((b"SET", b"k", b"new", b"XX", b"KEEPTTL"), OK),
        ((b"GET", b"k"), Bulk(b"new")), ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"SET", b"k", b"plain"), OK), ((b"PTTL", b"k"), Integer(-1)),
    ),
    "SET_EXPIRED_CONDITIONS": (
        ((b"SET", b"k", b"old"), OK), ((b"PEXPIREAT", b"k", b"1"), Integer(1)),
        ((b"SET", b"k", b"bad", b"XX"), Bulk(None)),
        ((b"SET", b"k", b"new", b"NX", b"KEEPTTL"), OK),
        ((b"PTTL", b"k"), Integer(-1)), ((b"GET", b"k"), Bulk(b"new")),
    ),
    "GETEX_PRESERVE_AND_PERSIST": (
        ((b"SET", b"k", b"\x00\r\n\xff", b"PX", b"60000"), OK),
        ((b"GETEX", b"k"), Bulk(b"\x00\r\n\xff")),
        ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"GETEX", b"k", b"PERSIST"), Bulk(b"\x00\r\n\xff")),
        ((b"PTTL", b"k"), Integer(-1)),
        ((b"GETEX", b"missing", b"EX", b"1"), Bulk(None)),
        ((b"EXISTS", b"missing"), Integer(0)),
    ),
    "GETEX_PAST_ABSOLUTE": (
        ((b"SET", b"k", b"old"), OK), ((b"GETEX", b"k", b"PXAT", b"1"), Bulk(b"old")),
        ((b"GET", b"k"), Bulk(None)), ((b"PTTL", b"k"), Integer(-2)),
        ((b"SET", b"k", b"again"), OK), ((b"GETEX", b"k", b"EXAT", b"1"), Bulk(b"again")),
        ((b"TYPE", b"k"), Simple(b"none")),
    ),
    "INVALID_OPTIONS_RETAIN_VALUE_AND_TTL": (
        ((b"SET", b"k", b"old", b"PX", b"60000"), OK),
        ((b"SET", b"k", b"bad", b"NX", b"XX"), Error(b"ERR syntax error")),
        ((b"GETEX", b"k", b"PERSIST", b"EX", b"1"), Error(b"ERR syntax error")),
        ((b"GETEX", b"k", b"PX", b"0"), Error(b"ERR invalid expire time in 'getex' command")),
        ((b"GET", b"k"), Bulk(b"old")), ((b"PTTL", b"k"), TTLRange(60_000)),
    ),
    "SET_GETEX_REPEATED_OPTIONS": (
        ((b"SET", b"k", b"old", b"NX", b"NX", b"PX", b"1", b"PX", b"60000"), OK),
        ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"GETEX", b"k", b"PX", b"1", b"PX", b"60000"), Bulk(b"old")),
        ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"GETEX", b"k", b"EX", b"1", b"PX", b"1"), Error(b"ERR syntax error")),
        ((b"SET", b"k", b"bad", b"KEEPTTL", b"EX", b"1"), Error(b"ERR syntax error")),
        ((b"GET", b"k"), Bulk(b"old")), ((b"PTTL", b"k"), TTLRange(60_000)),
    ),
    "M02_INPLACE_TTL": (
        ((b"SET", b"k", b"1", b"PX", b"60000"), OK), ((b"INCR", b"k"), Integer(2)),
        ((b"APPEND", b"k", b"x"), Integer(2)), ((b"GET", b"k"), Bulk(b"2x")),
        ((b"PTTL", b"k"), TTLRange(60_000)), ((b"INCR", b"k"), INVALID_INT),
        ((b"GET", b"k"), Bulk(b"2x")), ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"MSET", b"k", b"new", b"other", b"new"), OK),
        ((b"PTTL", b"k"), Integer(-1)),
    ),
    "HSET_PAIR_FAILURE_ATOMIC": (
        ((b"HSET", b"k", b"a", b"old"), Integer(1)),
        ((b"PEXPIRE", b"k", b"60000"), Integer(1)),
        ((b"HSET", b"k", b"a", b"bad", b"dangling"),
         Error(b"ERR wrong number of arguments for 'hset' command")),
        ((b"HGETALL", b"k"), Array((Bulk(b"a"), Bulk(b"old")))),
        ((b"PTTL", b"k"), TTLRange(60_000)),
    ),
    "COUNTED_POP_FAILURE_ATOMIC": (
        ((b"RPUSH", b"k", b"a", b"b"), Integer(2)),
        ((b"PEXPIRE", b"k", b"60000"), Integer(1)),
        ((b"LPOP", b"k", b"bad"), Error(b"ERR value is out of range, must be positive")),
        ((b"RPOP", b"k", b"-1"), Error(b"ERR value is out of range, must be positive")),
        ((b"LRANGE", b"k", b"0", b"-1"), Array((Bulk(b"a"), Bulk(b"b")))),
        ((b"PTTL", b"k"), TTLRange(60_000)),
    ),
    "ZADD_DUPLICATE_FAILURE_ATOMIC": (
        ((b"ZADD", b"k", b"1", b"a", b"2", b"b"), Integer(2)),
        ((b"PEXPIRE", b"k", b"60000"), Integer(1)),
        ((b"ZADD", b"k", b"3", b"a", b"NaN", b"a", b"4", b"a"), INVALID_FLOAT),
        ((b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"),
         Array((Bulk(b"a"), Bulk(b"1"), Bulk(b"b"), Bulk(b"2")))),
        ((b"PTTL", b"k"), TTLRange(60_000)),
        ((b"ZADD", b"missing", b"1", b"a", b"NaN", b"b"), INVALID_FLOAT),
        ((b"EXISTS", b"missing"), Integer(0)),
    ),
}


def normalize(reply: Reply, command: tuple[bytes, ...]) -> object:
    if command[0].upper() not in (b"HGETALL", b"SMEMBERS") or not isinstance(reply, Array):
        return reply
    assert reply.payload is not None
    values: list[bytes] = []
    for item in reply.payload:
        assert isinstance(item, Bulk) and item.payload is not None
        values.append(item.payload)
    if command[0].upper() == b"HGETALL":
        assert len(values) % 2 == 0
        assert len(set(values[::2])) == len(values) // 2
        return sorted(zip(values[::2], values[1::2], strict=True))
    assert len(values) == len(set(values))
    return sorted(values)


def assert_expected(actual: Reply, expected: Expected, command: tuple[bytes, ...]) -> None:
    if isinstance(expected, TTLRange):
        assert isinstance(actual, Integer) and 0 <= actual.payload <= expected.maximum, command
    else:
        assert normalize(actual, command) == normalize(expected, command), command


def assert_ttl_window(deadline: int, before: int, after: int, ttl: Reply, pttl: Reply) -> None:
    assert before <= after < deadline, "TTL sampling missed its bounded observation window"
    assert isinstance(ttl, Integer) and isinstance(pttl, Integer)
    assert deadline - after <= pttl.payload <= deadline - before
    assert (deadline - after + 500) // 1000 <= ttl.payload <= (deadline - before + 500) // 1000
