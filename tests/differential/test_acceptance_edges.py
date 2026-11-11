"""Additional mandatory M02-M05 comparisons; no internal revision oracle or M06."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from _support.acceptance_cases import (
    TRACES,
    TTLRange,
    assert_ttl_window,
)
from _support.comparison import Pair, peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.protocol import (
    Array,
    Bulk,
    Error,
    Integer,
    Reply,
    Simple,
)
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]








@pytest_asyncio.fixture
async def pair(reference_port: int) -> AsyncIterator[Pair]:
    server = await start_server(ServerConfig(port=0))
    port = server.sockets[0].getsockname()[1]
    async with server, peer(port) as local, peer(reference_port) as oracle:
        assert await oracle.call(b"FLUSHDB") == Simple(b"OK")
        yield Pair(local, oracle, server)


@pytest.mark.parametrize("name", TRACES)
async def test_additional_contract_trace(pair: Pair, name: str) -> None:
    for command, expected in TRACES[name]:
        await pair.check(command, expected)


def time_ms(reply: Reply) -> int:
    assert isinstance(reply, Array) and reply.payload is not None and len(reply.payload) == 2
    seconds, micros = reply.payload
    assert isinstance(seconds, Bulk) and seconds.payload is not None
    assert isinstance(micros, Bulk) and micros.payload is not None
    return int(seconds.payload) * 1000 + int(micros.payload) // 1000


@pytest.mark.parametrize("remaining", [499, 500, 1499, 1500])
async def test_ttl_precision_with_redis_time_brackets(reference_port: int, remaining: int) -> None:
    async with peer(reference_port) as oracle:
        assert await oracle.call(b"FLUSHDB") == Simple(b"OK")
        stamp = time_ms(await oracle.call(b"TIME"))
        clock = ManualClock(stamp)
        server = await start_server(ServerConfig(port=0), clock=clock)
        async with server, peer(server.sockets[0].getsockname()[1]) as local:
            deadline = stamp + 10_000 + remaining
            for target in (local, oracle):
                assert await target.call(b"SET", b"k", b"v") == Simple(b"OK")
                assert await target.call(b"PEXPIREAT", b"k", str(deadline).encode()) == Integer(1)
            first, ttl, pttl, last = await oracle.pipeline([
                (b"TIME",), (b"TTL", b"k"), (b"PTTL", b"k"), (b"TIME",)])
            before, after = time_ms(first), time_ms(last)
            assert_ttl_window(deadline, before, after, ttl, pttl)
            # Exact local boundary assertions use the injectable clock; reference
            # reads are checked against their own measured command-time bracket.
            clock.milliseconds = deadline - remaining
            assert await local.call(b"PTTL", b"k") == Integer(remaining)
            assert await local.call(b"TTL", b"k") == Integer((remaining + 500) // 1000)
            clock.milliseconds = before
            assert await local.call(b"PTTL", b"k") == Integer(deadline - before)
            assert await local.call(b"GET", b"k") == await oracle.call(b"GET", b"k") == Bulk(b"v")


async def test_natural_active_expiry_without_key_reads(pair: Pair) -> None:
    info = await pair.oracle.call(b"INFO", b"stats")
    assert isinstance(info, Bulk) and info.payload is not None
    baseline = info_field(info.payload, b"expired_keys")
    deadline = time_ms(await pair.oracle.call(b"TIME")) + 2000
    for number in range(4):
        key = str(number).encode()
        await pair.check((b"SET", key, b"v"), Simple(b"OK"))
        await pair.check((b"PEXPIREAT", key, str(deadline).encode()), Integer(1))
    # INFO stats and physical_count do not touch or passively expire these keys.
    async with asyncio.timeout(8):
        while True:
            stats = await pair.oracle.call(b"INFO", b"stats")
            assert isinstance(stats, Bulk) and stats.payload is not None
            if (info_field(stats.payload, b"expired_keys") >= baseline + 4
                    and pair.server.executor.store.physical_count == 0):
                break
            await asyncio.sleep(0.01)
    for number in range(4):
        key = str(number).encode()
        await pair.check((b"TTL", key), Integer(-2))
        await pair.check((b"PTTL", key), Integer(-2))
        await pair.check((b"GET", key), Bulk(None))


def info_field(payload: bytes, name: bytes) -> int:
    fields = dict(line.split(b":", 1) for line in payload.splitlines() if b":" in line)
    return int(fields[name])


CREATIONS = {
    "string": ((b"SET", b"k", b"old"), (b"GET", b"k")),
    "hash": ((b"HSET", b"k", b"f", b"v"), (b"HGETALL", b"k")),
    "list": ((b"RPUSH", b"k", b"a", b"b"), (b"LRANGE", b"k", b"0", b"-1")),
    "set": ((b"SADD", b"k", b"a", b"b"), (b"SMEMBERS", b"k")),
    "zset": ((b"ZADD", b"k", b"1", b"a", b"2", b"b"),
             (b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES")),
}
OPERATIONS = {
    "string": [(b"GET", b"k"), (b"GETEX", b"k", b"PERSIST"), (b"STRLEN", b"k"),
               (b"APPEND", b"k", b"v"), (b"INCR", b"k"), (b"DECR", b"k"),
               (b"INCRBY", b"k", b"1")],
    "hash": [(b"HSET", b"k", b"f", b"v"), (b"HGET", b"k", b"f"),
             (b"HDEL", b"k", b"f"), (b"HEXISTS", b"k", b"f"),
             (b"HGETALL", b"k"), (b"HLEN", b"k")],
    "list": [(b"LPUSH", b"k", b"v"), (b"RPUSH", b"k", b"v"),
             (b"LPOP", b"k"), (b"RPOP", b"k", b"1"),
             (b"LRANGE", b"k", b"0", b"-1"), (b"LLEN", b"k")],
    "set": [(b"SADD", b"k", b"v"), (b"SREM", b"k", b"v"),
            (b"SISMEMBER", b"k", b"v"), (b"SMEMBERS", b"k"), (b"SCARD", b"k")],
    "zset": [(b"ZADD", b"k", b"1", b"v"), (b"ZREM", b"k", b"v"),
             (b"ZSCORE", b"k", b"v"), (b"ZRANGE", b"k", b"0", b"-1"), (b"ZCARD", b"k")],
}


@pytest.mark.parametrize("kind", CREATIONS)
async def test_cross_type_matrix_preserves_value_and_expiry(pair: Pair, kind: str) -> None:
    create, snapshot = CREATIONS[kind]
    await pair.check(create)
    await pair.check((b"PEXPIRE", b"k", b"60000"), Integer(1))
    original = await pair.check(snapshot)
    wrongtype = Error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    for family, commands in OPERATIONS.items():
        if family == kind:
            continue
        for command in commands:
            await pair.check(command, wrongtype)
            await pair.check(snapshot, original)
            await pair.check((b"TYPE", b"k"), Simple(kind.encode()))
            await pair.check((b"PTTL", b"k"), TTLRange(60_000))
    if kind != "string":
        await pair.check((b"MGET", b"k", b"missing"), Array((Bulk(None), Bulk(None))))
    await pair.check((b"SET", b"k", b"new"), Simple(b"OK"))
    await pair.check((b"GET", b"k"), Bulk(b"new"))
    await pair.check((b"PTTL", b"k"), Integer(-1))


@pytest.mark.parametrize("kind,command", [
    ("hash", (b"INCRBY", b"k", b"bad")), ("string", (b"LPOP", b"k", b"bad")),
    ("string", (b"LRANGE", b"k", b"bad", b"-1")),
    ("hash", (b"ZADD", b"k", b"NaN", b"a")),
    ("hash", (b"GETEX", b"k", b"PX", b"0")),
    ("list", (b"HSET", b"k", b"f", b"v", b"dangling")),
])
async def test_error_precedence_has_unchanged_post_state(
    pair: Pair, kind: str, command: tuple[bytes, ...],
) -> None:
    create, snapshot = CREATIONS[kind]
    await pair.check(create)
    await pair.check((b"PEXPIRE", b"k", b"60000"), Integer(1))
    original = await pair.check(snapshot)
    # The actual Redis error bytes determine precedence, not a locally invented rule.
    assert isinstance(await pair.check(command), Error)
    await pair.check(snapshot, original)
    await pair.check((b"PTTL", b"k"), TTLRange(60_000))


@pytest.mark.parametrize("raw,formatted", [
    (b".5", b"0.5"), (b"1.", b"1"), (b"+01", b"1"), (b"1e2", b"100"),
    (b"-0.0e+10", b"0"), (b"INF", b"inf"), (b"+Infinity", b"inf"),
    (b"-Infinity", b"-inf"),
    (b"1.7976931348623157e308", b"1.7976931348623157e+308"),
    (b"-1.7976931348623157e308", b"-1.7976931348623157e+308"),
    (b"2.2250738585072014e-308", b"2.2250738585072014e-308"),
    (b"-5e-324", b"-5e-324"),
    (b"9007199254740995", b"9007199254740996"),
])
async def test_extended_score_spelling_and_precision(
    pair: Pair, raw: bytes, formatted: bytes,
) -> None:
    await pair.check((b"ZADD", b"k", raw, b"\x00\xff"), Integer(1))
    await pair.check((b"ZSCORE", b"k", b"\x00\xff"), Bulk(formatted))
    await pair.check((b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"),
                     Array((Bulk(b"\x00\xff"), Bulk(formatted))))


@pytest.mark.parametrize("raw", [b"+nan", b"-nan", b"-1e999", b"-1e-999", b"0x1p2", b"1e"])
async def test_extended_score_inputs_preserve_atomicity(pair: Pair, raw: bytes) -> None:
    await pair.check((b"ZADD", b"k", b"1", b"a"), Integer(1))
    await pair.check((b"PEXPIRE", b"k", b"60000"), Integer(1))
    # Independent Redis 7.4.12 traces establish that this hexadecimal input is
    # valid. Retain the case and verify both updates commit with expiry intact.
    hexadecimal = raw == b"0x1p2"
    await pair.check((b"ZADD", b"k", b"2", b"a", raw, b"b"),
                     Integer(1) if hexadecimal else Error(b"ERR value is not a valid float"))
    state = ((Bulk(b"a"), Bulk(b"2"), Bulk(b"b"), Bulk(b"4")) if hexadecimal
             else (Bulk(b"a"), Bulk(b"1")))
    await pair.check((b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"),
                     Array(state))
    await pair.check((b"PTTL", b"k"), TTLRange(60_000))


@pytest.mark.parametrize("large", [False, True], ids=["compact", "large"])
async def test_rounded_score_ties_and_binary_prefix_order(pair: Pair, large: bool) -> None:
    members = [b"", b"a", b"a\x00", b"ab", b"\xff", b"\xff\x00"]
    if large:
        members += [b"member" * 12 + str(number).encode() for number in range(150)]
    pairs = tuple(part for member in reversed(members) for part in (b"9007199254740993", member))
    await pair.check((b"ZADD", b"k", *pairs), Integer(len(members)))
    expected = Array(tuple(Bulk(part) for member in sorted(members)
                           for part in (member, b"9007199254740992")))
    await pair.check((b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"), expected)
    await pair.check((b"ZADD", b"k", b"-inf", b"\xff", b"inf", b"a"), Integer(0))
    await pair.check((b"ZRANGE", b"k", b"0", b"0"), Array((Bulk(b"\xff"),)))
    await pair.check((b"ZRANGE", b"k", b"-1", b"-1"), Array((Bulk(b"a"),)))


async def test_concurrent_score_pair_mutations_are_atomic_on_both_servers(pair: Pair) -> None:
    ports = (pair.server.sockets[0].getsockname()[1],
             pair.oracle.writer.get_extra_info("peername")[1])
    for port in ports:
        async with peer(port) as setup:
            assert await setup.call(b"ZADD", b"k", b"0", b"a", b"0", b"b") == Integer(2)
        barrier = asyncio.Barrier(3)

        async def update(score: bytes, port: int, barrier: asyncio.Barrier) -> None:
            async with peer(port) as connection:
                await asyncio.wait_for(barrier.wait(), 5)
                for _ in range(20):
                    assert await connection.call(b"ZADD", b"k", score, b"a", score, b"b") == (
                        Integer(0))

        async def observe(port: int, barrier: asyncio.Barrier) -> None:
            async with peer(port) as connection:
                await asyncio.wait_for(barrier.wait(), 5)
                for _ in range(40):
                    reply = await connection.call(b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES")
                    assert isinstance(reply, Array) and reply.payload is not None
                    assert reply.payload[::2] == (Bulk(b"a"), Bulk(b"b"))
                    assert reply.payload[1] == reply.payload[3]
                    assert reply.payload[1] in (Bulk(b"0"), Bulk(b"1"), Bulk(b"2"))

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(update(b"1", port, barrier))
            tasks.create_task(update(b"2", port, barrier))
            tasks.create_task(observe(port, barrier))
        async with peer(port) as final:
            assert await final.call(b"ZCARD", b"k") == Integer(2)
