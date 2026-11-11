"""Deterministic generated M02 traces against an independent bytes dictionary."""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple

KEYS = (b"", b"k", b"K", b"\x00\xff")
key = st.sampled_from(KEYS)
value = st.one_of(st.binary(max_size=16), st.sampled_from(
    [b"0", b"1", b"-1", b"01", b"-0", b"9223372036854775807", b"-9223372036854775808"]))
keys = st.lists(key, min_size=1, max_size=4)
commands = st.one_of(
    st.tuples(st.just(b"SET"), key, value),
    st.tuples(st.just(b"APPEND"), key, value),
    st.tuples(st.sampled_from([b"GET", b"TYPE", b"STRLEN", b"INCR", b"DECR"]), key),
    st.tuples(st.just(b"INCRBY"), key, st.integers(-20, 20).map(lambda n: str(n).encode())),
    st.tuples(st.sampled_from([b"DEL", b"EXISTS", b"MGET"]), keys).map(
        lambda pair: (pair[0], *pair[1])),
    st.lists(st.tuples(key, value), min_size=1, max_size=3).map(
        lambda pairs: (b"MSET", *(part for pair in pairs for part in pair))),
    st.sampled_from([(b"DBSIZE",), (b"FLUSHDB",)]),
)


def model(state: dict[bytes, bytes], command: tuple[bytes, ...]) -> Reply:
    name, *args = command
    if name in (b"SET", b"MSET"):
        state.update(zip(args[::2], args[1::2], strict=True))
        return Simple(b"OK")
    if name == b"GET":
        return Bulk(state.get(args[0]))
    if name == b"MGET":
        return Array(tuple(Bulk(state.get(item)) for item in args))
    if name == b"TYPE":
        return Simple(b"string" if args[0] in state else b"none")
    if name == b"EXISTS":
        return Integer(sum(item in state for item in args))
    if name == b"DEL":
        before = len(state)
        for item in args:
            state.pop(item, None)
        return Integer(before - len(state))
    if name == b"APPEND":
        state[args[0]] = state.get(args[0], b"") + args[1]
        return Integer(len(state[args[0]]))
    if name == b"STRLEN":
        return Integer(len(state.get(args[0], b"")))
    if name == b"DBSIZE":
        return Integer(len(state))
    if name == b"FLUSHDB":
        state.clear()
        return Simple(b"OK")
    raw = state.get(args[0], b"0")
    try:
        number = int(raw)
    except ValueError:
        return Error(b"ERR value is not an integer or out of range")
    if str(number).encode() != raw or not -(2**63) <= number < 2**63:
        return Error(b"ERR value is not an integer or out of range")
    delta = int(args[1]) if name == b"INCRBY" else 1 if name == b"INCR" else -1
    result = number + delta
    if not -(2**63) <= result < 2**63:
        return Error(b"ERR increment or decrement would overflow")
    state[args[0]] = str(result).encode()
    return Integer(result)


@settings(max_examples=150, derandomize=True, database=None, deadline=None)
@given(st.lists(commands, min_size=1, max_size=60))
def test_generated_string_traces_match_model(trace: list[tuple[bytes, ...]]) -> None:
    engine = Executor(ServerConfig())
    state: dict[bytes, bytes] = {}
    for command in trace:
        assert engine.execute(list(command)).reply == model(state, command), command
        assert engine.execute([b"DBSIZE"]).reply == Integer(len(state))
        assert engine.execute([b"MGET", *KEYS]).reply == Array(
            tuple(Bulk(state.get(item)) for item in KEYS))
