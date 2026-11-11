"""Generated list/dict models independent of the engine's mutable containers."""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Integer, Reply

deterministic = settings(max_examples=150, derandomize=True, database=None, deadline=None)
binary = st.binary(max_size=8)


@deterministic
@given(st.lists(st.one_of(
    st.tuples(st.sampled_from([b"LPUSH", b"RPUSH"]), st.lists(binary, min_size=1, max_size=5)),
    st.tuples(st.sampled_from([b"LPOP", b"RPOP"]), st.one_of(st.none(), st.integers(0, 10))),
    st.tuples(st.just(b"LRANGE"), st.tuples(st.integers(-30, 30), st.integers(-30, 30))),
), max_size=100))
def test_list_model(operations: list[tuple[bytes, object]]) -> None:
    engine = Executor(ServerConfig())
    state: list[bytes] = []
    for name, argument in operations:
        if name in (b"LPUSH", b"RPUSH"):
            assert isinstance(argument, list)
            expected = Integer(len(state) + len(argument))
            command = [name, b"l", *argument]
            if name == b"LPUSH":
                state = list(reversed(argument)) + state
            else:
                state += argument
        elif name in (b"LPOP", b"RPOP"):
            command = [name, b"l"]
            if argument is not None:
                assert isinstance(argument, int)
                command.append(str(argument).encode())
            if not state:
                expected = Bulk(None) if argument is None else Array(None)
            elif argument is None:
                expected = Bulk(state.pop(0) if name == b"LPOP" else state.pop())
            else:
                count = min(argument, len(state))
                removed = [state.pop(0) if name == b"LPOP" else state.pop() for _ in range(count)]
                expected = Array(tuple(Bulk(value) for value in removed))
        else:
            assert isinstance(argument, tuple)
            start, stop = argument
            command = [name, b"l", str(start).encode(), str(stop).encode()]
            # Normalize to Python's exclusive-end slicing only after Redis clamping.
            first = max(0, start if start >= 0 else len(state) + start)
            last = min(len(state) - 1, stop if stop >= 0 else len(state) + stop)
            selected = state[first:last + 1] if first <= last else []
            expected = Array(tuple(Bulk(value) for value in selected))
        assert engine.execute(command).reply == expected
        assert engine.execute([b"LRANGE", b"l", b"0", b"-1"]).reply == Array(
            tuple(Bulk(value) for value in state))
        assert engine.execute([b"LLEN", b"l"]).reply == Integer(len(state))
        stored = engine.store.get_list(b"l")
        assert (stored.byte_size if stored is not None else 0) == sum(map(len, state))
        assert engine.store.physical_count == int(bool(state))


@deterministic
@given(st.lists(st.one_of(
    st.tuples(st.just(b"HSET"), st.lists(st.tuples(binary, binary), min_size=1, max_size=5)),
    st.tuples(st.just(b"HDEL"), st.lists(binary, min_size=1, max_size=5)),
    st.tuples(st.sampled_from([b"HGET", b"HEXISTS"]), binary),
), max_size=100))
def test_hash_model(operations: list[tuple[bytes, object]]) -> None:
    engine = Executor(ServerConfig())
    state: dict[bytes, bytes] = {}
    for name, argument in operations:
        if name == b"HSET":
            assert isinstance(argument, list)
            before = len(state)
            state.update(argument)
            expected: Reply = Integer(len(state) - before)
            command = [name, b"h", *(part for pair in argument for part in pair)]
        elif name == b"HDEL":
            assert isinstance(argument, list)
            before = len(state)
            for field in argument:
                state.pop(field, None)
            expected = Integer(before - len(state))
            command = [name, b"h", *argument]
        else:
            assert isinstance(argument, bytes)
            expected = (Bulk(state.get(argument)) if name == b"HGET"
                        else Integer(int(argument in state)))
            command = [name, b"h", argument]
        assert engine.execute(command).reply == expected
        assert engine.execute([b"HLEN", b"h"]).reply == Integer(len(state))
        assert engine.execute([b"HGETALL", b"h"]).reply == Array(
            tuple(Bulk(part) for pair in state.items() for part in pair))
        stored = engine.store.get_hash(b"h")
        assert (stored.byte_size if stored is not None else 0) == sum(
            len(field) + len(value) for field, value in state.items())
        assert engine.store.physical_count == int(bool(state))
