"""Deterministic models use unordered maps/sets, independent of the ranked index."""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Integer

deterministic = settings(max_examples=150, derandomize=True, database=None, deadline=None)
binary = st.binary(max_size=8)


@deterministic
@given(st.lists(st.tuples(st.sampled_from([b"SADD", b"SREM"]),
                         st.lists(binary, min_size=1, max_size=5)), max_size=100))
def test_set_model(operations: list[tuple[bytes, list[bytes]]]) -> None:
    engine = Executor(ServerConfig())
    state: set[bytes] = set()
    for name, members in operations:
        before = len(state)
        if name == b"SADD":
            state.update(members)
        else:
            state.difference_update(members)
        assert engine.execute([name, b"k", *members]).reply == Integer(abs(len(state) - before))
        assert engine.execute([b"SCARD", b"k"]).reply == Integer(len(state))
        assert engine.execute([b"SMEMBERS", b"k"]).reply == Array(
            tuple(Bulk(member) for member in sorted(state)))
        for member in members:
            assert engine.execute([b"SISMEMBER", b"k", member]).reply == Integer(
                int(member in state))
        value = engine.store.get_set(b"k")
        assert (value.byte_size if value is not None else 0) == sum(map(len, state))
        assert engine.store.physical_count == int(bool(state))


@deterministic
@given(st.lists(st.tuples(st.sampled_from([b"ZADD", b"ZREM"]),
                         st.lists(st.tuples(binary, st.integers(-20, 20)), min_size=1, max_size=5),
                         st.integers(-30, 30), st.integers(-30, 30)), max_size=100))
def test_sorted_set_model(
    operations: list[tuple[bytes, list[tuple[bytes, int]], int, int]],
) -> None:
    engine = Executor(ServerConfig())
    state: dict[bytes, int] = {}
    for name, pairs, start, stop in operations:
        before = len(state)
        if name == b"ZADD":
            state.update(pairs)
            args = [part for member, score in pairs for part in (str(score).encode(), member)]
        else:
            for member, _ in pairs:
                state.pop(member, None)
            args = [member for member, _ in pairs]
        assert engine.execute([name, b"k", *args]).reply == Integer(abs(len(state) - before))
        # Independently sort member keys and derive scores from the model.
        order = sorted(state, key=lambda member: (state[member], member))
        first = max(0, start if start >= 0 else len(order) + start)
        last = min(len(order) - 1, stop if stop >= 0 else len(order) + stop)
        selected = order[first:last + 1] if first <= last else []
        command = [b"ZRANGE", b"k", str(start).encode(), str(stop).encode()]
        assert engine.execute(command).reply == Array(
            tuple(Bulk(member) for member in selected))
        assert engine.execute([b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"]).reply == Array(
            tuple(Bulk(part) for member in order for part in (member, str(state[member]).encode())))
        for member, _ in pairs:
            score = state.get(member)
            assert engine.execute([b"ZSCORE", b"k", member]).reply == Bulk(
                None if score is None else str(score).encode())
        assert engine.execute([b"ZCARD", b"k"]).reply == Integer(len(state))
        value = engine.store.get_sorted_set(b"k")
        if value is not None:
            assert value.byte_size == sum(len(member) + 8 for member in state)
            assert len(value.ranked) == len(value.scores) == len(state)
            assert value.ranked == [(float(state[member]), member) for member in order]
        assert engine.store.physical_count == int(bool(state))
