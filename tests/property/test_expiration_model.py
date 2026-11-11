"""Generated expiry/index checks against simple independent dictionaries."""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.expiration import ExpirationIndex
from pyredis_engine.protocol import Bulk, Integer, Simple

options = settings(max_examples=150, derandomize=True, database=None, deadline=None)


@options
@given(st.lists(st.tuples(st.sampled_from(["set", "remove"]), st.binary(max_size=5),
                          st.integers(-1000, 1000)), max_size=200))
def test_index_matches_dictionary(operations: list[tuple[str, bytes, int]]) -> None:
    index = ExpirationIndex()
    expected: dict[bytes, int] = {}
    for operation, key, deadline in operations:
        if operation == "set":
            expected[key] = deadline
            index.set(key, deadline)
        else:
            expected.pop(key, None)
            index.discard(key)
        assert len(index) == len(expected)
        assert index.first() == min(((value, key) for key, value in expected.items()), default=None)
        for current, value in expected.items():
            assert index.deadline(current) == value


@options
@given(st.lists(st.tuples(st.sampled_from(["set", "expire", "persist", "advance", "sweep"]),
                          st.sampled_from([b"", b"a", b"\xff\x00"]),
                          st.integers(-5, 20)), max_size=100))
def test_expiration_traces(operations: list[tuple[str, bytes, int]]) -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(expiry_batch_size=128), clock=clock)
    values: dict[bytes, bytes] = {}
    deadlines: dict[bytes, int] = {}
    for operation, key, number in operations:
        if operation == "advance":
            clock.advance(number)
        else:
            # Oracle performs lazy removal for the touched key, or all for a sweep.
            touched = list(values) if operation == "sweep" else [key]
            for current in touched:
                if current in deadlines and clock.now_ms() > deadlines[current]:
                    del values[current]
                    del deadlines[current]
            if operation == "set":
                assert engine.execute([b"SET", key, b"v"]).reply == Simple(b"OK")
                values[key] = b"v"
                deadlines.pop(key, None)
            elif operation == "expire":
                assert engine.execute([b"PEXPIRE", key, str(number).encode()]).reply == Integer(
                    int(key in values))
                if key in values:
                    if number <= 0:
                        del values[key]
                        deadlines.pop(key, None)
                    else:
                        deadlines[key] = clock.now_ms() + number
            elif operation == "persist":
                assert engine.execute([b"PERSIST", key]).reply == Integer(int(key in deadlines))
                deadlines.pop(key, None)
            else:
                engine.expire_batch()
        # Explicitly read keys so both models process the same passive removals.
        for current in [b"", b"a", b"\xff\x00"]:
            if current in deadlines and clock.now_ms() > deadlines[current]:
                del values[current]
                del deadlines[current]
            assert engine.execute([b"GET", current]).reply == Bulk(values.get(current))
        assert engine.store.expiration_count == len(deadlines)
        assert engine.store.physical_count == engine.store.revision_count == len(values)
