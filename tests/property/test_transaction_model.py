"""Ordered batch effects versus a tiny public string model, including runtime errors."""
from hypothesis import given, settings
from hypothesis import strategies as st

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple


@settings(max_examples=100, derandomize=True, deadline=None)
@given(st.lists(st.tuples(st.sampled_from(["set", "get", "delete", "incr", "bad"]),
                         st.integers(-100, 100)), max_size=50))
def test_exec_effects_match_model(operations: list[tuple[str, int]]) -> None:
    engine = Executor(ServerConfig())
    expected: list[Reply] = []
    value: bytes | None = None
    assert engine.execute([b"MULTI"]).reply == Simple(b"OK")
    for operation, argument in operations:
        if operation == "set":
            value = str(argument).encode()
            command, result = [b"SET", b"\xff\x00", value], Simple(b"OK")
        elif operation == "get":
            command, result = [b"GET", b"\xff\x00"], Bulk(value)
        elif operation == "delete":
            command, result = [b"DEL", b"\xff\x00"], Integer(int(value is not None))
            value = None
        elif operation == "incr":
            number = (0 if value is None else int(value)) + argument
            value = str(number).encode()
            command = [b"INCRBY", b"\xff\x00", str(argument).encode()]
            result = Integer(number)
        else:
            command = [b"INCRBY", b"\xff\x00", b"bad"]
            result = Error(b"ERR value is not an integer or out of range")
        assert engine.execute(command).reply == Simple(b"QUEUED")
        expected.append(result)
    assert engine.store.physical_count == 0
    assert engine.execute([b"EXEC"]).reply == Array(tuple(expected))
    assert engine.execute([b"GET", b"\xff\x00"]).reply == Bulk(value)
