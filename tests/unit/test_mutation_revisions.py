"""Internal hooks are unit invariants, never official Redis comparisons."""
from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import Store


def test_internal_change_hooks_are_separate_from_reference_comparisons() -> None:
    clock = ManualClock(1000)
    changes: list[tuple[bytes, int]] = []
    store = Store(clock=clock, on_change=lambda key, revision: changes.append((key, revision)))
    engine = Executor(ServerConfig(), store=store)
    steps = [
        ([b"SET", b"s", b"v"], 1), ([b"SET", b"s", b"v"], 1),
        ([b"HSET", b"h", b"f", b"v"], 1), ([b"HSET", b"h", b"f", b"v"], 1),
        ([b"SADD", b"set", b"a"], 1), ([b"SADD", b"set", b"a"], 0),
        ([b"ZADD", b"z", b"1", b"a"], 1), ([b"ZADD", b"z", b"1", b"a"], 0),
        ([b"PEXPIRE", b"z", b"10"], 1), ([b"PEXPIRE", b"z", b"10"], 1),
        ([b"PERSIST", b"z"], 1), ([b"PERSIST", b"z"], 0),
        ([b"ZREM", b"z", b"absent"], 0), ([b"ZREM", b"z", b"a"], 1),
    ]
    for command, count in steps:
        before = len(changes)
        engine.execute(command)
        assert len(changes) - before == count, command
    assert [revision for _, revision in changes] == list(range(1, len(changes) + 1))
    assert store.key_revision(b"z") == 0
