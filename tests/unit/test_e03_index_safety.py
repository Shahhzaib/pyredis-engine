from __future__ import annotations

import random

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.collections import HashValue, SetValue
from pyredis_engine.engine.executor import Executor
from pyredis_engine.engine.store import StoredValue
from pyredis_engine.ops.memory import AllocationRejected
from pyredis_engine.persistence.codec import Entry, Group
from pyredis_engine.persistence.errors import CapacityError
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple


@pytest.mark.parametrize("command,args", [(b"HSET", [b"f", b"v"]), (b"SADD", [b"f"])])
def test_index_allocation_failure_preserves_old_collection(
    monkeypatch: pytest.MonkeyPatch, command: bytes, args: list[bytes]
) -> None:
    from pyredis_engine.engine import store

    engine = Executor(ServerConfig())
    try:
        assert engine.execute([command, b"key", *args]).reply == Integer(1)
        previous = engine.store.physical_entry(b"key")
        before = engine.store.memory_bytes, engine.store.mutation_revision

        def fail(*args: object, **kwargs: object) -> None:
            raise MemoryError("injected scan index allocation")

        monkeypatch.setattr(store, "sorted", fail, raising=False)
        extra = [b"g", b"v"] if command == b"HSET" else [b"g"]
        result = engine.execute([command, b"key", *extra]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"OOM")
        assert engine.store.physical_entry(b"key") == previous
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
    finally:
        engine.close()


def test_restore_prepares_all_indexes_before_clearing_existing_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute([b"SET", b"safe", b"value"]).reply == Simple(b"OK")
        before = engine.store.memory_bytes, engine.store.mutation_revision
        prepare = engine.store.prepare_scan_index
        calls = 0

        def fail_second(value: StoredValue | None) -> tuple[bytes, ...] | None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise AllocationRejected
            return prepare(value)

        monkeypatch.setattr(engine.store, "prepare_scan_index", fail_second)
        group = Group(
            True,
            (Entry(b"h", HashValue({b"f": b"v"}, 2), None), Entry(b"s", SetValue({b"m"}, 1), None)),
        )
        with pytest.raises(CapacityError):
            engine.restore_group(group)
        assert engine.execute([b"GET", b"safe"]).reply == Bulk(b"value")
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
    finally:
        engine.close()


@pytest.mark.parametrize("command,scan", [(b"HSET", b"HSCAN"), (b"SADD", b"SSCAN")])
def test_large_collection_small_count_uses_logarithmic_seek(
    command: bytes,
    scan: bytes,
) -> None:
    class Counted(tuple[bytes, ...]):
        calls = 0

        def __getitem__(self, index: int) -> bytes:  # type: ignore[override]
            self.calls += 1
            return super().__getitem__(index)

    engine = Executor(ServerConfig())
    try:
        members = [f"{number:05}".encode() for number in range(4096)]
        args = [
            part
            for member in members
            for part in ((member, b"v") if command == b"HSET" else (member,))
        ]
        assert engine.execute([command, b"large", *args]).reply == Integer(4096)
        value, _ = engine.store.physical_entry(b"large")
        assert isinstance(value, (HashValue, SetValue))
        counted = Counted(value.scan_index)
        value.scan_index = counted
        result = engine.execute([scan, b"large", b"0", b"COUNT", b"1"]).reply
        assert isinstance(result, Array) and result.payload is not None
        cursor = result.payload[0]
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        counted.calls = 0
        result = engine.execute(
            [scan, b"large", cursor.payload, b"COUNT", b"1", b"MATCH", b"does-not-match*"]
        ).reply
        assert isinstance(result, Array) and result.payload is not None
        assert result.payload[1] == Array(())
        assert counted.calls <= 16
    finally:
        engine.close()


def test_exec_reply_overflow_restores_collection_and_its_index() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    try:
        assert engine.execute([b"HSET", b"h", b"a", b"v"]).reply == Integer(1)
        assert engine.execute([b"MULTI"]).reply == Simple(b"OK")
        engine.execute([b"HSET", b"h", b"b", b"v"])
        engine.execute([b"ECHO", b"x" * 120])
        result = engine.execute([b"EXEC"]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"EXECABORT")
        assert engine.execute([b"HSCAN", b"h", b"0"]).reply == Array(
            (Bulk(b"0"), Array((Bulk(b"a"), Bulk(b"v"))))
        )
    finally:
        engine.close()


@pytest.mark.parametrize(
    "command,initial,change",
    [
        (b"HSET", [b"a", b"1", b"b", b"2"], [b"a", b"new", b"c", b"3"]),
        (b"SADD", [b"a", b"b"], [b"b", b"c"]),
    ],
)
def test_mutation_prepares_index_from_delta_without_full_rebuild(
    monkeypatch: pytest.MonkeyPatch, command: bytes, initial: list[bytes], change: list[bytes]
) -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute([command, b"k", *initial]).reply == Integer(2)

        def fail(*args: object, **kwargs: object) -> None:
            raise AssertionError("ordinary writes must not rebuild the whole scan index")

        monkeypatch.setattr(engine.store, "prepare_scan_index", fail)
        assert engine.execute([command, b"k", *change]).reply == Integer(1)
        value, _ = engine.store.physical_entry(b"k")
        assert isinstance(value, (HashValue, SetValue))
        assert value.scan_index == (b"a", b"b", b"c")
        remove = b"HDEL" if command == b"HSET" else b"SREM"
        assert engine.execute([remove, b"k", b"b"]).reply == Integer(1)
        value, _ = engine.store.physical_entry(b"k")
        assert isinstance(value, (HashValue, SetValue))
        assert value.scan_index == (b"a", b"c")
    finally:
        engine.close()


@pytest.mark.parametrize("kind", [b"hash", b"set"])
def test_incremental_indexes_match_authoritative_members_under_churn(kind: bytes) -> None:
    engine = Executor(ServerConfig())
    randomizer = random.Random(531)
    try:
        members = [bytes((number, 0, 255)) for number in range(60)]
        for _ in range(150):
            chosen = randomizer.sample(members, randomizer.randrange(1, 8))
            if kind == b"hash":
                args = (
                    [b"HSET", b"k", *(part for member in chosen for part in (member, b"v"))]
                    if randomizer.randrange(2)
                    else [b"HDEL", b"k", *chosen]
                )
            else:
                args = [b"SADD" if randomizer.randrange(2) else b"SREM", b"k", *chosen]
            engine.execute(args)
            value, _ = engine.store.physical_entry(b"k")
            if isinstance(value, HashValue):
                assert value.scan_index == tuple(sorted(value.fields))
            elif isinstance(value, SetValue):
                assert value.scan_index == tuple(sorted(value.members))
            else:
                assert value is None
    finally:
        engine.close()


@pytest.mark.parametrize(
    "create,remove,args",
    [
        (b"HSET", b"HDEL", [b"a", b"v", b"b", b"v"]),
        (b"SADD", b"SREM", [b"a", b"b"]),
    ],
)
def test_delete_index_allocation_failure_preserves_state_and_full_delete_needs_no_index(
    monkeypatch: pytest.MonkeyPatch, create: bytes, remove: bytes, args: list[bytes]
) -> None:
    from pyredis_engine.engine import store

    engine = Executor(ServerConfig())
    try:
        assert engine.execute([create, b"k", *args]).reply == Integer(2)
        previous = engine.store.physical_entry(b"k")
        before = engine.store.memory_bytes, engine.store.mutation_revision

        def fail(*args: object, **kwargs: object) -> None:
            raise MemoryError("injected scan index failure")

        monkeypatch.setattr(store, "sorted", fail, raising=False)
        result = engine.execute([remove, b"k", b"a"]).reply
        assert isinstance(result, Error) and result.payload.startswith(b"OOM")
        assert engine.store.physical_entry(b"k") == previous
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
        assert engine.execute([remove, b"k", b"a", b"b"]).reply == Integer(2)
        assert engine.store.memory_bytes == 0
    finally:
        engine.close()
