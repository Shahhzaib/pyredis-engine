from __future__ import annotations

from collections.abc import ItemsView

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.collections import SortedSetValue
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer


def test_scan_stable_cursor_is_bounded_and_binary_safe() -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        for key in (b"a\x00", b"a\xff", b"b"):
            engine.execute([b"SET", key, b"v"])
        cursor = b"0"
        seen: set[bytes] = set()
        for _ in range(10):
            result = engine.execute(
                [b"SCAN", cursor, b"MATCH", b"a*", b"COUNT", b"1"], client
            ).reply
            assert isinstance(result, Array) and result.payload is not None
            next_cursor, values = result.payload
            assert isinstance(next_cursor, Bulk) and next_cursor.payload is not None
            assert isinstance(values, Array) and values.payload is not None
            assert len(values.payload) <= 1
            seen.update(
                item.payload
                for item in values.payload
                if isinstance(item, Bulk) and item.payload is not None
            )
            cursor = next_cursor.payload
            if cursor == b"0":
                break
        assert cursor == b"0" and seen == {b"a\x00", b"a\xff"}
    finally:
        engine.close()


def test_collection_scans_and_type_filter() -> None:
    engine = Executor(ServerConfig())
    try:
        engine.execute([b"HSET", b"h", b"\x00", b"v1", b"\xff", b"v2"])
        engine.execute([b"SADD", b"s", b"one", b"\xff"])
        engine.execute([b"ZADD", b"z", b"1.5", b"one", b"-inf", b"\xff"])
        expected = {
            b"HSCAN": {b"\x00", b"v1", b"\xff", b"v2"},
            b"SSCAN": {b"one", b"\xff"},
            b"ZSCAN": {b"one", b"1.5", b"\xff", b"-inf"},
        }
        for name, key in ((b"HSCAN", b"h"), (b"SSCAN", b"s"), (b"ZSCAN", b"z")):
            result = engine.execute([name, key, b"0", b"COUNT", b"99"]).reply
            assert isinstance(result, Array) and result.payload is not None
            cursor, values = result.payload
            assert cursor == Bulk(b"0") and isinstance(values, Array)
            assert values.payload is not None
            assert {item.payload for item in values.payload if isinstance(item, Bulk)} == expected[
                name
            ]
        result = engine.execute([b"SCAN", b"0", b"TYPE", b"set", b"COUNT", b"99"]).reply
        assert result == Array((Bulk(b"0"), Array((Bulk(b"s"),))))
    finally:
        engine.close()


def test_cursor_options_expiration_and_limits() -> None:
    clock = ManualClock(1000)
    engine = Executor(ServerConfig(), clock=clock)
    client = engine.new_client()
    try:
        for index in range(20):
            engine.execute([b"SET", str(index).encode(), b"v"])
        for command in (
            [b"SCAN", b"bad"],
            [b"SCAN", b"-1"],
            [b"SCAN", b"0", b"COUNT", b"0"],
            [b"HSCAN", b"k", b"0", b"TYPE", b"set"],
            [b"SCAN", b"999"],
            [b"SCAN", b"0", b"MATCH", b"x" * 129],
        ):
            assert isinstance(engine.execute(command, client).reply, Error)
        engine.execute([b"HSET", b"expires", b"a", b"v", b"b", b"w"])
        assert engine.execute([b"PEXPIRE", b"expires", b"5"]).reply == Integer(1)
        reply = engine.execute([b"HSCAN", b"expires", b"0", b"COUNT", b"1"], client).reply
        assert isinstance(reply, Array) and reply.payload is not None
        cursor = reply.payload[0]
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        clock.advance(6)
        assert engine.execute([b"HSCAN", b"expires", cursor.payload], client).reply == Array(
            (Bulk(b"0"), Array(()))
        )
        assert client.scan is None
        assert engine.execute([b"MULTI"], client).reply.payload == b"OK"
        assert isinstance(engine.execute([b"SCAN", b"0"], client).reply, Error)
    finally:
        engine.close()


def test_count_hint_has_a_hard_per_call_step_cap() -> None:
    engine = Executor(ServerConfig())
    try:
        for number in range(1100):
            engine.execute([b"SET", str(number).encode(), b"v"])
        result = engine.execute([b"SCAN", b"0", b"COUNT", b"1000000"]).reply
        assert isinstance(result, Array) and result.payload is not None
        cursor, values = result.payload
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        assert isinstance(values, Array) and values.payload is not None
        assert len(values.payload) == 1024
    finally:
        engine.close()


def test_mutation_invalidates_cursor_without_keeping_old_storage() -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        for key in (b"a", b"b", b"c"):
            engine.execute([b"HSET", b"hash", key, key])
        reply = engine.execute([b"HSCAN", b"hash", b"0", b"COUNT", b"1"], client).reply
        assert isinstance(reply, Array) and reply.payload is not None
        cursor = reply.payload[0]
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        engine.execute([b"SET", b"unrelated", b"v"])
        assert client.scan is not None
        engine.execute([b"DEL", b"hash"])
        assert client.scan is None
        assert isinstance(engine.execute([b"HSCAN", b"hash", cursor.payload], client).reply, Error)
        assert engine.execute([b"HSCAN", b"hash", b"0"], client).reply == Array(
            (Bulk(b"0"), Array(()))
        )
    finally:
        engine.close()


def test_zscan_uses_ranked_index_without_scanning_hash_table() -> None:
    class NoItems(dict[bytes, float]):
        def items(self) -> ItemsView[bytes, float]:
            raise AssertionError("ZSCAN must not traverse the score hash table")

    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        scores = NoItems({b"a": 1.0, b"b": 2.0, b"c": 3.0})
        engine.store.put(b"z", SortedSetValue(scores, [(1.0, b"a"), (2.0, b"b"), (3.0, b"c")], 27))
        result = engine.execute([b"ZSCAN", b"z", b"0", b"COUNT", b"1"], client).reply
        assert isinstance(result, Array) and result.payload is not None
        assert result.payload[1] == Array((Bulk(b"a"), Bulk(b"1")))
    finally:
        engine.close()


def test_hash_and_set_cursors_continue_across_mutations() -> None:
    engine = Executor(ServerConfig())
    client = engine.new_client()
    try:
        for name, insert, remove in ((b"HSCAN", b"HSET", b"HDEL"), (b"SSCAN", b"SADD", b"SREM")):
            key = name
            for member in (b"a", b"b", b"c"):
                args = [insert, key, member] + ([member] if insert == b"HSET" else [])
                engine.execute(args)
            result = engine.execute([name, key, b"0", b"COUNT", b"1"], client).reply
            assert isinstance(result, Array) and result.payload is not None
            cursor, first = result.payload
            assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
            assert isinstance(first, Array) and first.payload is not None
            engine.execute([remove, key, b"b"])
            engine.execute([insert, key, b"d"] + ([b"d"] if insert == b"HSET" else []))
            result = engine.execute([name, key, cursor.payload, b"COUNT", b"10"], client).reply
            assert isinstance(result, Array) and result.payload is not None
            assert result.payload[0] == Bulk(b"0")
            values = result.payload[1]
            assert isinstance(values, Array) and values.payload is not None
            assert Bulk(b"b") not in values.payload
            assert Bulk(b"c") in values.payload
    finally:
        engine.close()


def test_keyspace_cursor_survives_value_updates_and_aborted_value_changes() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    client, writer = engine.new_client(), engine.new_client()
    try:
        for key in (b"a", b"b", b"c"):
            assert engine.execute([b"SET", key, b"v"], writer).reply.payload == b"OK"
        page = engine.execute([b"SCAN", b"0", b"COUNT", b"1"], client).reply
        assert isinstance(page, Array) and page.payload is not None
        cursor = page.payload[0]
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        assert engine.execute([b"SET", b"b", b"changed"], writer).reply.payload == b"OK"
        assert engine.execute([b"MULTI"], writer).reply.payload == b"OK"
        engine.execute([b"SET", b"b", b"temporary"], writer)
        engine.execute([b"ECHO", b"x" * 120], writer)
        result = engine.execute([b"EXEC"], writer).reply
        assert isinstance(result, Error) and result.payload.startswith(b"EXECABORT")
        page = engine.execute([b"SCAN", cursor.payload, b"COUNT", b"10"], client).reply
        assert isinstance(page, Array) and page.payload is not None
        assert page.payload[0] == Bulk(b"0")
        assert engine.execute([b"GET", b"b"], writer).reply == Bulk(b"changed")
    finally:
        engine.close()


def test_keyspace_cursor_invalidates_after_aborted_structural_change() -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    client, writer = engine.new_client(), engine.new_client()
    try:
        for key in (b"a", b"b", b"c"):
            engine.execute([b"SET", key, b"v"], writer)
        page = engine.execute([b"SCAN", b"0", b"COUNT", b"1"], client).reply
        assert isinstance(page, Array) and page.payload is not None
        cursor = page.payload[0]
        assert isinstance(cursor, Bulk) and cursor.payload not in (None, b"0")
        engine.execute([b"MULTI"], writer)
        engine.execute([b"SET", b"new", b"v"], writer)
        engine.execute([b"ECHO", b"x" * 120], writer)
        assert isinstance(engine.execute([b"EXEC"], writer).reply, Error)
        assert isinstance(engine.execute([b"SCAN", cursor.payload], client).reply, Error)
        assert engine.execute([b"GET", b"new"], writer).reply == Bulk(None)
    finally:
        engine.close()
