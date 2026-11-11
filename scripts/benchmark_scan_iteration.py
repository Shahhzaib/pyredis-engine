"""Bounded in-process SCAN measurements; no network or RSS claims."""

from __future__ import annotations

import argparse
import json
import platform
import sys
from time import perf_counter

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Integer, Simple


def measure(engine: Executor, prefix: list[bytes], pages: int, selective: bool) -> list[float]:
    samples: list[float] = []
    for _ in range(3):
        client = engine.new_client()
        cursor = b"0"
        start = 0.0
        for step in range(pages + 16):
            if step == 16:
                start = perf_counter()
            options = [b"MATCH", b"never-match-*"] if selective else []
            reply = engine.execute([*prefix, cursor, b"COUNT", b"1", *options], client).reply
            assert isinstance(reply, Array) and reply.payload is not None
            next_cursor, values = reply.payload
            assert isinstance(next_cursor, Bulk) and next_cursor.payload not in (None, b"0")
            assert isinstance(values, Array) and values.payload is not None
            assert len(values.payload) <= (2 if prefix[0] in (b"HSCAN", b"ZSCAN") else 1)
            if selective:
                assert not values.payload
            cursor = next_cursor.payload
        samples.append((perf_counter() - start) * 1_000_000 / pages)
        engine.close_client(client)
    return samples


def verify(engine: Executor, prefix: list[bytes], members: list[bytes]) -> None:
    cursor = b"0"
    values: list[bytes] = []
    for _ in range(len(members) + 2):
        options = [b"MATCH", b"member:*"] if prefix[0] == b"SCAN" else []
        reply = engine.execute([*prefix, cursor, b"COUNT", b"1024", *options]).reply
        assert isinstance(reply, Array) and reply.payload is not None
        raw_cursor, items = reply.payload
        assert isinstance(raw_cursor, Bulk) and raw_cursor.payload is not None
        assert isinstance(items, Array) and items.payload is not None
        for item in items.payload:
            assert isinstance(item, Bulk) and item.payload is not None
            values.append(item.payload)
        cursor = raw_cursor.payload
        if cursor == b"0":
            break
    assert cursor == b"0"
    if prefix[0] in (b"HSCAN", b"ZSCAN"):
        expected = b"v" if prefix[0] == b"HSCAN" else b"1"
        assert dict(zip(values[::2], values[1::2], strict=True)) == dict.fromkeys(members, expected)
    else:
        assert len(values) == len(members) and set(values) == set(members)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=4096)
    parser.add_argument("--pages", type=int, default=128)
    args = parser.parse_args()
    if not 18 <= args.size <= 100_000 or not 1 <= args.pages <= min(args.size - 17, 512):
        parser.error("size must be 18..100000; pages must be 1..512 with room for warmup")
    engine = Executor(ServerConfig())
    try:
        members = [f"member:{number:06}".encode() for number in range(args.size)]
        assert engine.execute(
            [b"HSET", b"h", *(part for m in members for part in (m, b"v"))]
        ).reply == Integer(args.size)
        assert engine.execute([b"SADD", b"s", *members]).reply == Integer(args.size)
        assert engine.execute(
            [b"ZADD", b"z", *(part for m in members for part in (b"1", m))]
        ).reply == Integer(args.size)
        for member in members:
            assert engine.execute([b"SET", member, b"v"]).reply == Simple(b"OK")
        result = {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "size": args.size,
            "pages_per_sample": args.pages,
            "warmup_pages": 16,
            "count": 1,
            "accounted_total_bytes": engine.store.memory_bytes,
            "accounted_hash_bytes": engine.store._memory_sizes[b"h"],
            "accounted_set_bytes": engine.store._memory_sizes[b"s"],
            "microseconds_per_page": {},
        }
        timings: dict[str, list[float]] = {}
        for name, prefix in (
            ("scan", [b"SCAN"]),
            ("hscan", [b"HSCAN", b"h"]),
            ("sscan", [b"SSCAN", b"s"]),
            ("zscan", [b"ZSCAN", b"z"]),
        ):
            verify(engine, prefix, members)
            timings[name] = measure(engine, prefix, args.pages, False)
            timings[name + "_selective"] = measure(engine, prefix, args.pages, True)
        result["microseconds_per_page"] = timings
        print(json.dumps(result, indent=2))
    finally:
        engine.close()


if __name__ == "__main__":
    main()
