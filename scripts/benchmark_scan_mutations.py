"""Bounded in-process mutation benchmark for isolated PyRedis source versions."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import platform
import random
import statistics
import sys
from time import perf_counter_ns

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Bulk, Integer


def peak_rss_bytes() -> int:
    if os.name == "nt":
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("faults", wintypes.DWORD),
                *[
                    (name, ctypes.c_size_t)
                    for name in (
                        "peak_working_set",
                        "working_set",
                        "peak_paged_pool",
                        "paged_pool",
                        "peak_nonpaged_pool",
                        "nonpaged_pool",
                        "pagefile",
                        "peak_pagefile",
                    )
                ],
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel = ctypes.windll.kernel32  # type: ignore[attr-defined]
        process_info = ctypes.windll.psapi.GetProcessMemoryInfo  # type: ignore[attr-defined]
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        process_info.argtypes = (wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD)
        process_info.restype = wintypes.BOOL
        if not process_info(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            raise OSError("GetProcessMemoryInfo failed")
        return int(counters.peak_working_set)
    import resource

    scale = 1 if sys.platform == "darwin" else 1024
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale)


def run(size: int, warmup: int, samples: int) -> dict[str, object]:
    engine = Executor(ServerConfig())
    try:
        members = [f"field:{number:06}".encode() for number in range(size)]
        random.Random(42).shuffle(members)
        assert engine.execute(
            [b"HSET", b"h", *(part for m in members for part in (m, b"v"))]
        ).reply == Integer(size)
        assert engine.execute([b"SADD", b"s", *members]).reply == Integer(size)
        member, extra = members[0], b"extra"
        operations = {
            "HSET": ([b"HSET", b"h", member, b"v"], Integer(0), None),
            "HDEL": ([b"HDEL", b"h", member], Integer(1), [b"HSET", b"h", member, b"v"]),
            "SADD": ([b"SADD", b"s", extra], Integer(1), [b"SREM", b"s", extra]),
            "SREM": ([b"SREM", b"s", member], Integer(1), [b"SADD", b"s", member]),
        }
        timings: dict[str, list[float]] = {}
        for name, (command, expected, undo) in operations.items():
            values: list[float] = []
            for iteration in range(warmup + samples):
                start = perf_counter_ns()
                result = engine.execute(command).reply
                elapsed = perf_counter_ns() - start
                assert result == expected
                if undo is not None:
                    assert engine.execute(undo).reply == Integer(1)
                if iteration >= warmup:
                    values.append(elapsed / 1000)
            timings[name] = values
        assert engine.execute([b"HGET", b"h", member]).reply == Bulk(b"v")
        assert engine.execute([b"HLEN", b"h"]).reply == Integer(size)
        assert engine.execute([b"SCARD", b"s"]).reply == Integer(size)
        assert engine.execute([b"SISMEMBER", b"s", member]).reply == Integer(1)
        return {
            "size": size,
            "median_us": {name: statistics.median(values) for name, values in timings.items()},
            "accounted_bytes": engine.store.memory_bytes,
            "peak_rss_bytes_process_highwater": peak_rss_bytes(),
        }
    finally:
        engine.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 5000, 20000])
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=10)
    args = parser.parse_args()
    if not all(2 <= size <= 50000 for size in args.sizes) or not 1 <= args.samples <= 100:
        parser.error("invalid bounded benchmark parameters")
    if not 0 <= args.warmup <= 100:
        parser.error("invalid warmup")
    print(
        json.dumps(
            {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "processor": platform.processor(),
                "samples": args.samples,
                "warmup": args.warmup,
                "results": [run(size, args.warmup, args.samples) for size in args.sizes],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
