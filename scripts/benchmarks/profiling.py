"""Separate cProfile samples: never mix instrumentation into TCP throughput."""
from __future__ import annotations

import cProfile
import pstats
import time
from pathlib import Path
from typing import Any

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Error, Integer, Simple


def profiles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for size in (64, 512, 4096):
        for name in ('zadd_update', 'zrange_10_scores', 'zrange_full_scores',
                     'hset_replace', 'dbsize'):
            engine = Executor(ServerConfig())
            try:
                for start in range(0, size, 100):
                    indices = range(start, min(start + 100, size))
                    pairs = [part for index in indices for part in
                             (str(index + .25).encode(), str(index).encode())]
                    engine.execute([b'ZADD', b'z', *pairs])
                    engine.execute([b'HSET', b'h', *pairs])
                    engine.execute([b'MSET', *[part for index in indices for part in
                                              (f'key:{index}'.encode(), b'x')]])
                def operation(index: int, name: str = name, engine: Executor = engine,
                              size: int = size) -> None:
                    command = ([b'ZADD', b'z', b'0.125' if index % 2 else b'0.375', b'0']
                        if name == 'zadd_update' else
                        [b'ZRANGE', b'z', b'0', b'9' if name == 'zrange_10_scores' else b'-1',
                         b'WITHSCORES'] if name.startswith('zrange') else
                        [b'HSET', b'h', b'0', b'x'] if name == 'hset_replace' else [b'DBSIZE'])
                    reply = engine.execute(command).reply
                    assert not isinstance(reply, Error), reply
                    if name == 'zadd_update':
                        assert reply == Integer(0)
                    elif name == 'dbsize':
                        assert reply == Integer(size + 2)
                    elif name.startswith('zrange'):
                        assert isinstance(reply, Array) and reply.payload is not None
                        assert len(reply.payload) == 2 * (min(size, 10)
                                                        if name == 'zrange_10_scores' else size)
                for index in range(10):
                    operation(index)
                count = 10 if name == 'zrange_full_scores' else 200
                profile = cProfile.Profile()
                started = time.perf_counter()
                profile.enable()
                for index in range(count):
                    operation(index)
                profile.disable()
                elapsed = time.perf_counter() - started
                stats: Any = pstats.Stats(profile)
                functions = [{'file': Path(key[0]).name, 'line': key[1], 'function': key[2],
                              'calls': value[1], 'self_seconds': value[2],
                              'cumulative_seconds': value[3]} for key, value in stats.stats.items()]
                rows.append({'name': name, 'size': size, 'operations': count,
                    'warmup_operations': 10, 'seconds': elapsed,
                    'top_cumulative': sorted(functions,
                        key=lambda row: row['cumulative_seconds'], reverse=True)[:15],
                    'correctness': 'passed'})
                assert engine.execute([b'PING']).reply == Simple(b'PONG')
            finally:
                engine.close()
    return rows
