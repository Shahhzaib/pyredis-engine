"""Owned benchmark server and pipe controls, never an application wire extension."""
from __future__ import annotations

import asyncio
import gc
import json
import os
import sys
import time
import tracemalloc
from collections import deque
from pathlib import Path

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence.generations import CompactionResult
from pyredis_engine.server import start_server

from .measure import memory


async def main() -> None:
    options = json.loads(sys.argv[1])
    config = ServerConfig(port=0, maxmemory_bytes=options.get('maxmemory', 64 * 1024 * 1024),
        max_clients=options.get('max_clients', 128),
        read_timeout=30, aof_path=options.get('aof_path'),
        aof_fsync=options.get('fsync', 'always'), snapshot_enabled=options.get('snapshots', False),
        replication_enabled=options.get('replication', False),
        replication_recovery=options.get('replication', False),
        replica_host=options.get('replica_host'), replica_port=options.get('replica_port'),
        pubsub_queue_bytes=256 * 1024, pubsub_queue_frames=32,
        write_timeout=1, shutdown_timeout=2)
    server = await start_server(config)
    lag: deque[int] = deque(maxlen=4096)
    peak = 0
    lag_max = 0
    running = True

    async def monitor() -> None:
        nonlocal peak, lag_max
        while running:
            started = time.perf_counter_ns()
            await asyncio.sleep(.01)
            delay = max(0, time.perf_counter_ns() - started - 10_000_000)
            lag.append(delay)
            lag_max = max(lag_max, delay)
            peak = max(peak, int(memory()['rss_bytes']))
            if peak > 256 * 1024 * 1024:
                server.close()
                raise RuntimeError('benchmark RSS watchdog exceeded 256 MiB')

    task = asyncio.create_task(monitor())
    compact: asyncio.Task[CompactionResult] | None = None
    compact_started = 0.0
    print(json.dumps({'port': server.sockets[0].getsockname()[1], 'pid': os.getpid()}), flush=True)
    try:
        async with server:
            while line := await asyncio.to_thread(sys.stdin.readline):
                request = json.loads(line)
                action = request['action']
                extra: dict[str, object] = {}
                if action == 'close':
                    break
                if action == 'reset':
                    lag.clear()
                    peak = lag_max = 0
                elif action == 'compact_start':
                    assert server.persistence is not None
                    compact_started = time.perf_counter()
                    compact = asyncio.create_task(server.persistence.compact())
                elif action == 'compact_wait':
                    assert compact is not None
                    result = await compact
                    assert result.activated, result.error
                    extra['compaction_seconds'] = time.perf_counter() - compact_started
                elif action == 'trace_start':
                    tracemalloc.start(10)
                elif action == 'trace_stop':
                    extra['tracemalloc_current_peak'] = tracemalloc.get_traced_memory()
                    extra['top_traced_allocations'] = [
                        {'file': Path(item.traceback[0].filename).name,
                         'line': item.traceback[0].lineno, 'bytes': item.size, 'count': item.count}
                        for item in tracemalloc.take_snapshot().statistics('lineno')[:10]]
                    tracemalloc.stop()
                elif action == 'collect':
                    lag.clear()  # exclude growth of the observation buffer from retention rounds
                    gc.collect()
                elif action == 'wait_replica':
                    assert server.replication is not None
                    await server.replication.wait_offset(request['offset'])
                elif action != 'metrics':
                    raise ValueError('unknown benchmark control')
                replication = server.replication
                print(json.dumps({'metrics': server.metrics(), 'memory': memory(),
                    'sampled_peak_rss_bytes': peak, 'lag_ns': list(lag), 'lag_max_ns': lag_max,
                    'cpu_seconds': time.process_time(),
                    'traced_current_bytes': tracemalloc.get_traced_memory()[0]
                        if tracemalloc.is_tracing() else None,
                    'offset': replication.offset if replication else None,
                    'journal_bytes': sum(path.stat().st_size for path in
                        Path(config.aof_path).parent.iterdir() if path.is_file())
                        if config.aof_path else 0, **extra}), flush=True)
    finally:
        running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        server.close()
        await server.wait_closed()


if __name__ == '__main__':
    asyncio.run(main())
