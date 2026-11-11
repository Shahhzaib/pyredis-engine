"""Finite, correctness-gated command matrices; JSON is published only after success."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from .client import ROOT, Child, Peer
from .measure import environment, latency
from .workloads import Case, initialize, operation, verify


@dataclass(frozen=True)
class Settings:
    samples: int = 3
    operations: int = 20
    warmup: int = 4
    minimum_seconds: float = .15

    def validate(self) -> None:
        if any(not isinstance(value, int) or isinstance(value, bool)
               for value in (self.samples, self.operations, self.warmup)):
            raise ValueError('benchmark counts must be integers')
        if (not 2 <= self.samples <= 5 or not 1 <= self.operations <= 100
                or not 1 <= self.warmup <= 16 or not 0 <= self.minimum_seconds <= 1):
            raise ValueError('unsafe benchmark settings')


class Telemetry(Protocol):
    async def control(self, action: str, **args: Any) -> dict[str, Any]: ...


async def measure_row(port: int, case: Case, settings: Settings,
                      child: Telemetry | None = None) -> dict[str, Any]:
    case.validate()
    settings.validate()
    peers: list[Peer] = []
    completed = [0] * case.clients
    samples: list[dict[str, Any]] = []
    try:
        for _ in completed:
            peers.append(await Peer.open(port))
        await initialize(peers[0], case)
        # Warmup is outside sample timing but included in final-state verification.
        for _ in range(settings.warmup):
            await asyncio.gather(*(operation(peer, case, index, completed[index])
                                   for index, peer in enumerate(peers)))
            completed = [count + 1 for count in completed]
        all_latencies: list[int] = []
        for sample in range(settings.samples):
            before = await child.control('reset') if child else None
            if case.mode == 'compact':
                assert child is not None
                await child.control('compact_start')
            gate = asyncio.Event()
            limit = min(2048, 60000 // (case.clients * settings.samples))

            async def requests(index: int, gate: asyncio.Event = gate,
                               limit: int = limit) -> list[int]:
                observed: list[int] = []
                await gate.wait()
                started = time.perf_counter()
                while len(observed) < limit:
                    tick = time.perf_counter_ns()
                    await operation(peers[index], case, index, completed[index])
                    observed.append(time.perf_counter_ns() - tick)
                    completed[index] += 1
                    if (len(observed) >= settings.operations
                            and time.perf_counter() - started >= settings.minimum_seconds):
                        break
                return observed

            tasks = [asyncio.create_task(requests(index)) for index in range(case.clients)]
            started = time.perf_counter()
            gate.set()
            try:
                measured = await asyncio.wait_for(asyncio.gather(*tasks), 40)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            seconds = time.perf_counter() - started
            after = await child.control('metrics') if child else None
            maintenance = (await child.control('compact_wait') if case.mode == 'compact'
                           and child else None)
            observed = [value for sequence in measured for value in sequence]
            all_latencies.extend(observed)
            record = {'sample': sample, 'operations': len(observed), 'seconds': seconds,
                      'ops_per_second': len(observed) / seconds, 'latency': latency(observed)}
            if after and before:
                record.update(server=after, server_cpu_seconds=after['cpu_seconds']
                              - before['cpu_seconds'], maintenance=maintenance)
            samples.append(record)
        await verify(peers[0], case, completed)
        total = sum(sample['operations'] for sample in samples)
        return {'case': asdict(case), 'correctness': 'passed', 'settings': asdict(settings),
                'samples': samples, 'operations': total,
                'ops_per_second': total / sum(sample['seconds'] for sample in samples),
                'latency': latency(all_latencies),
                'engine': 'PyRedis',
                'final': await child.control('metrics') if child else None}
    finally:
        for peer in peers:
            await peer.close()


async def run_case(case: Case, settings: Settings) -> dict[str, Any]:
    case.validate()
    base = ROOT / 'build' / 'm13' / 'temp'
    base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='case-', dir=base) as directory:
        persisted = case.mode != 'memory'
        options: dict[str, Any] = {}
        if persisted:
            options.update(aof_path=str(Path(directory) / 'primary.aof'),
                fsync=case.mode if case.mode in ('none', 'everysec', 'always') else 'always',
                snapshots=case.mode in ('compact', 'replication'),
                replication=case.mode == 'replication')
        async with Child(options) as child:
            if case.mode != 'replication':
                return await measure_row(child.port, case, settings, child)
            # Initial state is populated by the measured runner after this empty sync;
            # live propagation and target-barrier correctness are checked separately.
            async with Child(dict(aof_path=str(Path(directory) / 'replica.aof'), fsync='always',
                    snapshots=True, replication=True, replica_host='127.0.0.1',
                    replica_port=child.port)) as replica:
                initial = await child.control('metrics')
                await replica.control('wait_replica', offset=initial['offset'])
                result = await measure_row(child.port, case, settings, child)
                started = time.perf_counter()
                await replica.control('wait_replica', offset=result['final']['offset'])
                result['replica_catchup_seconds'] = time.perf_counter() - started
                result['replica'] = await replica.control('metrics')
                assert result['replica']['offset'] == result['final']['offset']
                peer = await Peer.open(replica.port)
                try:
                    # SET leaves seeded bytes unchanged; check the entire replica dataset.
                    await verify(peer, case, [0] * case.clients)
                finally:
                    await peer.close()
                return result


def cases(suite: str) -> list[Case]:
    if suite == 'smoke':
        return [Case('GET', 1, 16), Case('INCR', 2, 16), Case('ZRANGE', 1, 16)]
    if suite == 'range':
        return [Case('ZRANGE', clients, 512) for clients in (1, 10, 100)]
    if suite == 'overhead':
        return [Case('SET', 10, 512, 1024, mode)
                for mode in ('memory', 'none', 'everysec', 'always', 'compact', 'replication')]
    if suite == 'memory':
        return [Case(command, clients, keys, size) for command in ('GET', 'SET')
                for clients, keys, size in ((1, 64, 32), (100, 512, 65536))]
    core = [Case(command, clients, keys, size)
            for command in ('GET', 'SET') for clients in (1, 10, 100)
            for keys in (64, 512) for size in (32, 1024, 65536)]
    core += [Case('INCR', clients, keys, 1) for clients in (1, 10, 100) for keys in (64, 512)]
    if suite == 'core':
        return core
    if suite == 'compare':
        return [Case(command, clients, 512, 32 if command != 'INCR' else 1)
                for command in ('GET', 'SET', 'INCR') for clients in (1, 10, 100)]
    return core + [Case(command, clients, keys) for command in
        ('HSET', 'RPUSH', 'SADD', 'ZADD', 'ZRANGE') for clients in (1, 10, 100)
        for keys in (64, 512)]


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    from .profiling import profiles
    from .stress import stress

    settings = Settings(args.samples, args.operations, args.warmup, args.minimum_seconds)
    settings.validate()
    digest = hashlib.sha256()
    for path in sorted((ROOT / 'src').rglob('*.py')):
        digest.update(path.relative_to(ROOT).as_posix().encode() + b'\0')
        digest.update(path.read_bytes().replace(b'\r\n', b'\n'))
    result: dict[str, Any] = {'schema_version': 1, 'environment': environment(),
        'recorded_utc': datetime.now(UTC).isoformat(), 'runtime_sha256': digest.hexdigest(),
        'rss_probe_version': 2,
        'settings': asdict(settings), 'suite': args.suite,
        'git_revision': subprocess.check_output(['git', '-c', f'safe.directory={ROOT.as_posix()}',
            'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'rows': []}
    if args.suite == 'profile':
        result['profiles'] = profiles()
    elif args.suite == 'stress':
        result['stress'] = await stress()
    else:
        for case in cases(args.suite):
            print(f'{case.command} c={case.clients} keys={case.keys} bytes={case.payload_bytes} '
                  f'mode={case.mode}', flush=True)
            row = await run_case(case, settings)
            result['rows'].append(row)
            if args.suite == 'compare':
                from .reference import run_reference
                result['rows'].append(await run_reference(case, settings))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description='Bounded correctness-checked PyRedis benchmarks')
    parser.add_argument('--suite', choices=('smoke', 'core', 'matrix', 'range', 'overhead',
                        'profile', 'stress', 'compare', 'memory'), default='smoke')
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--operations', type=int, default=20)
    parser.add_argument('--warmup', type=int, default=4)
    parser.add_argument('--minimum-seconds', type=float, default=.15)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or sys.flags.optimize:
        parser.error('use a new output path and run without -O (correctness assertions required)')
    async def bounded() -> dict[str, Any]:
        async with asyncio.timeout(600):
            return await execute(args)
    started = time.perf_counter()
    result = asyncio.run(bounded())
    result['elapsed_seconds'] = time.perf_counter() - started
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation prevents accidental overwrite of baseline evidence.
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(f'Validated results: {args.output} ({result["elapsed_seconds"]:.2f}s)', flush=True)
