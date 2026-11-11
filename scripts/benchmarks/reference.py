"""Reuse the version/identity-checked isolated Redis fixture for matched workloads."""
from __future__ import annotations

import importlib
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from .client import ROOT


class Observer:
    def __init__(self, pid: int) -> None:
        self.pid = pid

    async def control(self, action: str, **args: Any) -> dict[str, Any]:
        if action not in ('reset', 'metrics') or not sys.platform.startswith('linux'):
            raise RuntimeError('reference measurement supports Linux core workloads only')
        root = Path('/proc') / str(self.pid)
        rss = int((root / 'statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
        peak = next(int(line.split()[1]) * 1024 for line in
                    (root / 'status').read_text().splitlines() if line.startswith('VmHWM:'))
        fields = (root / 'stat').read_text().rsplit(')', 1)[1].split()
        cpu = (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')
        return {'memory': {'rss_bytes': rss, 'peak_rss_bytes': peak,
                           'source': 'Linux /proc reference PID RSS/VmHWM'},
                'cpu_seconds': cpu, 'metrics': None, 'lag_ns': [], 'lag_max_ns': None,
                'sampled_peak_rss_bytes': None, 'offset': None, 'journal_bytes': 0}


async def run_reference(case: Any, settings: Any) -> dict[str, Any]:
    from .runner import measure_row

    if case.mode != 'memory' or case.command not in ('GET', 'SET', 'INCR'):
        raise ValueError('matched Redis comparison supports nonpersistent core commands only')
    sys.path.insert(0, str(ROOT / 'tests'))
    reference = importlib.import_module('_support.redis_reference')
    executable = reference.configured_executable(os.environ.get('PYREDIS_REDIS_SERVER'),
                                                  shutil.which('redis-server'))
    if executable is None:
        raise RuntimeError('Redis executable unavailable: use the documented existing Docker image')
    base = ROOT / 'build' / 'm13' / 'temp'
    base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='redis-', dir=base) as directory:
        async with reference.private_reference((executable,), Path(directory)) as peer:
            if peer.version != (7, 4, 12):
                raise RuntimeError('this comparison report requires Redis 7.4.12')
            result = await measure_row(peer.port, case, settings, Observer(peer.process.pid))
            result['engine'] = 'Redis 7.4.12'
            return result
