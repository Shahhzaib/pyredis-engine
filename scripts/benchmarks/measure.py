"""OS memory, environment and explicitly defined latency statistics."""
from __future__ import annotations

import ctypes
import math
import os
import platform
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any


@lru_cache(maxsize=1)
def _windows_memory_api() -> tuple[Any, Any, Any]:
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [('cb', wintypes.DWORD), ('faults', wintypes.DWORD),
            ('peak', ctypes.c_size_t), ('rss', ctypes.c_size_t),
            ('page_peak', ctypes.c_size_t), ('page', ctypes.c_size_t),
            ('nonpage_peak', ctypes.c_size_t), ('nonpage', ctypes.c_size_t),
            ('swap', ctypes.c_size_t), ('swap_peak', ctypes.c_size_t)]

    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    api = ctypes.WinDLL('psapi', use_last_error=True).GetProcessMemoryInfo
    api.argtypes = [ctypes.c_void_p, ctypes.POINTER(Counters), wintypes.DWORD]
    api.restype = wintypes.BOOL
    return api, kernel.GetCurrentProcess(), Counters


def memory() -> dict[str, int | str]:
    if os.name == 'nt':
        api, handle, Counters = _windows_memory_api()
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        if not api(handle, ctypes.byref(counters), counters.cb):
            raise OSError(ctypes.get_last_error(), 'GetProcessMemoryInfo failed')
        return {'rss_bytes': counters.rss, 'peak_rss_bytes': counters.peak,
                'source': 'Windows GetProcessMemoryInfo WorkingSet/PeakWorkingSet'}
    if sys.platform.startswith('linux'):
        import resource

        rss = int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
        return {'rss_bytes': rss,
                'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                'source': 'Linux /proc/self/statm and getrusage ru_maxrss'}
    raise RuntimeError('RSS measurement currently supports Windows and Linux only')


def environment() -> dict[str, Any]:
    cpu = platform.processor()
    extra: dict[str, Any] = {}
    if os.name == 'nt':
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                r'HARDWARE\DESCRIPTION\System\CentralProcessor\0') as register:
            cpu = winreg.QueryValueEx(register, 'ProcessorNameString')[0].strip()
        class MemoryStatus(ctypes.Structure):
            _fields_ = [('length', ctypes.c_ulong), ('load', ctypes.c_ulong),
                        ('values', ctypes.c_ulonglong * 7)]
        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        api = ctypes.WinDLL('kernel32', use_last_error=True).GlobalMemoryStatusEx
        api.argtypes = [ctypes.POINTER(MemoryStatus)]
        api.restype = ctypes.c_int
        if not api(ctypes.byref(status)):
            raise OSError(ctypes.get_last_error(), 'GlobalMemoryStatusEx failed')
        extra['physical_ram_bytes'] = status.values[0]
    if sys.platform.startswith('linux'):
        for line in Path('/proc/cpuinfo').read_text().splitlines():
            if line.startswith('model name'):
                cpu = line.partition(':')[2].strip()
                break
        meminfo = Path('/proc/meminfo').read_text().splitlines()
        extra['memory_total_kib'] = int(meminfo[0].split()[1])
        for name in ('cpu.max', 'memory.max'):
            path = Path('/sys/fs/cgroup') / name
            if path.is_file():
                extra['cgroup_' + name] = path.read_text().strip()
    return {'python': sys.version, 'platform': platform.platform(), 'machine': platform.machine(),
            'cpu': cpu, 'logical_cpus': os.cpu_count(), 'clock': 'perf_counter_ns',
            'rss_probe': memory()['source'], 'pid': os.getpid(), **extra}


def latency(values_ns: list[int]) -> dict[str, float | int]:
    if not values_ns or any(value < 0 for value in values_ns):
        raise ValueError('latency requires nonnegative samples')
    ordered = sorted(values_ns)
    def percentile(fraction: float) -> float:
        return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] / 1_000_000
    return {'count': len(ordered), 'min_ms': ordered[0] / 1_000_000,
            'p50_ms': percentile(.5), 'p95_ms': percentile(.95), 'p99_ms': percentile(.99),
            'max_ms': ordered[-1] / 1_000_000, 'mean_ms': sum(ordered) / len(ordered) / 1_000_000}
