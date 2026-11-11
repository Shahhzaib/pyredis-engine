from __future__ import annotations

import os

import pytest

from scripts.benchmarks.measure import latency, memory
from scripts.benchmarks.runner import Settings, cases
from scripts.benchmarks.workloads import Case


def test_nearest_rank_percentiles_and_counts() -> None:
    result = latency([value * 1_000_000 for value in range(1, 101)])
    assert result == {'count': 100, 'min_ms': 1, 'p50_ms': 50, 'p95_ms': 95,
                      'p99_ms': 99, 'max_ms': 100, 'mean_ms': 50.5}
    assert latency([42])['p99_ms'] == 42 / 1_000_000
    for invalid in ([], [-1]):
        with pytest.raises(ValueError):
            latency(invalid)


@pytest.mark.parametrize('case', [Case('BOGUS', 1, 1), Case('GET', 101, 1),
    Case('GET', 1, 5000), Case('GET', 1, 1024, 65536), Case('GET', True, 1),
    Case('GET', 1, 1, 0), Case('SET', 1, 1, mode='unsafe')])
def test_case_safety_limits(case: Case) -> None:
    with pytest.raises(ValueError):
        case.validate()


@pytest.mark.parametrize('settings', [Settings(samples=1), Settings(operations=0),
    Settings(warmup=0), Settings(minimum_seconds=float('nan'))])
def test_sampling_limits(settings: Settings) -> None:
    with pytest.raises(ValueError):
        settings.validate()


def test_matrix_has_unique_representative_cases_and_real_os_memory() -> None:
    matrix = cases('matrix')
    assert len(set(matrix)) == len(matrix)
    assert {case.clients for case in matrix} == {1, 10, 100}
    assert {case.payload_bytes for case in matrix if case.command == 'GET'} == {32, 1024, 65536}
    for case in matrix:
        case.validate()
    observed = memory()
    assert int(observed['rss_bytes']) > 0 and int(observed['peak_rss_bytes']) > 0
    if os.name == 'nt':
        from scripts.benchmarks.measure import _windows_memory_api
        for _ in range(50):
            memory()
        assert _windows_memory_api.cache_info().currsize == 1
        assert _windows_memory_api.cache_info().hits >= 50
