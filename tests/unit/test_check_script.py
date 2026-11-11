"""Verify quality-gate selection and failure propagation without rerunning suites."""
from __future__ import annotations

import runpy
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check.py"
COMPILE = ["-m", "compileall", "-q", "src"]
ALL_CHECKS = [
    COMPILE,
    ["-m", "pytest", "-q"],
    ["-m", "ruff", "check", "src", "tests", "scripts"],
    ["-m", "mypy", "src"],
]


@pytest.mark.parametrize(("flag", "expected"), [
    ("--quick", [COMPILE, ["-m", "pytest", "-q", "tests/unit", "tests/integration"]]),
    ("--all", ALL_CHECKS),
])
def test_gate_runs_each_required_check_once(
    monkeypatch: pytest.MonkeyPatch, flag: str, expected: list[list[str]],
) -> None:
    calls: list[list[str]] = []

    def record(command: list[str], *, check: bool) -> None:
        assert check is True
        calls.append(command)

    monkeypatch.setattr(sys, "argv", [str(SCRIPT), flag])
    monkeypatch.setattr(subprocess, "run", record)
    runpy.run_path(str(SCRIPT), run_name="__main__")
    assert calls == [[sys.executable, *args] for args in expected]


@pytest.mark.parametrize("failed_step", range(len(ALL_CHECKS)))
def test_full_gate_stops_and_reports_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], failed_step: int,
) -> None:
    calls: list[list[str]] = []

    def fail(command: list[str], *, check: bool) -> None:
        assert check is True
        calls.append(command)
        if len(calls) == failed_step + 1:
            raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--all"])
    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError) as error:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert error.value.returncode == 7
    assert calls == [[sys.executable, *args] for args in ALL_CHECKS[:failed_step + 1]]
    assert "Quality gate passed" not in capsys.readouterr().out


def test_full_gate_can_require_every_redis_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def record(command: list[str], *, check: bool) -> None:
        assert check
        calls.append(command)

    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--all', '--require-redis'])
    monkeypatch.setattr(subprocess, 'run', record)
    runpy.run_path(str(SCRIPT), run_name='__main__')
    assert calls == [[sys.executable, *COMPILE],
                     [sys.executable, '-m', 'pytest', '-q', '--require-redis'],
                     [sys.executable, *ALL_CHECKS[2]], [sys.executable, *ALL_CHECKS[3]]]
