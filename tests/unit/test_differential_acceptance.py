"""Exercise pytest policy in isolated subprocesses without running an oracle."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("body,options,accepted", [
    ("pass", [], True), ("pytest.skip('unavailable')", [], False),
    ("pytest.xfail('pending')", [], False),
    ("pass", ["--collect-only"], False),
    ("pass", ["-k", "other"], False),
    ("assert False", [], False),
    ("pass", ["-m", "not differential"], False),
])
def test_strict_acceptance(
    tmp_path: Path, body: str, options: list[str], accepted: bool,
) -> None:
    script = tmp_path / "test_policy.py"
    script.write_text("import pytest\n@pytest.mark.differential\ndef test_comparison():\n    "
                      + body + "\ndef test_other():\n    pass\n")
    environment = os.environ.copy()
    environment.update(PYTHONPATH=str(TESTS), PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", PYTEST_ADDOPTS="")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p",
         "_support.acceptance", "--require-redis", *options, str(script)],
        cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
    )
    assert (result.returncode == 0) == accepted, result.stdout + result.stderr
    if not accepted and "assert False" not in body:
        assert "Redis acceptance incomplete" in result.stdout


@pytest.mark.parametrize("mode", ["setup_skip", "xfail", "xpass"])
def test_skips_and_xfails_cannot_pass_acceptance(tmp_path: Path, mode: str) -> None:
    script = tmp_path / "test_policy.py"
    decorator = ("@pytest.mark.skip(reason='unavailable')" if mode == "setup_skip"
                 else "@pytest.mark.xfail(reason='pending', strict=False)")
    body = "assert False" if mode == "xfail" else "pass"
    script.write_text(f"import pytest\n@pytest.mark.differential\n{decorator}\n"
                      f"def test_comparison():\n    {body}\n")
    environment = os.environ.copy()
    environment.update(PYTHONPATH=str(TESTS), PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", PYTEST_ADDOPTS="")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p",
         "_support.acceptance", "--require-redis", str(script)],
        cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
    )
    assert result.returncode != 0
    assert "Redis acceptance incomplete" in result.stdout
