from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_release_demo_runs_end_to_end(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[2] / 'scripts' / 'release_demo.py'
    source = script.parent.parent / 'src'
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.pathsep.join(filter(None, (str(source),
                                                               environment.get('PYTHONPATH'))))
    result = subprocess.run([sys.executable, str(script), '--work-dir', str(tmp_path)],
                            text=True, capture_output=True, timeout=45, check=False,
                            env=environment)
    assert result.returncode == 0, result.stdout + result.stderr
    for marker in ('transaction', 'expiration', 'consumer group', 'replica', 'restart'):
        assert marker in result.stdout.lower()
