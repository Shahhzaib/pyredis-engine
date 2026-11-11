"""Entry point: python scripts/benchmark.py --help (no dependencies installed)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))

from scripts.benchmarks.runner import main  # noqa: E402

if __name__ == '__main__':
    main()
