"""Run reproducible local quality gates, exiting nonzero on failures."""
from __future__ import annotations

import argparse
import subprocess
import sys


def run(*argv: str) -> None:
    cmd = [sys.executable, *argv]
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--require-redis", action="store_true",
                        help="require all Redis differential tests during the full gate")
    args = parser.parse_args()
    if not (args.quick or args.all):
        parser.error("specify --quick or --all")
    if args.require_redis and not args.all:
        parser.error("--require-redis requires --all")
    run("-m", "compileall", "-q", "src")
    if args.all:
        # Full discovery includes unit/integration: do not execute that subset twice.
        run("-m", "pytest", "-q", *(["--require-redis"] if args.require_redis else []))
        run("-m", "ruff", "check", "src", "tests", "scripts")
        run("-m", "mypy", "src")
    else:
        run("-m", "pytest", "-q", "tests/unit", "tests/integration")
    print("Quality gate passed", flush=True)


if __name__ == "__main__":
    main()
