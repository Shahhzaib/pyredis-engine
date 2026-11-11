# Testing

Install Python 3.11 or newer and development dependencies:

```bash
python -m pip install -e '.[dev]'
python scripts/check.py --quick
python scripts/check.py --all
```

The quick gate runs compilation plus unit and integration tests. The full gate
runs the complete pytest suite, Ruff and Mypy. Tests use loopback, isolated temporary
directories and synthetic credentials. TLS tests require an `openssl` executable or
an existing synthetic certificate directory supplied through `PYREDIS_TEST_TLS_DIR`.
Do not use production keys as test fixtures.

Redis differential tests launch a private local reference process on an ephemeral
port. Point `PYREDIS_REDIS_SERVER` at the Redis 7.4.12 executable and run:

```bash
python scripts/check.py --all --require-redis
```

The strict flag requires all selected differential comparisons to execute and pass;
missing Redis or skipped comparisons do not count as acceptance. Without the flag,
missing reference binaries are reported as skips. Never target a shared Redis server.
See [validation and limitations](validation-and-limitations.md) for recorded results,
including their environment and scope. The GitHub Actions workflow has not yet been
verified on a hosted runner.
