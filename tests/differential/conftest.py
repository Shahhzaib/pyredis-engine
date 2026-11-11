"""Only absent, unconfigured Redis may skip; broken configured references fail."""
from __future__ import annotations

import os
import shutil
from collections.abc import AsyncIterator, Callable

import pytest
import pytest_asyncio
from _support.redis_reference import configured_executable, private_reference


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def reference_port(
    tmp_path_factory: pytest.TempPathFactory,
    record_testsuite_property: Callable[[str, object], None],
) -> AsyncIterator[int]:
    explicit = os.environ.get("PYREDIS_REDIS_SERVER")
    executable = configured_executable(explicit, shutil.which("redis-server"))
    if executable is None:
        pytest.skip("official redis-server unavailable; configure PYREDIS_REDIS_SERVER")
    directory = tmp_path_factory.mktemp("official-redis")
    async with private_reference((executable,), directory) as reference:
        record_testsuite_property("reference_redis_version", ".".join(map(str, reference.version)))
        yield reference.port
