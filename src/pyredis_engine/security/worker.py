"""One bounded credential worker; cancellation retains ownership until completion."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

T = TypeVar("T")


class SecurityBusy(Exception):
    pass


class SecurityWorker:
    def __init__(self) -> None:
        self.pool: ThreadPoolExecutor | None = None
        self.pending = 0
        self.closed = False

    async def run(self, operation: Callable[[], T]) -> T:
        if self.closed or self.pending >= 4:
            raise SecurityBusy
        if self.pool is None:
            self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pyredis-auth")
        self.pending += 1
        cancelled = False
        try:
            future = asyncio.get_running_loop().run_in_executor(self.pool, operation)
            while not future.done():
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    cancelled = True
            result = future.result()
            if cancelled:
                raise asyncio.CancelledError
            return result
        finally:
            self.pending -= 1

    def close(self) -> None:
        self.closed = True
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)
