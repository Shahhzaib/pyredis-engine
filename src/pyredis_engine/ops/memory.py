"""Deterministic accounting units; these are not Python allocation measurements."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

OOM = b"OOM command not allowed when used memory > 'maxmemory'."


class MemoryLimitError(Exception):
    """A projected datastore write would exceed the configured accounting budget."""


class AllocationRejected(Exception):
    """Private preparation failed before changing a live datastore object."""


def charge(key: bytes, kind: bytes, payload_bytes: int, count: int = 0) -> int:
    overhead = {b'hash': 72, b'list': 32, b'set': 72, b'zset': 128, b'stream': 128}.get(kind, 0)
    return 192 + len(key) + payload_bytes + overhead * count


@contextmanager
def preparing() -> Iterator[None]:
    try:
        yield
    except MemoryError:
        raise AllocationRejected from None
