"""Bytes-backed typed values and absolute expiration; no I/O or persistence."""
from __future__ import annotations

from bisect import bisect_left
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from heapq import merge
from typing import TYPE_CHECKING, ClassVar, Protocol

from ..ops.memory import MemoryLimitError, charge, preparing
from .clock import Clock, SystemClock
from .collections import (
    HashValue,
    ListValue,
    SetValue,
    SortedSetValue,
    StreamValue,
    stream_metadata,
)
from .expiration import ExpirationIndex

if TYPE_CHECKING:
    from .exec_stage import ExecStage


class StoredValue(Protocol):
    @property
    def kind(self) -> bytes: ...


@dataclass(frozen=True, slots=True)
class StringValue:
    data: bytes
    kind: ClassVar[bytes] = b"string"

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):
            raise TypeError("string values must be bytes")


class WrongTypeError(Exception):
    """An operation encountered another stored value kind."""


class Store:
    def __init__(
        self, *, clock: Clock | None = None,
        on_change: Callable[[bytes, int], None] | None = None,
        maxmemory_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        if (not isinstance(maxmemory_bytes, int) or isinstance(maxmemory_bytes, bool)
                or maxmemory_bytes < 1):
            raise ValueError('maxmemory_bytes must be a positive integer')
        self._values: dict[bytes, StoredValue] = {}
        self.clock = clock if clock is not None else SystemClock()
        self._time: int | None = None
        self._expiration = ExpirationIndex()
        self._revisions: dict[bytes, int] = {}
        self._sequence = 0
        self._keyspace_revision = 0
        self._on_change = on_change
        self._listeners: set[Callable[[bytes, int], None]] = set()
        self._clear_listeners: set[Callable[[], None]] = set()
        self.clearing = False
        self.expire_locally = True
        self.maxmemory_bytes = maxmemory_bytes
        self.memory_bytes = 0
        self.memory_rejections = 0
        self.allocation_rejections = 0
        self._memory_sizes: dict[bytes, int] = {}
        self._memory_batch = False
        self._exec_stage: ExecStage | None = None

    @staticmethod
    def value_charge(key: bytes, value: StoredValue) -> int:
        if isinstance(value, StringValue):
            return charge(key, value.kind, len(value.data))
        if isinstance(value, HashValue):
            return charge(key, value.kind, value.byte_size, len(value.fields))
        if isinstance(value, ListValue):
            return charge(key, value.kind, value.byte_size, len(value.items))
        if isinstance(value, SetValue):
            return charge(key, value.kind, value.byte_size, len(value.members))
        if isinstance(value, SortedSetValue):
            return charge(key, value.kind, value.byte_size, len(value.scores))
        if isinstance(value, StreamValue):
            _, _, metadata_charge = stream_metadata(value)
            return charge(
                key, value.kind, value.byte_size + 64 * value.field_count + metadata_charge,
                len(value.entries),
            )
        return charge(key, value.kind, 0)  # embedded custom sentinel; not a persisted type

    def _check_budget(self, projected: int) -> None:
        if projected > self.maxmemory_bytes:
            self.memory_rejections += 1
            raise MemoryLimitError

    def check_growth(self, key: bytes, kind: bytes, size: int, count: int = 0) -> None:
        self.get(key)
        if not self._memory_batch:
            self._check_budget(self.memory_bytes - self._memory_sizes.get(key, 0)
                               + charge(key, kind, size, count))

    def check_batch(self, values: Mapping[bytes, StoredValue | None], *,
                    clear: bool = False) -> None:
        projected = 0 if clear else self.memory_bytes
        for key, value in values.items():
            if not clear:
                projected -= self._memory_sizes.get(key, 0)
            projected += 0 if value is None else self.value_charge(key, value)
        self._check_budget(projected)

    @contextmanager
    def memory_batch(self, values: Mapping[bytes, StoredValue | None], *,
                     clear: bool = False) -> Iterator[None]:
        self.check_batch(values, clear=clear)
        previous = self._memory_batch
        self._memory_batch = True
        try:
            yield
        finally:
            self._memory_batch = previous

    def add_clear_listener(self, listener: Callable[[], None]) -> None:
        self._clear_listeners.add(listener)

    def remove_clear_listener(self, listener: Callable[[], None]) -> None:
        self._clear_listeners.discard(listener)

    def physical_entry(self, key: bytes) -> tuple[StoredValue | None, int | None]:
        """Inspect a postimage without triggering expiry or observer reentry."""
        return self._values.get(key), self._expiration.deadline(key)

    @property
    def mutation_revision(self) -> int:
        return self._sequence

    def install_state(self, staged: Store) -> None:
        """Transfer private validated state; caller holds the serialized commit lock."""
        self._check_budget(staged.memory_bytes)
        self._keyspace_revision += 1
        previous = self._values
        self._values, self._expiration = staged._values, staged._expiration
        staged._values, staged._expiration = {}, ExpirationIndex()
        self._memory_sizes, staged._memory_sizes = staged._memory_sizes, {}
        self.memory_bytes, staged.memory_bytes = staged.memory_bytes, 0
        staged._revisions.clear()
        self._revisions.clear()
        for key in previous:
            self._touch(key)
        for key in self._values:
            if key not in previous:
                self._touch(key)

    def add_change_listener(self, listener: Callable[[bytes, int], None]) -> None:
        self._listeners.add(listener)

    def remove_change_listener(self, listener: Callable[[bytes, int], None]) -> None:
        self._listeners.discard(listener)

    @property
    def now_ms(self) -> int:
        return self.clock.now_ms() if self._time is None else self._time

    @contextmanager
    def at_time(self, milliseconds: int) -> Iterator[None]:
        previous = self._time
        self._time = milliseconds
        try:
            yield
        finally:
            self._time = previous

    def _touch(self, key: bytes) -> None:
        self._sequence += 1
        if key in self._values:
            self._revisions[key] = self._sequence
        else:
            self._revisions.pop(key, None)
        if self._exec_stage is not None:
            self._exec_stage.events.append((key, self._sequence, self.clearing))
        else:
            self._notify(key, self._sequence)

    def _notify(self, key: bytes, revision: int) -> None:
        if self._on_change is not None:
            self._on_change(key, revision)
        for listener in tuple(self._listeners):
            listener(key, revision)

    def _remember(self, key: bytes) -> None:
        if self._exec_stage is not None:
            self._exec_stage.remember(key)

    def _remove(self, key: bytes) -> None:
        self._remember(key)
        del self._values[key]
        self._keyspace_revision += 1
        self.memory_bytes -= self._memory_sizes.pop(key)
        self._expiration.discard(key)
        self._touch(key)

    def get(self, key: bytes) -> StoredValue | None:
        deadline = self._expiration.deadline(key)
        if deadline is not None and self.now_ms > deadline:
            if self.expire_locally:
                self._remove(key)
            return None
        return self._values.get(key)

    def get_string(self, key: bytes) -> bytes | None:
        value = self.get(key)
        if value is None:
            return None
        if not isinstance(value, StringValue):
            raise WrongTypeError
        return value.data

    def get_hash(self, key: bytes) -> HashValue | None:
        value = self.get(key)
        if value is not None and not isinstance(value, HashValue):
            raise WrongTypeError
        return value

    def get_list(self, key: bytes) -> ListValue | None:
        value = self.get(key)
        if value is not None and not isinstance(value, ListValue):
            raise WrongTypeError
        return value

    def get_set(self, key: bytes) -> SetValue | None:
        value = self.get(key)
        if value is not None and not isinstance(value, SetValue):
            raise WrongTypeError
        return value

    def get_sorted_set(self, key: bytes) -> SortedSetValue | None:
        value = self.get(key)
        if value is not None and not isinstance(value, SortedSetValue):
            raise WrongTypeError
        return value

    def get_stream(self, key: bytes) -> StreamValue | None:
        value = self.get(key)
        if value is not None and not isinstance(value, StreamValue):
            raise WrongTypeError
        return value

    def put(
        self, key: bytes, value: StoredValue, *, keep_ttl: bool = False,
        expires_at_ms: int | None = None, _prepared_scan_index: tuple[bytes, ...] | None = None,
    ) -> None:
        if not isinstance(key, bytes):
            raise TypeError("keys must be bytes")
        self.get(key)
        cost = self.value_charge(key, value)
        if not self._memory_batch:
            self._check_budget(self.memory_bytes - self._memory_sizes.get(key, 0) + cost)
        if isinstance(value, (HashValue, SetValue)):
            index = (_prepared_scan_index if _prepared_scan_index is not None
                     else self.prepare_scan_index(value))
            assert index is not None
            value.scan_index = index
        if keep_ttl:
            expires_at_ms = self._expiration.deadline(key)
        self._remember(key)
        new_key = key not in self._values
        self._values[key] = value
        if new_key:
            self._keyspace_revision += 1
        self.memory_bytes += cost - self._memory_sizes.get(key, 0)
        self._memory_sizes[key] = cost
        if expires_at_ms is None:
            self._expiration.discard(key)
        else:
            self._expiration.set(key, expires_at_ms)
        self._touch(key)

    @staticmethod
    def prepare_scan_index(value: StoredValue | None) -> tuple[bytes, ...] | None:
        if isinstance(value, (HashValue, SetValue)):
            with preparing():
                members = value.fields if isinstance(value, HashValue) else value.members
                return tuple(sorted(members))
        return None

    @staticmethod
    def update_scan_index(
        index: tuple[bytes, ...], added: set[bytes], removed: set[bytes]
    ) -> tuple[bytes, ...]:
        """Prepare a replacement without modifying the published index."""
        if not added and not removed:
            return index
        with preparing():
            additions = sorted(added)
            if len(additions) == 1 and not removed:
                pos = bisect_left(index, additions[0])
                return index[:pos] + (additions[0],) + index[pos:]
            if len(removed) == 1 and not additions:
                member = next(iter(removed))
                pos = bisect_left(index, member)
                assert pos < len(index) and index[pos] == member
                return index[:pos] + index[pos + 1:]
            return tuple(merge((member for member in index if member not in removed), additions))

    def delete(self, key: bytes) -> bool:
        if self.get(key) is None:
            return False
        self._remove(key)
        return True

    def contains(self, key: bytes) -> bool:
        return self.get(key) is not None

    def deadline(self, key: bytes) -> int | None:
        self.get(key)
        return self._expiration.deadline(key)

    def set_deadline(self, key: bytes, deadline: int) -> bool:
        if self.get(key) is None:
            return False
        if deadline <= self.now_ms:
            self._remove(key)
        else:
            self._remember(key)
            self._expiration.set(key, deadline)
            self._touch(key)
        return True

    def persist(self, key: bytes) -> bool:
        if self.get(key) is None or self._expiration.deadline(key) is None:
            return False
        self._remember(key)
        self._expiration.discard(key)
        self._touch(key)
        return True

    def key_revision(self, key: bytes) -> int:
        self.get(key)
        return self._revisions.get(key, 0)

    def expire_batch(self, limit: int, max_key_bytes: int | None = None) -> int:
        if not self.expire_locally:
            return 0
        removed = 0
        used = 0
        now = self.now_ms
        while removed < limit:
            item = self._expiration.first()
            if item is None or item[0] >= now:
                break
            cost = len(item[1]) + 14  # encoded deletion entry, without the group header
            if max_key_bytes is not None and cost > max_key_bytes - used:
                break
            self._remove(item[1])
            used += cost
            removed += 1
        return removed

    @property
    def physical_count(self) -> int:
        return len(self._values)

    @property
    def expiration_count(self) -> int:
        return len(self._expiration)

    @property
    def revision_count(self) -> int:
        return len(self._revisions)

    def clear(self) -> None:
        # FLUSHDB already has O(N) semantics; notify without retaining tombstones.
        self._keyspace_revision += 1
        self.clearing = True
        try:
            if self._exec_stage is not None:
                self._exec_stage.events.append((None, 0, True))
                for key in self._values:
                    self._remember(key)
            else:
                for listener in tuple(self._clear_listeners):
                    listener()
            self._expiration.clear()
            while self._values:
                key, _ = self._values.popitem()
                self.memory_bytes -= self._memory_sizes.pop(key)
                self._touch(key)
        finally:
            self.clearing = False

    def __len__(self) -> int:
        now = self.now_ms
        return sum(1 for key in self._values
                   if (deadline := self._expiration.deadline(key)) is None or deadline >= now)
