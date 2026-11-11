"""Mutable bytes-backed collections, confined to synchronous command execution."""

from __future__ import annotations

from bisect import bisect_left, insort
from collections import deque
from dataclasses import dataclass, field
from typing import ClassVar


@dataclass(slots=True)
class HashValue:
    fields: dict[bytes, bytes] = field(default_factory=dict)
    byte_size: int = 0
    scan_index: tuple[bytes, ...] = field(default=(), compare=False, repr=False)
    kind: ClassVar[bytes] = b"hash"


@dataclass(slots=True)
class ListValue:
    items: deque[bytes] = field(default_factory=deque)
    byte_size: int = 0
    kind: ClassVar[bytes] = b"list"


@dataclass(slots=True)
class SetValue:
    members: set[bytes] = field(default_factory=set)
    byte_size: int = 0
    scan_index: tuple[bytes, ...] = field(default=(), compare=False, repr=False)
    kind: ClassVar[bytes] = b"set"


@dataclass(slots=True)
class SortedSetValue:
    scores: dict[bytes, float] = field(default_factory=dict)
    ranked: list[tuple[float, bytes]] = field(default_factory=list)
    byte_size: int = 0
    kind: ClassVar[bytes] = b"zset"

    def update(self, member: bytes, score: float) -> bool:
        previous = self.scores.get(member)
        if previous == score:
            return False
        if previous is not None:
            self.ranked.pop(bisect_left(self.ranked, (previous, member)))
        else:
            self.byte_size += len(member) + 8
        self.scores[member] = score
        insort(self.ranked, (score, member))
        return True

    def remove(self, member: bytes) -> None:
        score = self.scores.pop(member)
        self.ranked.pop(bisect_left(self.ranked, (score, member)))
        self.byte_size -= len(member) + 8


@dataclass(frozen=True, slots=True)
class StreamEntry:
    id: tuple[int, int]
    fields: tuple[tuple[bytes, bytes], ...]


@dataclass(frozen=True, slots=True)
class PendingEntry:
    id: tuple[int, int]
    consumer: bytes
    delivered_ms: int
    deliveries: int


@dataclass(frozen=True, slots=True)
class StreamGroup:
    name: bytes
    last_id: tuple[int, int]
    consumers: tuple[bytes, ...] = ()
    pending: tuple[PendingEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class StreamValue:
    entries: tuple[StreamEntry, ...] = ()
    last_id: tuple[int, int] = (0, 0)
    byte_size: int = 0
    field_count: int = 0
    groups: tuple[StreamGroup, ...] = ()
    kind: ClassVar[bytes] = b"stream"


def stream_metadata(value: StreamValue) -> tuple[int, int, int]:
    units = raw = charge = 0
    for group in value.groups:
        units += 1 + len(group.consumers) + len(group.pending)
        raw += len(group.name)
        charge += 128 + len(group.name)
        for consumer in group.consumers:
            raw += len(consumer)
            charge += 96 + len(consumer)
        for pending in group.pending:
            raw += len(pending.consumer)
            charge += 128 + len(pending.consumer)
    return units, raw, charge
