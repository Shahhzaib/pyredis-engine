"""Bounded whole-frame history; offsets identify exact commit boundaries."""
from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass

from ..persistence.errors import JournalError
from .wire import MAX_OFFSET, OVERHEAD, packet


def advance(proof: bytes, digest: bytes) -> bytes:
    return hashlib.sha256(proof + digest).digest()


@dataclass(frozen=True)
class Record:
    start: int
    end: int
    frame: bytes


class Backlog:
    def __init__(self, max_bytes: int, max_frames: int, history_id: bytes) -> None:
        self.max_bytes, self.max_frames = max_bytes, max_frames
        self.offset = self.bytes = 0
        self.proof = hashlib.sha256(b'PyRedis history v2:' + history_id).digest()
        self.records: deque[Record] = deque()
        self.boundaries: dict[int, bytes] = {0: self.proof}

    @property
    def first_offset(self) -> int:
        return self.records[0].start if self.records else self.offset

    def append(self, payload: bytes) -> bytes:
        start, end = self.offset, self.offset + OVERHEAD + len(payload)
        if end > MAX_OFFSET:
            raise JournalError('replication offset exhausted')
        frame = packet(b'E', end, payload)
        self.proof = advance(self.proof, frame[-32:])
        self.offset = end
        self.records.append(Record(start, end, frame))
        self.bytes += len(frame)
        self.boundaries[end] = self.proof
        while self.bytes > self.max_bytes or len(self.records) > self.max_frames:
            retired = self.records.popleft()
            self.bytes -= len(retired.frame)
            del self.boundaries[retired.start]
        return frame

    def resume(self, offset: int, proof: bytes) -> tuple[bytes, ...] | None:
        if self.boundaries.get(offset) != proof:
            return None
        return tuple(record.frame for record in self.records if record.start >= offset)
