"""Streaming versioned snapshots of detached, committed typed state."""
from __future__ import annotations

import hashlib
import os
import stat
import struct
from collections.abc import Callable, Iterator
from pathlib import Path

from ..config import ServerConfig
from .codec import Entry, Group, decode_group, encode_group
from .errors import CapacityError, CorruptJournal
from .journal import _read, _write_all, frame, read_frame

MAGIC = b"PYREDSNP\x00\x01\r\n"
HEADER = struct.Struct(">12s16s16sQQ")


def write_snapshot(path: Path, store_id: bytes, generation: bytes, sequence: int,
                   entries: Iterator[Entry], count: int, config: ServerConfig,
                   stage: Callable[[str], None] = lambda _: None) -> None:
    if count > config.aof_max_keys:
        raise CapacityError("snapshot key count exceeds limit")
    header = HEADER.pack(MAGIC, store_id, generation, sequence, count)
    digest = hashlib.sha256(header)
    used = len(header) + 32
    stage("snapshot_create")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
    try:
        _write_all(fd, header)
        stage("snapshot_write")
        actual = 0
        previous: bytes | None = None
        for entry in entries:
            if entry.value is None or previous is not None and entry.key <= previous:
                raise CorruptJournal("snapshot entries must be sorted unique postimages")
            previous = entry.key
            actual += 1
            record = frame(encode_group(Group(False, (entry,)), config), actual)
            used += len(record)
            if used > config.snapshot_max_bytes:
                raise CapacityError("snapshot byte limit exceeded")
            _write_all(fd, record)
            digest.update(record)
        if actual != count or used > config.snapshot_max_bytes:
            raise CorruptJournal("snapshot count/size mismatch")
        _write_all(fd, digest.digest())
        stage("snapshot_sync")
        os.fsync(fd)
    finally:
        os.close(fd)


class SnapshotReader:
    def __init__(self, path: Path, store_id: bytes, generation: bytes, sequence: int,
                 config: ServerConfig) -> None:
        self.path, self.config = path, config
        self.expected = store_id, generation, sequence
        self.fd: int | None = None
        self.count = self.index = 0
        self.previous: bytes | None = None
        self.digest = hashlib.sha256()
        self.finished = False

    def open(self) -> None:
        self.fd = os.open(self.path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
        info = os.fstat(self.fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > self.config.snapshot_max_bytes:
            raise CapacityError("invalid/oversized snapshot file")
        raw = _read(self.fd, HEADER.size)
        if len(raw) != HEADER.size:
            raise CorruptJournal("truncated snapshot header")
        magic, store_id, generation, sequence, self.count = HEADER.unpack(raw)
        if magic != MAGIC or (store_id, generation, sequence) != self.expected:
            raise CorruptJournal("snapshot identity/version mismatch")
        if self.count > self.config.aof_max_keys:
            raise CapacityError("snapshot key count exceeds limit")
        self.digest.update(raw)

    def next_group(self) -> Group | None:
        if self.fd is None:
            raise CorruptJournal("snapshot is closed")
        if self.finished:
            return None
        if self.index == self.count:
            if _read(self.fd, 32) != self.digest.digest() or _read(self.fd, 1):
                raise CorruptJournal("snapshot checksum/trailer mismatch")
            self.finished = True
            return None
        self.index += 1
        payload = read_frame(self.fd, self.config, self.index)
        if payload is None:
            raise CorruptJournal("truncated snapshot entries")
        self.digest.update(frame(payload, self.index))
        group = decode_group(payload, self.config)
        if group.clear or len(group.entries) != 1 or group.entries[0].value is None:
            raise CorruptJournal("invalid snapshot entry")
        key = group.entries[0].key
        if self.previous is not None and key <= self.previous:
            raise CorruptJournal("duplicate/unordered snapshot key")
        self.previous = key
        return group

    def close(self) -> None:
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)
