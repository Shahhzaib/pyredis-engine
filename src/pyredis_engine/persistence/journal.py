"""Single-worker-owned AOF framing, exclusive lock and explicit tail repair."""
from __future__ import annotations

import hashlib
import os
import stat
import struct
import sys
import zlib
from pathlib import Path

from ..config import ServerConfig
from .codec import Group, decode_group
from .errors import CapacityError, CorruptJournal, JournalError, TornTail

MAGIC = b"PYREDAOF\x00\x01\r\n"
HEADER = struct.Struct(">IQI")


def read_frame(fd: int, config: ServerConfig, sequence: int,
               end: int | None = None) -> bytes | None:
    position = os.lseek(fd, 0, os.SEEK_CUR)
    if end is not None and position == end:
        return None
    if end is not None and (position > end or end - position < HEADER.size):
        raise CorruptJournal("frame exceeds pinned journal prefix")
    header = _read(fd, HEADER.size)
    if not header:
        if end is not None:
            raise CorruptJournal("pinned journal was truncated")
        return None
    if len(header) != HEADER.size:
        raise TornTail("incomplete AOF frame header")
    length, actual_sequence, crc = HEADER.unpack(header)
    if zlib.crc32(header[:12]) != crc:
        raise CorruptJournal("AOF header checksum mismatch")
    if not 5 <= length <= config.aof_max_record_bytes:
        raise CapacityError("AOF record length exceeds configured limit")
    if actual_sequence != sequence:
        raise CorruptJournal("AOF sequence mismatch")
    if end is not None and length + 32 > end - position - HEADER.size:
        raise CorruptJournal("frame exceeds pinned journal prefix")
    payload, checksum = _read(fd, length), _read(fd, 32)
    if len(payload) != length or len(checksum) != 32:
        raise TornTail("incomplete AOF frame body")
    digest = hashlib.sha256(header)
    digest.update(payload)
    if digest.digest() != checksum:
        raise CorruptJournal("AOF record checksum mismatch")
    return payload


def frame(payload: bytes, sequence: int) -> bytes:
    prefix = struct.pack(">IQ", len(payload), sequence)
    header = prefix + struct.pack(">I", zlib.crc32(prefix))
    digest = hashlib.sha256(header)
    digest.update(payload)
    return header + payload + digest.digest()


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        count = os.write(fd, view)
        if count <= 0:
            raise OSError("AOF write made no progress")
        view = view[count:]


def _read(fd: int, count: int) -> bytes:
    chunks: list[bytes] = []
    while count:
        chunk = os.read(fd, count)
        if not chunk:
            break
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def _lock(fd: int) -> None:
    if sys.platform == "win32":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


class FileJournal:
    """All methods run on one dedicated I/O thread, never in command handlers."""

    def __init__(self, config: ServerConfig, *, identity: bytes | None = None,
                 base_sequence: int = 0, minimum_sequence: int = 0,
                 require_existing: bool = False,
                 exclusive_new: bool = False) -> None:
        if config.aof_path is None:
            raise ValueError("journal requires an explicit path")
        self.path = Path(config.aof_path)
        self.config = config
        self.fd: int | None = None
        self.identity = identity
        self.base_sequence = base_sequence
        self.minimum_sequence = minimum_sequence
        self.require_existing = require_existing
        self.exclusive_new = exclusive_new
        prefix = MAGIC[:-3] + b"\x02\r\n" + (identity or b"") + struct.pack(">Q", base_sequence)
        self.header = MAGIC if identity is None else prefix + hashlib.sha256(prefix).digest()
        self.sequence = self.synced_sequence = base_sequence
        self.size = self.last_good = len(self.header)
        self.recovered = False
        self.repaired_bytes = 0
        self.dirty = False

    def open(self) -> None:
        if (not self.config.snapshot_enabled
                and self.path.with_name(self.path.name + ".lock").exists()):
            raise JournalError("snapshot-managed store requires snapshot mode")
        flags = os.O_RDWR | os.O_APPEND | getattr(os, "O_BINARY", 0)
        try:
            if self.require_existing:
                raise FileExistsError
            fd = os.open(self.path, flags | os.O_CREAT | os.O_EXCL, 0o600)
            created = True
        except FileExistsError:
            if self.exclusive_new:
                raise
            fd = os.open(self.path, flags)
            created = False
        self.fd = fd
        success = False
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise JournalError("AOF path is not a regular file")
            _lock(fd)
            if created:
                _write_all(fd, self.header)
                self.dirty = True
                if self.config.aof_fsync == "always":
                    self.sync()
                if sys.platform != "win32":
                    directory = os.open(self.path.parent,
                                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            os.lseek(fd, 0, os.SEEK_SET)
            if _read(fd, len(self.header)) != self.header:
                raise CorruptJournal("unknown/truncated AOF header or version")
            self.size = os.fstat(fd).st_size
            success = True
        finally:
            if not success:
                self.close()

    def _descriptor(self) -> int:
        if self.fd is None:
            raise JournalError("AOF file is closed")
        return self.fd

    def _tail(self) -> None:
        if self.sequence < self.minimum_sequence:
            raise CorruptJournal("incomplete committed cutover prefix")
        if not self.config.aof_repair_tail:
            raise TornTail("incomplete AOF tail; explicit repair required")
        fd = self._descriptor()
        self.repaired_bytes = os.fstat(fd).st_size - self.last_good
        os.ftruncate(fd, self.last_good)
        os.lseek(fd, self.last_good, os.SEEK_SET)
        os.fsync(fd)
        self.synced_sequence = self.sequence
        self.dirty = False
        self.size = self.last_good
        self.recovered = True

    def next_group(self) -> Group | None:
        fd = self._descriptor()
        try:
            payload = read_frame(fd, self.config, self.sequence + 1)
        except TornTail:
            self._tail()
            return None
        if payload is None:
            if self.sequence < self.minimum_sequence:
                raise CorruptJournal("missing committed cutover prefix")
            self.recovered = True
            self.dirty = self.dirty or self.sequence > 0  # prior sync status is not knowable
            return None
        group = decode_group(payload, self.config)
        self.sequence += 1
        self.last_good = os.lseek(fd, 0, os.SEEK_CUR)
        return group

    def append(self, payload: bytes) -> None:
        if not self.recovered:
            raise JournalError("AOF replay must complete before appending")
        if len(payload) > self.config.aof_max_record_bytes:
            raise CapacityError("AOF record exceeds configured limit")
        if self.sequence == 2**64 - 1:
            raise JournalError("AOF sequence exhausted")
        record = frame(payload, self.sequence + 1)
        _write_all(self._descriptor(), record)
        self.sequence += 1
        self.size += len(record)
        self.dirty = True
        if self.config.aof_fsync == "always":
            self.sync()

    def sync(self) -> None:
        os.fsync(self._descriptor())
        self.synced_sequence = self.sequence
        self.dirty = False

    def rewind(self) -> None:
        os.lseek(self._descriptor(), 0, os.SEEK_SET)
        if _read(self._descriptor(), len(self.header)) != self.header:
            raise CorruptJournal("generation journal header changed")
        self.sequence = self.base_sequence
        self.recovered = False

    def close(self) -> None:
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)  # descriptor close releases the exclusive lock
