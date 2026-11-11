"""Manifest-owned generations; no scanning for a guessed recovery candidate."""
from __future__ import annotations

import hashlib
import os
import struct
import uuid
from dataclasses import dataclass, replace
from pathlib import Path

from ..config import ServerConfig
from ..engine.executor import Executor
from .codec import Entry, decode_group
from .effects import Dataset
from .errors import CapacityError, CorruptJournal, JournalError, SourceCorruption
from .filesystem import atomic_replace, sync_directory
from .journal import FileJournal, _lock, _read, _write_all, read_frame
from .snapshot import SnapshotReader, write_snapshot

MANIFEST_MAGIC = b"PYREDMAN\x00\x01\r\n"
LEASE_MAGIC = b"PYREDLCK\x00\x01\r\n"
DESCRIPTOR = struct.Struct(">B16sQQ")


@dataclass(frozen=True)
class Generation:
    kind: int
    uid: bytes
    base: int
    minimum: int


@dataclass(frozen=True)
class Manifest:
    store_id: bytes
    current: Generation
    retained: tuple[Generation, ...]


@dataclass(frozen=True)
class Pinned:
    path: Path
    identity: tuple[int, int]
    header_size: int
    end: int
    sequence: int
    base: int
    previous: Generation | None


@dataclass(frozen=True)
class CompactionResult:
    activated: bool
    base_sequence: int = 0
    cutover_sequence: int = 0
    cleanup_deferred: bool = False
    error: str | None = None


def encode_manifest(manifest: Manifest, config: ServerConfig) -> bytes:
    generations = (manifest.current, *manifest.retained)
    if not 2 <= len(generations) <= config.snapshot_max_generations:
        raise CapacityError("manifest generation count exceeds limit")
    body = bytearray(MANIFEST_MAGIC + manifest.store_id + struct.pack(">I", len(generations)))
    for generation in generations:
        body.extend(DESCRIPTOR.pack(generation.kind, generation.uid,
                                    generation.base, generation.minimum))
    return bytes(body) + hashlib.sha256(body).digest()


def decode_manifest(raw: bytes, store_id: bytes, config: ServerConfig) -> Manifest:
    if len(raw) < 64 or raw[:12] != MANIFEST_MAGIC or raw[12:28] != store_id:
        raise CorruptJournal("manifest identity/version mismatch")
    count = int(struct.unpack(">I", raw[28:32])[0])
    if not 2 <= count <= config.snapshot_max_generations:
        raise CapacityError("manifest generation count exceeds limit")
    if len(raw) != 32 + count * DESCRIPTOR.size + 32:
        raise CorruptJournal("manifest length mismatch")
    if hashlib.sha256(raw[:-32]).digest() != raw[-32:]:
        raise CorruptJournal("manifest checksum mismatch")
    generations = tuple(Generation(*DESCRIPTOR.unpack_from(raw, 32 + n * DESCRIPTOR.size))
                        for n in range(count))
    if (generations[0].kind != 1 or len({g.uid for g in generations}) != count
            or any(g.kind not in (0, 1) or g.base > g.minimum or g.kind == 0 and g.base != 0
                   for g in generations)):
        raise CorruptJournal("invalid manifest generation descriptors")
    if any(new.minimum < old.minimum
           for new, old in zip(generations, generations[1:], strict=False)):
        raise CorruptJournal("unordered manifest generations")
    return Manifest(store_id, generations[0], generations[1:])


class GenerationFiles:
    def __init__(self, config: ServerConfig) -> None:
        if config.aof_path is None:
            raise ValueError("snapshots require a journal path")
        root = Path(config.aof_path).absolute()
        self.root = root.parent.resolve() / root.name
        self.config = config
        self.store_id = b""
        self.lease: int | None = None
        self.manifest: Manifest | None = None
        self.activation_attempted = False
        self.directory_durable = False

    def _stage(self, name: str) -> None:
        """Named I/O boundaries for deterministic faults and owned kill tests."""

    def path(self, uid: bytes, suffix: str) -> Path:
        if len(uid) != 16 or suffix not in ("aof", "snap", "snap.tmp", "manifest.tmp"):
            raise CorruptJournal("invalid generation name")
        target = self.root.parent / (self.root.name + ".g-" + uid.hex() + "." + suffix)
        if target.parent != self.root.parent or target.is_symlink():
            raise CorruptJournal("unsafe generation path")
        return target

    def open_lease(self) -> None:
        lease = self.root.with_name(self.root.name + ".lock")
        if self.root.is_symlink() or lease.is_symlink():
            raise CorruptJournal("snapshot root/lease must not be symlinks")
        flags = os.O_RDWR | getattr(os, "O_BINARY", 0)
        if not lease.exists() and self.root.exists():
            with self.root.open('rb') as stream:
                if stream.read(12) == MANIFEST_MAGIC:
                    raise CorruptJournal("missing lease for snapshot manifest")
        try:
            fd = os.open(lease, flags | os.O_CREAT | os.O_EXCL, 0o600)
            created = True
        except FileExistsError:
            if not self.root.exists():
                raise CorruptJournal("missing snapshot root with existing lease") from None
            fd = os.open(lease, flags)
            created = False
        self.lease = fd
        _lock(fd)
        os.lseek(fd, 0, os.SEEK_SET)
        if created:
            self.store_id = uuid.uuid4().bytes
            body = LEASE_MAGIC + self.store_id
            _write_all(fd, body + hashlib.sha256(body).digest())
            os.fsync(fd)
            sync_directory(self.root.parent)
        else:
            raw = _read(fd, 61)
            if (len(raw) != 60 or raw[:12] != LEASE_MAGIC
                    or hashlib.sha256(raw[:28]).digest() != raw[28:]):
                raise CorruptJournal("invalid snapshot lease identity")
            self.store_id = raw[12:28]

    def close(self) -> None:
        if self.lease is not None:
            fd, self.lease = self.lease, None
            os.close(fd)

    def read_manifest(self, path: Path | None = None) -> Manifest | None:
        path = path or self.root
        if not path.exists():
            return None
        with path.open("rb") as stream:
            prefix = stream.read(12)
            if prefix != MANIFEST_MAGIC:
                return None
            raw = prefix + stream.read(65536)
        manifest = decode_manifest(raw, self.store_id, self.config)
        if path == self.root:
            self.manifest = manifest
        return manifest

    def current_journal(self) -> FileJournal:
        if self.manifest is None:
            return FileJournal(self.config)
        current = self.manifest.current
        return FileJournal(replace(self.config, aof_path=str(self.path(current.uid, "aof"))),
                           identity=self.store_id + current.uid, base_sequence=current.base,
                           minimum_sequence=current.minimum,
                           require_existing=True)

    def snapshot_reader(self, generation: Generation) -> SnapshotReader:
        return SnapshotReader(self.path(generation.uid, "snap"), self.store_id, generation.uid,
                              generation.base, self.config)

    def pin(self, journal: FileJournal) -> Pinned:
        journal.sync()
        info = os.fstat(journal._descriptor())
        return Pinned(journal.path, (info.st_dev, info.st_ino), len(journal.header), journal.size,
                      journal.sequence, journal.base_sequence,
                      None if self.manifest is None else self.manifest.current)

    def _restore_prefix(self, pinned: Pinned, engine: Executor, dataset: Dataset) -> None:
        reader = self.snapshot_reader(pinned.previous) if pinned.previous is not None else None
        try:
            if reader is not None:
                reader.open()
                while (group := reader.next_group()) is not None:
                    dataset.apply(group)
                    engine.restore_group(group)
            fd = os.open(pinned.path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
            try:
                info = os.fstat(fd)
                if (info.st_dev, info.st_ino) != pinned.identity:
                    raise CorruptJournal("pinned journal identity changed")
                os.lseek(fd, pinned.header_size, os.SEEK_SET)
                sequence = pinned.base
                while (payload := read_frame(
                        fd, self.config, sequence + 1, pinned.end)) is not None:
                    group = decode_group(payload, self.config)
                    dataset.apply(group)
                    engine.restore_group(group)
                    sequence += 1
                if sequence != pinned.sequence:
                    raise CorruptJournal("pinned sequence mismatch")
            finally:
                os.close(fd)
        except (OSError, JournalError) as error:
            raise SourceCorruption(
                "authoritative snapshot/journal prefix failed validation") from error
        finally:
            if reader is not None:
                reader.close()

    def build(self, pinned: Pinned, uid: bytes) -> None:
        self._stage("snapshot_rebuild")
        engine, dataset = Executor(self.config), Dataset(self.config)
        try:
            self._restore_prefix(pinned, engine, dataset)
            entries = (Entry(key, *engine.store.physical_entry(key))
                       for key in sorted(dataset.sizes))
            temporary = self.path(uid, "snap.tmp")
            write_snapshot(temporary, self.store_id, uid, pinned.sequence, entries,
                           len(dataset.sizes), self.config, self._stage)
            self._stage("snapshot_verify")
            self.verify_snapshot(temporary, uid, pinned.sequence)
            self._stage("snapshot_replace")
            atomic_replace(temporary, self.path(uid, "snap"))
            self._stage("snapshot_directory_sync")
            sync_directory(self.root.parent)
        finally:
            engine.close()

    def verify_snapshot(self, path: Path, uid: bytes, base: int) -> None:
        reader = SnapshotReader(path, self.store_id, uid, base, self.config)
        try:
            reader.open()
            while reader.next_group() is not None:
                pass
        finally:
            reader.close()

    def activate(self, pinned: Pinned, uid: bytes, source: FileJournal, *,
                 replacement: bool = False) -> FileJournal:
        self.activation_attempted = False
        end = source.size
        cutoff = pinned.sequence if replacement else source.sequence
        if end - pinned.end > self.config.snapshot_max_tail_bytes:
            raise CapacityError("snapshot journal tail exceeds cutover limit")
        self._stage("snapshot_final_verify")
        self.verify_snapshot(self.path(uid, "snap"), uid, pinned.sequence)
        self._stage("journal_rotation")
        target = self.path(uid, "aof")
        candidate = FileJournal(replace(self.config, aof_path=str(target), aof_fsync="none"),
                                identity=self.store_id + uid, base_sequence=pinned.sequence,
                                exclusive_new=True)
        handed_over = False
        try:
            candidate.open()
            assert candidate.next_group() is None
            fd = os.open(source.path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
            try:
                info = os.fstat(fd)
                if (info.st_dev, info.st_ino) != pinned.identity:
                    raise SourceCorruption("cutover journal identity changed")
                os.lseek(fd, pinned.end, os.SEEK_SET)
                sequence = pinned.sequence
                self._stage("journal_tail")
                while not replacement:
                    try:
                        payload = read_frame(fd, self.config, sequence + 1, end)
                        if payload is not None:
                            decode_group(payload, self.config)
                    except (OSError, JournalError) as error:
                        raise SourceCorruption("cutover source journal is corrupt") from error
                    if payload is None:
                        break
                    candidate.append(payload)
                    sequence += 1
                if sequence != cutoff:
                    raise CorruptJournal("cutover sequence mismatch")
            finally:
                os.close(fd)
            self._stage("journal_sync")
            candidate.sync()
            candidate.minimum_sequence = cutoff
            candidate.rewind()
            self._stage("journal_verify")
            while candidate.next_group() is not None:
                pass
            candidate.dirty = False  # The verified cutover bytes were just synced.
            candidate.config = replace(self.config, aof_path=str(target))
            self._stage("journal_directory_sync")
            sync_directory(self.root.parent)
            history: tuple[Generation, ...]
            if self.manifest is None:
                old = Generation(0, uuid.uuid4().bytes, 0, source.sequence)
                self._stage("backup_create")
                os.link(self.root, self.path(old.uid, "aof"))
                sync_directory(self.root.parent)
                history = (old,)
            else:
                old = replace(self.manifest.current, minimum=source.sequence)
                history = (old, *(g for g in self.manifest.retained
                                  if self.path(g.uid, "aof").exists()))
            manifest = Manifest(self.store_id, Generation(1, uid, pinned.sequence, cutoff), history)
            raw = encode_manifest(manifest, self.config)
            self._stage("manifest_create")
            temporary = self.path(uid, "manifest.tmp")
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_BINARY", 0), 0o600)
            try:
                _write_all(fd, raw)
                self._stage("manifest_sync")
                os.fsync(fd)
            finally:
                os.close(fd)
            self._stage("manifest_verify")
            if self.read_manifest(temporary) != manifest:
                raise CorruptJournal("manifest verification mismatch")
            self._stage("manifest_activate")
            self.activation_attempted = True
            if self.manifest is None:
                # Windows forbids replacing an open destination. The stable lease
                # still excludes other M08 processes throughout this transition.
                self._stage("legacy_release")
                source.close()
            atomic_replace(temporary, self.root, existing=True)
            self._stage("manifest_activated")
            self._stage("directory_sync")
            self.directory_durable = sync_directory(self.root.parent)
            if self.read_manifest() != manifest:
                raise CorruptJournal("activated manifest mismatch")
            self._stage("journal_rotate")
            source.close()
            handed_over = True
            return candidate
        finally:
            if not handed_over:
                candidate.close()

    def cleanup(self) -> bool:
        self._stage("old_generation_cleanup")
        if not self.directory_durable or self.manifest is None:
            return False
        for generation in self.manifest.retained[1:]:
            suffixes = ("aof",) if generation.kind == 0 else ("snap", "aof")
            for suffix in suffixes:
                path = self.path(generation.uid, suffix)
                if path.exists():
                    if not path.is_file() or path.is_symlink():
                        raise CorruptJournal("unsafe retired generation file")
                    self._stage("cleanup_unlink")
                    path.unlink()
        sync_directory(self.root.parent)
        return True
