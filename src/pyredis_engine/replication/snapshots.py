"""Off-loop snapshot exports/imports use private executors, never the live store."""
from __future__ import annotations

from pathlib import Path

from ..config import ServerConfig
from ..engine.executor import Executor
from ..persistence.codec import Entry
from ..persistence.effects import Dataset
from ..persistence.filesystem import atomic_replace, sync_directory
from ..persistence.generations import GenerationFiles, Pinned
from ..persistence.snapshot import SnapshotReader, write_snapshot
from .wire import SnapshotInfo


def export_snapshot(files: GenerationFiles, pinned: Pinned, uid: bytes, path: Path) -> None:
    engine, dataset = Executor(files.config), Dataset(files.config)
    try:
        files._stage('replication_snapshot')
        files._restore_prefix(pinned, engine, dataset)
        entries = (Entry(key, *engine.store.physical_entry(key)) for key in sorted(dataset.sizes))
        write_snapshot(path, files.store_id, uid, pinned.sequence, entries,
                       len(dataset.sizes), files.config)
        files.verify_snapshot(path, uid, pinned.sequence)
    finally:
        engine.close()


def prepare_snapshot(source: Path, info: SnapshotInfo, files: GenerationFiles,
                     uid: bytes, sequence: int, config: ServerConfig) -> None:
    engine, dataset = Executor(config), Dataset(config)
    reader = SnapshotReader(source, info.store_id, info.generation, info.sequence, config)
    try:
        reader.open()
        while (group := reader.next_group()) is not None:
            dataset.apply(group)
            engine.restore_group(group)
        temporary = files.path(uid, 'snap.tmp')
        entries = (Entry(key, *engine.store.physical_entry(key)) for key in sorted(dataset.sizes))
        write_snapshot(temporary, files.store_id, uid, sequence, entries,
                       len(dataset.sizes), config)
        files.verify_snapshot(temporary, uid, sequence)
        atomic_replace(temporary, files.path(uid, 'snap'))
        sync_directory(files.root.parent)
    finally:
        reader.close()
        engine.close()
