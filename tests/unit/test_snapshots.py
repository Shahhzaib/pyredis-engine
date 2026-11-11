from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.store import StringValue
from pyredis_engine.persistence.codec import Entry
from pyredis_engine.persistence.errors import JournalError
from pyredis_engine.persistence.snapshot import HEADER, MAGIC, SnapshotReader, write_snapshot

STORE, GENERATION = b"s" * 16, b"g" * 16


@pytest.mark.parametrize("entries", [(), (Entry(b"\x00\xff", StringValue(b"\r\n\x00"), 1234),)])
def test_snapshot_roundtrip_and_boundary(tmp_path: Path, entries: tuple[Entry, ...]) -> None:
    path = tmp_path / "snapshot"
    config = ServerConfig()
    write_snapshot(path, STORE, GENERATION, 123, iter(entries), len(entries), config)
    reader = SnapshotReader(path, STORE, GENERATION, 123, config)
    reader.open()
    try:
        decoded = []
        while (group := reader.next_group()) is not None:
            decoded.extend(group.entries)
        assert tuple(decoded) == entries
    finally:
        reader.close()


@pytest.mark.parametrize("damage", ["header", "body", "checksum", "truncated", "extra"])
def test_snapshot_integrity_rejects_damaged_file(tmp_path: Path, damage: str) -> None:
    path = tmp_path / "snapshot"
    write_snapshot(path, STORE, GENERATION, 1, iter((Entry(b"k", StringValue(b"v"), None),)),
                   1, ServerConfig())
    raw = bytearray(path.read_bytes())
    if damage == "truncated":
        raw = raw[:-1]
    elif damage == "extra":
        raw += b"garbage"
    else:
        raw[{"header": 0, "body": 65, "checksum": -1}[damage]] ^= 1
    path.write_bytes(raw)
    reader = SnapshotReader(path, STORE, GENERATION, 1, ServerConfig())
    try:
        with pytest.raises(JournalError):
            reader.open()
            while reader.next_group() is not None:
                pass
    finally:
        reader.close()


def test_snapshot_identity_and_limits(tmp_path: Path) -> None:
    path = tmp_path / "snapshot"
    write_snapshot(path, STORE, GENERATION, 1, iter(()), 0, ServerConfig())
    reader = SnapshotReader(path, STORE, b"x" * 16, 1, ServerConfig())
    try:
        with pytest.raises(JournalError):
            reader.open()
    finally:
        reader.close()


@pytest.mark.parametrize('damage', ['count', 'size', 'key_limit', 'record_limit'])
def test_snapshot_untrusted_lengths_and_recovery_limits(tmp_path: Path, damage: str) -> None:
    path = tmp_path / 'snapshot'
    entries = (Entry(b'a', StringValue(b'x' * 1001), None),
               Entry(b'b', StringValue(b'v'), None))
    config = ServerConfig()
    write_snapshot(path, STORE, GENERATION, 1, iter(entries), 2, config)
    if damage == 'count':
        raw = path.read_bytes()
        path.write_bytes(HEADER.pack(MAGIC, STORE, GENERATION, 1, 2**64 - 1) + raw[HEADER.size:])
    elif damage == 'size':
        config = replace(config, snapshot_max_bytes=128)
    elif damage == 'key_limit':
        config = replace(config, aof_max_keys=1)
    else:
        config = replace(config, aof_max_record_bytes=1024)
    before = path.read_bytes()
    reader = SnapshotReader(path, STORE, GENERATION, 1, config)
    try:
        with pytest.raises(JournalError):
            reader.open()
            while reader.next_group() is not None:
                pass
    finally:
        reader.close()
    assert path.read_bytes() == before
