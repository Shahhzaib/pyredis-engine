from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.store import StringValue
from pyredis_engine.persistence import journal
from pyredis_engine.persistence.codec import Entry, Group, encode_group
from pyredis_engine.persistence.errors import CapacityError, CorruptJournal, TornTail
from pyredis_engine.persistence.journal import MAGIC, FileJournal, frame


def payload(value: bytes = b"v") -> bytes:
    return encode_group(Group(False, (Entry(b"k", StringValue(value), None),)), ServerConfig())


def test_ordered_records_short_writes_and_lock_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ServerConfig(aof_path=str(tmp_path / "journal.aof"), aof_fsync="always")
    owner = FileJournal(config)
    owner.open()
    try:
        assert owner.next_group() is None
        original = os.write
        monkeypatch.setattr(journal.os, "write", lambda fd, data: original(fd, data[:3]))
        owner.append(payload())
        owner.append(payload(b"new"))
        assert owner.sequence == owner.synced_sequence == 2
        second = FileJournal(config)
        with pytest.raises(OSError):
            second.open()
        assert second.fd is None
    finally:
        owner.close()
    restored = FileJournal(config)
    restored.open()
    try:
        assert restored.next_group().entries[0].value == StringValue(b"v")
        assert restored.next_group().entries[0].value == StringValue(b"new")
        assert restored.next_group() is None
    finally:
        restored.close()


@pytest.mark.parametrize("cut", [1, 10, 17, 30, 60])
def test_incomplete_transaction_tail_requires_explicit_repair(tmp_path: Path, cut: int) -> None:
    path = tmp_path / "journal.aof"
    good = MAGIC + frame(payload(), 1)
    incomplete = frame(payload(b"unacknowledged transaction"), 2)[:cut]
    path.write_bytes(good + incomplete)
    owner = FileJournal(ServerConfig(aof_path=str(path)))
    owner.open()
    try:
        assert owner.next_group() is not None
        with pytest.raises(TornTail):
            owner.next_group()
    finally:
        owner.close()
    assert path.read_bytes() == good + incomplete
    repaired = FileJournal(ServerConfig(aof_path=str(path), aof_repair_tail=True))
    repaired.open()
    try:
        assert repaired.next_group() is not None
        assert repaired.next_group() is None
        assert repaired.repaired_bytes == len(incomplete)
        repaired.append(payload(b"after repair"))
    finally:
        repaired.close()
    assert path.read_bytes() == good + frame(payload(b"after repair"), 2)


@pytest.mark.parametrize("damage", [
    "header", "payload", "checksum", "sequence", "version", "schema",
])
def test_corruption_never_repaired_even_with_valid_following_record(
    tmp_path: Path, damage: str,
) -> None:
    path = tmp_path / "journal.aof"
    record = bytearray(frame(payload(), 1))
    if damage == "header":
        record[0] ^= 1  # header CRC distinguishes length damage from an incomplete tail
    elif damage == "payload":
        record[20] ^= 1
    elif damage == "checksum":
        record[-1] ^= 1
    elif damage == "sequence":
        record = bytearray(frame(payload(), 2))
    elif damage == "schema":
        effect = bytearray(payload())
        effect[10] = 255
        record = bytearray(frame(bytes(effect), 1))
    magic = MAGIC if damage != "version" else MAGIC.replace(b"\x01", b"\x02")
    original = magic + bytes(record) + frame(payload(b"later"), 2)
    path.write_bytes(original)
    owner = FileJournal(ServerConfig(aof_path=str(path), aof_repair_tail=True))
    try:
        with pytest.raises(CorruptJournal):
            owner.open()
            owner.next_group()
    finally:
        owner.close()
    assert path.read_bytes() == original


def test_untrusted_length_is_rejected_before_payload_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.aof"
    prefix = struct.pack(">IQ", 2**32 - 1, 1)
    original = MAGIC + prefix + struct.pack(">I", zlib.crc32(prefix))
    path.write_bytes(original)
    owner = FileJournal(ServerConfig(aof_path=str(path), aof_repair_tail=True))
    owner.open()
    requests = []
    read = os.read

    def observe(fd: int, count: int) -> bytes:
        requests.append(count)
        return read(fd, count)

    monkeypatch.setattr(journal.os, "read", observe)
    try:
        with pytest.raises(CapacityError):
            owner.next_group()
        assert max(requests) == 16
    finally:
        owner.close()
    assert path.read_bytes() == original


def test_zero_progress_write_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    owner = FileJournal(ServerConfig(aof_path=str(tmp_path / "journal.aof")))
    owner.open()
    try:
        assert owner.next_group() is None
        monkeypatch.setattr(journal.os, "write", lambda fd, data: 0)
        with pytest.raises(OSError, match="no progress"):
            owner.append(payload())
    finally:
        owner.close()


@pytest.mark.parametrize("length", [0, 1, 5, 11])
def test_existing_truncated_header_is_never_reinitialized(tmp_path: Path, length: int) -> None:
    path = tmp_path / "journal.aof"
    damaged = MAGIC[:length]
    path.write_bytes(damaged)
    owner = FileJournal(ServerConfig(aof_path=str(path), aof_repair_tail=True))
    with pytest.raises(CorruptJournal, match="header"):
        owner.open()
    assert owner.fd is None
    assert path.read_bytes() == damaged


@pytest.mark.parametrize("changes", [
    {"aof_path": ""}, {"aof_path": " "}, {"aof_path": "\x00"}, {"aof_path": False},
    {"aof_fsync": "maybe"}, {"aof_repair_tail": True}, {"aof_repair_tail": 1},
    {"aof_max_record_bytes": 1}, {"aof_max_record_bytes": 2**32},
    {"aof_max_dataset_bytes": True}, {"aof_max_keys": 0},
    {"aof_sync_interval": 0}, {"aof_sync_interval": float("inf")},
    {"aof_sync_interval": float("nan")}, {"aof_sync_interval": True},
])
def test_persistence_configuration_is_validated(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ServerConfig(**changes)  # type: ignore[arg-type]
