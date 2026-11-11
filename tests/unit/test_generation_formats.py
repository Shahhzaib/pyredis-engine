from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.store import StringValue
from pyredis_engine.persistence.codec import Entry, Group, encode_group
from pyredis_engine.persistence.errors import CapacityError, CorruptJournal, JournalError
from pyredis_engine.persistence.generations import (
    Generation,
    GenerationFiles,
    Manifest,
    decode_manifest,
    encode_manifest,
)
from pyredis_engine.persistence.journal import FileJournal

CONFIG = ServerConfig()
STORE = b's' * 16
MANIFEST = Manifest(STORE, Generation(1, b'n' * 16, 10, 12),
                    (Generation(0, b'o' * 16, 0, 10),))


def test_manifest_roundtrip() -> None:
    assert decode_manifest(encode_manifest(MANIFEST, CONFIG), STORE, CONFIG) == MANIFEST


@pytest.mark.parametrize('damage', ['magic', 'identity', 'digest', 'truncated', 'extra',
                                   'count', 'kind', 'duplicate', 'base', 'order'])
def test_manifest_rejects_corrupt_or_incompatible_input(damage: str) -> None:
    raw = bytearray(encode_manifest(MANIFEST, CONFIG))
    if damage == 'magic':
        raw[9] = 9
    elif damage == 'identity':
        raw[12] ^= 1
    elif damage == 'digest':
        raw[-1] ^= 1
    elif damage == 'truncated':
        raw.pop()
    elif damage == 'extra':
        raw.extend(b'x')
    elif damage == 'count':
        raw[28:32] = (2**32 - 1).to_bytes(4, 'big')
    else:
        if damage == 'kind':
            raw[32] = 0
        elif damage == 'duplicate':
            raw[66:82] = raw[33:49]
        elif damage == 'base':
            raw[49:57] = (13).to_bytes(8, 'big')
        else:
            raw[90:98] = (13).to_bytes(8, 'big')
        raw[-32:] = hashlib.sha256(raw[:-32]).digest()
    with pytest.raises(JournalError):
        decode_manifest(bytes(raw), STORE, CONFIG)


def test_generation_header_and_committed_cutover_floor(tmp_path: Path) -> None:
    config = replace(CONFIG, aof_path=str(tmp_path / 'gen.aof'), snapshot_enabled=True,
                     aof_fsync='always', aof_repair_tail=True)
    identity = STORE + b'n' * 16
    writer = FileJournal(config, identity=identity, base_sequence=10)
    try:
        writer.open()
        assert writer.next_group() is None
        payload = encode_group(Group(False, (Entry(b'k', StringValue(b'1'), None),)), config)
        writer.append(payload)
        writer.sync()
    finally:
        writer.close()
    original = Path(config.aof_path).read_bytes()
    Path(config.aof_path).write_bytes(original[:-1])
    reader = FileJournal(config, identity=identity, base_sequence=10, minimum_sequence=11)
    try:
        reader.open()
        with pytest.raises(CorruptJournal, match='cutover'):
            reader.next_group()
    finally:
        reader.close()
    assert Path(config.aof_path).read_bytes() == original[:-1]
    Path(config.aof_path).write_bytes(original)
    wrong = FileJournal(config, identity=STORE + b'x' * 16, base_sequence=10)
    with pytest.raises(CorruptJournal):
        wrong.open()


@pytest.mark.parametrize('field,value', [('snapshot_enabled', 1), ('compact_on_start', 1),
    ('snapshot_max_bytes', 127), ('snapshot_max_bytes', True),
    ('snapshot_max_tail_bytes', 0), ('snapshot_max_generations', 1),
    ('snapshot_max_generations', 1025)])
def test_snapshot_configuration_bounds(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(CONFIG, **{field: value})


def test_snapshot_configuration_dependencies() -> None:
    with pytest.raises(ValueError):
        replace(CONFIG, snapshot_enabled=True)
    with pytest.raises(ValueError):
        replace(CONFIG, compact_on_start=True)


def test_manifest_capacity_is_bounded() -> None:
    with pytest.raises(CapacityError):
        encode_manifest(replace(MANIFEST, retained=()), CONFIG)


def test_paths_and_missing_root_are_rejected(tmp_path: Path) -> None:
    config = replace(CONFIG, aof_path=str(tmp_path / 'store'), snapshot_enabled=True)
    files = GenerationFiles(config)
    with pytest.raises(CorruptJournal):
        files.path(b'../escape', 'aof')
    try:
        files.open_lease()
    finally:
        files.close()
    with pytest.raises(CorruptJournal, match='missing snapshot root'):
        files.open_lease()
