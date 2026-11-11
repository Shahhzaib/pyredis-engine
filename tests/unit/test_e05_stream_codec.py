from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.collections import PendingEntry, StreamEntry, StreamGroup, StreamValue
from pyredis_engine.persistence.codec import Entry, Group, decode_group, encode_group
from pyredis_engine.persistence.errors import CorruptJournal, JournalError


def test_stream_postimage_preserves_binary_entries_and_last_id() -> None:
    value = StreamValue((StreamEntry((7, 1), ((b"\x00", b"\xff"), (b"\x00", b""))),), (9, 0), 3, 2)
    group = Group(False, (Entry(b"stream", value, 1234),))
    assert decode_group(encode_group(group, ServerConfig()), ServerConfig()) == group


def test_empty_stream_retains_last_generated_id_and_rejects_unknown_version() -> None:
    group = Group(False, (Entry(b"s", StreamValue((), (7, 4), 0, 0), None),))
    encoded = encode_group(group, ServerConfig())
    assert decode_group(encoded, ServerConfig()) == group
    damaged = bytearray(encoded)
    damaged[20] = 99  # group header, key blob, kind/expiry, then stream version
    with pytest.raises(CorruptJournal):
        decode_group(bytes(damaged), ServerConfig())


def test_group_postimage_roundtrip_and_version_validation() -> None:
    group = StreamGroup(b"g\x00", (7, 1), (b"c\xff",), (PendingEntry((7, 1), b"c\xff", 123, 2),))
    value = StreamValue((StreamEntry((7, 1), ((b"f", b"v"),)),), (7, 1), 2, 1, (group,))
    encoded = encode_group(Group(False, (Entry(b"s", value, None),)), ServerConfig())
    assert decode_group(encoded, ServerConfig()) == Group(False, (Entry(b"s", value, None),))
    bad = bytearray(encoded)
    bad[20] = 99
    with pytest.raises(CorruptJournal):
        decode_group(bytes(bad), ServerConfig())
    for length in range(len(encoded)):
        with pytest.raises(JournalError):
            decode_group(encoded[:length], ServerConfig())
