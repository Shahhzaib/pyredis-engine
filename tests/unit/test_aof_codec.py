from __future__ import annotations

from collections import deque

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.collections import HashValue, ListValue, SetValue, SortedSetValue
from pyredis_engine.engine.store import StringValue
from pyredis_engine.persistence.codec import Entry, Group, decode_group, encode_group
from pyredis_engine.persistence.errors import JournalError


def test_all_types_binary_deadlines_and_canonical_roundtrip() -> None:
    zset = SortedSetValue()
    for member, score in [(b"\xff", float("inf")), (b"a", 0.1), (b"\x00", 0.1)]:
        zset.update(member, score)
    group = Group(True, (
        Entry(b"\x00\xff", StringValue(b"\r\n\x00"), 1234567890123),
        Entry(b"hash", HashValue({b"\xff": b"", b"": b"\x00"}, 2), None),
        Entry(b"list", ListValue(deque([b"\xff", b"", b"\x00"]), 2), None),
        Entry(b"set", SetValue({b"", b"\xff", b"\x00"}, 2), None),
        Entry(b"zset", zset, None), Entry(b"deleted", None, None),
    ))
    encoded = encode_group(group, ServerConfig())
    decoded = decode_group(encoded, ServerConfig())
    assert decoded == group
    assert encode_group(decoded, ServerConfig()) == encoded
    assert decoded.entries[4].value.ranked == zset.ranked  # type: ignore[union-attr]


@pytest.mark.parametrize("payload", [b"", b"\x02\x00\x00\x00\x00", b"\x00\xff\xff\xff\xff"])
def test_invalid_flags_counts_and_truncation(payload: bytes) -> None:
    with pytest.raises(JournalError):
        decode_group(payload, ServerConfig())


def test_duplicate_keys_and_unknown_types_are_rejected() -> None:
    entry = Entry(b"k", StringValue(b"v"), None)
    with pytest.raises(JournalError):
        encode_group(Group(False, (entry, entry)), ServerConfig())
    encoded = bytearray(encode_group(Group(False, (entry,)), ServerConfig()))
    encoded[10] = 255  # clear/count, key length/key, then kind
    with pytest.raises(JournalError):
        decode_group(bytes(encoded), ServerConfig())


def test_replay_respects_collection_bulk_and_record_limits() -> None:
    group = Group(False, (Entry(b"k", ListValue(deque([b"a", b"b"]), 2), None),))
    encoded = encode_group(group, ServerConfig())
    with pytest.raises(JournalError, match="collection"):
        decode_group(encoded, ServerConfig(max_collection_items=1))
    large = Group(False, (Entry(b"k", StringValue(b"a" * 129), None),))
    with pytest.raises(JournalError, match="bulk"):
        decode_group(encode_group(large, ServerConfig()), ServerConfig(max_bulk_bytes=128))
