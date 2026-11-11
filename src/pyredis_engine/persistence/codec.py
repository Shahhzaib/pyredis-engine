"""Bounded binary typed effects. No pickle, eval, command replay or locale decoding."""

from __future__ import annotations

import math
import struct
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass

from ..config import ServerConfig
from ..engine.collections import (
    HashValue,
    ListValue,
    PendingEntry,
    SetValue,
    SortedSetValue,
    StreamEntry,
    StreamGroup,
    StreamValue,
    stream_metadata,
)
from ..engine.store import StoredValue, StringValue
from ..protocol import INT64_MAX, INT64_MIN
from .errors import CapacityError, CorruptJournal


@dataclass(frozen=True)
class Entry:
    key: bytes
    value: StoredValue | None
    expires_at_ms: int | None


@dataclass(frozen=True)
class Group:
    clear: bool
    entries: tuple[Entry, ...]


def entry_size(entry: Entry) -> int:
    size = 14 + len(entry.key)
    value = entry.value
    if value is None:
        return size
    if isinstance(value, StringValue):
        return size + 4 + len(value.data)
    if isinstance(value, HashValue):
        return size + 4 + value.byte_size + 8 * len(value.fields)
    if isinstance(value, (ListValue, SetValue)):
        count = len(value.items) if isinstance(value, ListValue) else len(value.members)
        return size + 4 + value.byte_size + 4 * count
    if isinstance(value, SortedSetValue):
        return size + 4 + value.byte_size + 4 * len(value.scores)
    if isinstance(value, StreamValue):
        total = size + 21 + value.byte_size + 20 * len(value.entries) + 8 * value.field_count
        if value.groups:
            total += 4
            for group in value.groups:
                total += 28 + len(group.name)
                total += sum(4 + len(consumer) for consumer in group.consumers)
                total += sum(36 + len(item.consumer) for item in group.pending)
        return total
    raise CorruptJournal("unsupported stored kind")


def _blob(value: bytes, config: ServerConfig) -> Iterator[bytes]:
    if len(value) > config.max_bulk_bytes:
        raise CapacityError("AOF bulk exceeds configured limit")
    yield struct.pack(">I", len(value))
    yield value


def _parts(entry: Entry, config: ServerConfig) -> Iterator[bytes]:
    yield from _blob(entry.key, config)
    value = entry.value
    kind = (
        0
        if value is None
        else 1
        if isinstance(value, StringValue)
        else 2
        if isinstance(value, HashValue)
        else 3
        if isinstance(value, ListValue)
        else 4
        if isinstance(value, SetValue)
        else 5
        if isinstance(value, SortedSetValue)
        else 6
        if isinstance(value, StreamValue)
        else -1
    )
    if kind < 0:
        raise CorruptJournal("unsupported stored kind")
    expiry = entry.expires_at_ms
    if expiry is not None and not INT64_MIN <= expiry <= INT64_MAX:
        raise CorruptJournal("AOF deadline out of range")
    if value is None and expiry is not None:
        raise CorruptJournal("deleted key has an expiration")
    yield bytes((kind, int(expiry is not None))) + struct.pack(">q", expiry or 0)
    if value is None:
        return
    if isinstance(value, StringValue):
        yield from _blob(value.data, config)
        return
    if isinstance(value, StreamValue):
        count = len(value.entries)
        units, raw_metadata, _ = stream_metadata(value)
        if (
            count + value.field_count + units > config.max_collection_items
            or value.byte_size + raw_metadata > config.max_collection_bytes
        ):
            raise CapacityError("AOF stream exceeds configured limit")
        if any(not 0 <= number <= 2**64 - 1 for number in value.last_id):
            raise CorruptJournal("invalid stream last ID")
        yield (b"\x02" if value.groups else b"\x01") + struct.pack(">QQI", *value.last_id, count)
        previous = (0, 0)
        actual_size = actual_fields = 0
        for stream_entry in value.entries:
            if (
                stream_entry.id <= previous
                or stream_entry.id > value.last_id
                or not stream_entry.fields
                or any(not 0 <= number <= 2**64 - 1 for number in stream_entry.id)
            ):
                raise CorruptJournal("invalid stream entry ID")
            previous = stream_entry.id
            actual_fields += len(stream_entry.fields)
            yield struct.pack(">QQI", *stream_entry.id, len(stream_entry.fields))
            for field, payload in stream_entry.fields:
                actual_size += len(field) + len(payload)
                yield from _blob(field, config)
                yield from _blob(payload, config)
        if actual_size != value.byte_size or actual_fields != value.field_count:
            raise CorruptJournal("invalid stream accounting")
        if value.groups:
            yield struct.pack(">I", len(value.groups))
            previous_group: bytes | None = None
            for group in value.groups:
                if (
                    previous_group is not None
                    and group.name <= previous_group
                    or any(not 0 <= number <= 2**64 - 1 for number in group.last_id)
                    or tuple(sorted(set(group.consumers))) != group.consumers
                ):
                    raise CorruptJournal("invalid stream group")
                previous_group = group.name
                yield from _blob(group.name, config)
                yield struct.pack(">QQI", *group.last_id, len(group.consumers))
                for consumer in group.consumers:
                    yield from _blob(consumer, config)
                yield struct.pack(">I", len(group.pending))
                previous_pending = (0, 0)
                for item in group.pending:
                    if (
                        item.id <= previous_pending
                        or item.consumer not in group.consumers
                        or any(not 0 <= number <= 2**64 - 1 for number in item.id)
                        or not INT64_MIN <= item.delivered_ms <= INT64_MAX
                        or not 1 <= item.deliveries <= 2**64 - 1
                    ):
                        raise CorruptJournal("invalid stream pending entry")
                    previous_pending = item.id
                    yield struct.pack(">QQ", *item.id)
                    yield from _blob(item.consumer, config)
                    yield struct.pack(">qQ", item.delivered_ms, item.deliveries)
        return
    if not isinstance(value, (HashValue, ListValue, SetValue, SortedSetValue)):
        raise CorruptJournal("unsupported stored kind")
    count = (
        len(value.fields)
        if isinstance(value, HashValue)
        else len(value.items)
        if isinstance(value, ListValue)
        else len(value.members)
        if isinstance(value, SetValue)
        else len(value.scores)
    )
    if count > config.max_collection_items or value.byte_size > config.max_collection_bytes:
        raise CapacityError("AOF collection exceeds configured limit")
    yield struct.pack(">I", count)
    if isinstance(value, HashValue):
        for field, payload in sorted(value.fields.items()):
            yield from _blob(field, config)
            yield from _blob(payload, config)
    elif isinstance(value, (ListValue, SetValue)):
        members = value.items if isinstance(value, ListValue) else sorted(value.members)
        for member in members:
            yield from _blob(member, config)
    elif isinstance(value, SortedSetValue):
        for member, score in sorted(value.scores.items()):
            if math.isnan(score):
                raise CorruptJournal("NaN in AOF score")
            yield from _blob(member, config)
            yield struct.pack(">d", 0.0 if score == 0 else score)


def encode_group(group: Group, config: ServerConfig) -> bytes:
    if len({entry.key for entry in group.entries}) != len(group.entries):
        raise CorruptJournal("duplicate AOF key")
    output = bytearray(bytes((int(group.clear),)) + struct.pack(">I", len(group.entries)))
    for entry in group.entries:
        for chunk in _parts(entry, config):
            if len(chunk) > config.aof_max_record_bytes - len(output):
                raise CapacityError("AOF record exceeds configured limit")
            output.extend(chunk)
    return bytes(output)


class _Reader:
    def __init__(self, data: bytes, config: ServerConfig) -> None:
        if len(data) > config.aof_max_record_bytes:
            raise CapacityError("AOF record exceeds configured limit")
        self.data, self.config, self.offset = data, config, 0

    def take(self, count: int) -> bytes:
        if count < 0 or count > len(self.data) - self.offset:
            raise CorruptJournal("truncated AOF effect")
        value = self.data[self.offset : self.offset + count]
        self.offset += count
        return value

    def number(self) -> int:
        return int(struct.unpack(">I", self.take(4))[0])

    def blob(self) -> bytes:
        size = self.number()
        if size > self.config.max_bulk_bytes:
            raise CapacityError("AOF bulk exceeds configured limit")
        return self.take(size)

    def value(self, kind: int) -> StoredValue | None:
        if kind == 0:
            return None
        if kind == 1:
            return StringValue(self.blob())
        if kind == 6:
            version = self.take(1)
            if version not in (b"\x01", b"\x02"):
                raise CorruptJournal("unsupported stream value version")
            last_ms, last_seq, count = struct.unpack(">QQI", self.take(20))
            if (
                count > self.config.max_collection_items
                or count > (len(self.data) - self.offset) // 20
            ):
                raise CapacityError("AOF stream exceeds configured limit")
            entries: list[StreamEntry] = []
            previous = (0, 0)
            size = fields_total = 0
            for _ in range(count):
                ms, seq, fields_count = struct.unpack(">QQI", self.take(20))
                identity = (ms, seq)
                if identity <= previous or identity > (last_ms, last_seq) or fields_count == 0:
                    raise CorruptJournal("invalid stream entry ID or fields")
                fields_total += fields_count
                if (
                    count + fields_total > self.config.max_collection_items
                    or fields_count > (len(self.data) - self.offset) // 8
                ):
                    raise CapacityError("AOF stream fields exceed configured limit")
                stream_fields: list[tuple[bytes, bytes]] = []
                for _ in range(fields_count):
                    field, payload = self.blob(), self.blob()
                    size += len(field) + len(payload)
                    if size > self.config.max_collection_bytes:
                        raise CapacityError("AOF stream bytes exceed configured limit")
                    stream_fields.append((field, payload))
                entries.append(StreamEntry(identity, tuple(stream_fields)))
                previous = identity
            groups: list[StreamGroup] = []
            if version == b"\x02":
                group_count = self.number()
                units = count + fields_total + group_count
                if group_count == 0 or units > self.config.max_collection_items:
                    raise CapacityError("AOF stream groups exceed configured limit")
                previous_group: bytes | None = None
                for _ in range(group_count):
                    name = self.blob()
                    if previous_group is not None and name <= previous_group:
                        raise CorruptJournal("invalid stream group ordering")
                    previous_group = name
                    last_group_id = struct.unpack(">QQ", self.take(16))
                    consumers_count = self.number()
                    units += consumers_count
                    if (
                        units > self.config.max_collection_items
                        or consumers_count > (len(self.data) - self.offset) // 4
                    ):
                        raise CapacityError("AOF stream consumers exceed configured limit")
                    consumers: list[bytes] = []
                    for _ in range(consumers_count):
                        consumer = self.blob()
                        if consumers and consumer <= consumers[-1]:
                            raise CorruptJournal("invalid stream consumer ordering")
                        consumers.append(consumer)
                    pending_count = self.number()
                    units += pending_count
                    if (
                        units > self.config.max_collection_items
                        or pending_count > (len(self.data) - self.offset) // 36
                    ):
                        raise CapacityError("AOF stream pending exceeds configured limit")
                    pending: list[PendingEntry] = []
                    previous_id = (0, 0)
                    owners = set(consumers)
                    for _ in range(pending_count):
                        pending_id = struct.unpack(">QQ", self.take(16))
                        owner = self.blob()
                        delivered, deliveries = struct.unpack(">qQ", self.take(16))
                        if pending_id <= previous_id or owner not in owners or not deliveries:
                            raise CorruptJournal("invalid stream pending entry")
                        pending.append(PendingEntry(pending_id, owner, delivered, deliveries))
                        previous_id = pending_id
                    groups.append(
                        StreamGroup(name, last_group_id, tuple(consumers), tuple(pending))
                    )
            value = StreamValue(
                tuple(entries), (last_ms, last_seq), size, fields_total, tuple(groups)
            )
            _, raw_metadata, _ = stream_metadata(value)
            if size + raw_metadata > self.config.max_collection_bytes:
                raise CapacityError("AOF stream metadata exceeds configured limit")
            return value
        if kind not in (2, 3, 4, 5):
            raise CorruptJournal("unsupported AOF value type")
        count = self.number()
        if count > self.config.max_collection_items:
            raise CapacityError("AOF collection exceeds configured limit")
        fields: dict[bytes, bytes] = {}
        members: list[bytes] = []
        scores: dict[bytes, float] = {}
        size = 0
        for _ in range(count):
            member = self.blob()
            if kind == 2:
                payload = self.blob()
                if member in fields:
                    raise CorruptJournal("duplicate AOF hash field")
                fields[member] = payload
                size += len(member) + len(payload)
            elif kind == 5:
                score = float(struct.unpack(">d", self.take(8))[0])
                if math.isnan(score) or member in scores:
                    raise CorruptJournal("invalid/duplicate AOF score")
                scores[member] = 0.0 if score == 0 else score
                size += len(member) + 8
            else:
                members.append(member)
                size += len(member)
            if size > self.config.max_collection_bytes:
                raise CapacityError("AOF collection exceeds configured limit")
        if kind == 2:
            return HashValue(fields, size)
        if kind == 3:
            return ListValue(deque(members), size)
        if kind == 4:
            unique = set(members)
            if len(unique) != count:
                raise CorruptJournal("duplicate AOF set member")
            return SetValue(unique, size)
        return SortedSetValue(scores, sorted((score, key) for key, score in scores.items()), size)


def decode_group(payload: bytes, config: ServerConfig) -> Group:
    reader = _Reader(payload, config)
    flag = reader.take(1)[0]
    if flag not in (0, 1):
        raise CorruptJournal("invalid AOF clear flag")
    count = reader.number()
    if count > (len(payload) - reader.offset) // 14:
        raise CorruptJournal("invalid AOF effect count")
    entries: list[Entry] = []
    keys: set[bytes] = set()
    for _ in range(count):
        key = reader.blob()
        if key in keys:
            raise CorruptJournal("duplicate AOF key")
        keys.add(key)
        kind, expiry_flag = reader.take(2)
        expiry = int(struct.unpack(">q", reader.take(8))[0])
        if expiry_flag not in (0, 1) or (expiry_flag == 0 and expiry != 0):
            raise CorruptJournal("invalid AOF expiration")
        value = reader.value(kind)
        if value is None and expiry_flag:
            raise CorruptJournal("deleted key has an expiration")
        entries.append(Entry(key, value, expiry if expiry_flag else None))
    if reader.offset != len(payload):
        raise CorruptJournal("trailing bytes in AOF group")
    return Group(bool(flag), tuple(entries))
