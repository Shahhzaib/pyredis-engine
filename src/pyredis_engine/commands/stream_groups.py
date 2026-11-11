"""Bounded, nonblocking single-stream consumer-group subset."""

from __future__ import annotations

from bisect import bisect_left
from collections import Counter
from dataclasses import dataclass, replace

from ..config import ServerConfig
from ..engine.collections import PendingEntry, StreamGroup, StreamValue, stream_metadata
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import INT64_MAX, INT64_MIN, Array, Bulk, Integer, Reply, Simple, reply_size
from .collection_helpers import check_elements
from .numbers import integer
from .registry import Command, CommandError
from .streams import _entry_reply, _range_bound, encode_id, parse_id

MAX_PENDING_RESULTS = 1024
MAX_PENDING_VISITS = 4096

MISSING_KEY = (
    b"ERR The XGROUP subcommand requires the key to exist. Note that for "
    b"CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
)


def _safe(value: bytes) -> bytes:
    return value if len(value) <= 128 and all(32 <= byte < 127 for byte in value) else b"<binary>"


def _missing(key: bytes, name: bytes, *, read: bool = False) -> CommandError:
    suffix = b" in XREADGROUP with GROUP option" if read else b""
    return CommandError(
        b"NOGROUP No such key '"
        + _safe(key)
        + b"' or consumer group '"
        + _safe(name)
        + b"'"
        + suffix
    )


def _group(value: StreamValue, name: bytes) -> StreamGroup | None:
    return next((item for item in value.groups if item.name == name), None)


def _replace(value: StreamValue, group: StreamGroup | None, name: bytes) -> StreamValue:
    groups = tuple(
        sorted(
            (
                item
                for item in (*value.groups, *((group,) if group else ()))
                if item.name != name or item is group
            ),
            key=lambda item: item.name,
        )
    )
    return replace(value, groups=groups)


def _validate_value(value: StreamValue, config: ServerConfig) -> int:
    units, raw, metadata = stream_metadata(value)
    if (
        len(value.entries) + value.field_count + units > config.max_collection_items
        or value.byte_size + raw > config.max_collection_bytes
    ):
        raise CommandError(b"ERR collection exceeds configured maximum size")
    return value.byte_size + 64 * value.field_count + metadata


def _store(store: Store, key: bytes, value: StreamValue, config: ServerConfig) -> None:
    cost = _validate_value(value, config)
    store.check_growth(key, b"stream", cost, len(value.entries))
    store.put(key, value, keep_ttl=True)


def xgroup(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    operation = args[0].upper()
    if operation == b"CREATE":
        if len(args) not in (4, 5) or len(args) == 5 and args[4].upper() != b"MKSTREAM":
            raise CommandError(b"ERR syntax error")
        key, name, raw = args[1:4]
        check_elements((name,), config)
        identity = None if raw == b"$" else parse_id(raw)
        value = store.get_stream(key)
        if value is None:
            if len(args) != 5:
                raise CommandError(MISSING_KEY)
            value = StreamValue()
        if _group(value, name) is not None:
            raise CommandError(b"BUSYGROUP Consumer Group name already exists")
        with preparing():
            updated = _replace(
                value, StreamGroup(name, value.last_id if identity is None else identity), name
            )
        _store(store, key, updated, config)
        return Simple(b"OK")
    if operation == b"DESTROY" and len(args) == 3:
        key, name = args[1:3]
        value = store.get_stream(key)
        if value is None:
            raise CommandError(MISSING_KEY)
        if _group(value, name) is None:
            return Integer(0)
        with preparing():
            updated = _replace(value, None, name)
        _store(store, key, updated, config)
        return Integer(1)
    if operation == b"SETID" and len(args) == 4:
        key, name, raw = args[1:4]
        identity = None if raw == b"$" else parse_id(raw)
        value = store.get_stream(key)
        group = _group(value, name) if value else None
        if value is None or group is None:
            raise CommandError(
                b"NOGROUP No such consumer group '"
                + _safe(name)
                + b"' for key name '"
                + _safe(key)
                + b"'"
            )
        with preparing():
            updated = _replace(
                value, replace(group, last_id=value.last_id if identity is None else identity), name
            )
        _store(store, key, updated, config)
        return Simple(b"OK")
    if operation in (b"CREATECONSUMER", b"DELCONSUMER") and len(args) == 4:
        key, name, consumer = args[1:4]
        check_elements((consumer,), config)
        value = store.get_stream(key)
        if value is None:
            raise CommandError(MISSING_KEY)
        group = _group(value, name)
        if group is None:
            raise CommandError(
                b"NOGROUP No such consumer group '"
                + _safe(name)
                + b"' for key name '"
                + _safe(key)
                + b"'"
            )
        exists = consumer in group.consumers
        if operation == b"CREATECONSUMER":
            if exists:
                return Integer(0)
            with preparing():
                replacement = replace(group, consumers=tuple(sorted((*group.consumers, consumer))))
                updated = _replace(value, replacement, name)
            _store(store, key, updated, config)
            return Integer(1)
        if not exists:
            return Integer(0)
        with preparing():
            pending = tuple(item for item in group.pending if item.consumer != consumer)
            replacement = replace(
                group, consumers=tuple(c for c in group.consumers if c != consumer), pending=pending
            )
            updated = _replace(value, replacement, name)
        _store(store, key, updated, config)
        return Integer(len(group.pending) - len(pending))
    raise CommandError(b"ERR unsupported XGROUP subcommand or arguments")


def xack(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    key, name = args[:2]
    value = store.get_stream(key)
    group = _group(value, name) if value else None
    if value is None or group is None:
        return Integer(0)
    ids = {parse_id(raw) for raw in args[2:]}
    with preparing():
        pending = tuple(item for item in group.pending if item.id not in ids)
        count = len(group.pending) - len(pending)
        updated = _replace(value, replace(group, pending=pending), name) if count else value
    if count:
        _store(store, key, updated, config)
    return Integer(count)


def xpending(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    key, name = args[:2]
    if len(args) > 2:
        return _xpending_range(store, args, config)
    value = store.get_stream(key)
    group = _group(value, name) if value else None
    if group is None:
        raise _missing(key, name)
    if not group.pending:
        return Array((Integer(0), Bulk(None), Bulk(None), Array(None)))
    counts = Counter(item.consumer for item in group.pending)
    result = Array(
        (
            Integer(len(group.pending)),
            Bulk(encode_id(group.pending[0].id)),
            Bulk(encode_id(group.pending[-1].id)),
            Array(
                tuple(
                    Array((Bulk(consumer), Bulk(str(count).encode())))
                    for consumer, count in sorted(counts.items())
                )
            ),
        )
    )
    if reply_size(result) > config.max_reply_bytes:
        raise CommandError(b"ERR reply exceeds configured maximum size")
    return result


def _xpending_range(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    key, name = args[:2]
    position, minimum_idle = 2, 0
    if len(args) > position and args[position].upper() == b"IDLE":
        if len(args) <= position + 1:
            raise CommandError(b"ERR syntax error")
        minimum_idle = integer(args[position + 1])
        if minimum_idle < 0:
            raise CommandError(b"ERR invalid idle time")
        position += 2
    if len(args) - position not in (3, 4):
        raise CommandError(b"ERR syntax error")
    low, low_exclusive = _range_bound(args[position], high=False)
    high, high_exclusive = _range_bound(args[position + 1], high=True)
    count = integer(args[position + 2])
    if count < 0:
        raise CommandError(b"ERR value is out of range, must be positive")
    if count > MAX_PENDING_RESULTS:
        raise CommandError(b"ERR pending result count exceeds configured limit")
    consumer = args[position + 3] if len(args) - position == 4 else None
    value = store.get_stream(key)
    group = _group(value, name) if value else None
    if group is None:
        raise _missing(key, name)
    if count == 0:
        return Array(())
    offset = bisect_left(group.pending, low, key=lambda item: item.id)
    selected: list[Reply] = []
    size, visited = 16, 0
    while offset < len(group.pending):
        item = group.pending[offset]
        if item.id > high or high_exclusive and item.id == high:
            break
        if visited == MAX_PENDING_VISITS:
            raise CommandError(b"ERR pending scan exceeds configured work limit")
        visited += 1
        offset += 1
        idle = min(INT64_MAX, max(0, store.now_ms - item.delivered_ms))
        if low_exclusive and item.id == low or consumer is not None and item.consumer != consumer:
            continue
        if idle < minimum_idle:
            continue
        if item.deliveries > INT64_MAX:
            raise CommandError(b"ERR delivery count exceeds RESP integer range")
        reply = Array(
            (Bulk(encode_id(item.id)), Bulk(item.consumer), Integer(idle), Integer(item.deliveries))
        )
        size += reply_size(reply)
        if size > config.max_reply_bytes:
            raise CommandError(b"ERR reply exceeds configured maximum size")
        selected.append(reply)
        if len(selected) >= count:
            break
    return Array(tuple(selected))


def xclaim(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    key, name, consumer, raw_idle = args[:4]
    check_elements((consumer,), config)
    try:
        minimum_idle = max(0, integer(raw_idle))
    except CommandError:
        raise CommandError(b"ERR Invalid min-idle-time argument for XCLAIM") from None
    raw_ids = args[4:]
    justid = bool(raw_ids and raw_ids[-1].upper() == b"JUSTID")
    if justid:
        raw_ids = raw_ids[:-1]
    if not raw_ids:
        raise CommandError(b"ERR syntax error")
    identities = []
    for raw in raw_ids:
        try:
            identities.append(parse_id(raw))
        except CommandError:
            raise CommandError(b"ERR unsupported or invalid XCLAIM argument") from None
    value = store.get_stream(key)
    group = _group(value, name) if value else None
    if value is None or group is None:
        raise _missing(key, name)
    if not INT64_MIN <= store.now_ms <= INT64_MAX:
        raise CommandError(b"ERR clock is out of range")
    with preparing():
        pending = {item.id: item for item in group.pending}
        consumers = tuple(sorted(set((*group.consumers, consumer))))
        replies: list[Reply] = []
        size = 16
        for identity in identities:
            item = pending.get(identity)
            if item is None:
                continue
            position = bisect_left(value.entries, identity, key=lambda entry: entry.id)
            if position == len(value.entries) or value.entries[position].id != identity:
                del pending[identity]
                continue
            if max(0, store.now_ms - item.delivered_ms) < minimum_idle:
                continue
            if not justid and item.deliveries == 2**64 - 1:
                raise CommandError(b"ERR delivery count exhausted")
            reply: Reply = (
                Bulk(encode_id(identity)) if justid else _entry_reply(value.entries[position])
            )
            size += reply_size(reply)
            if size > config.max_reply_bytes:
                raise CommandError(b"ERR reply exceeds configured maximum size")
            replies.append(reply)
            pending[identity] = replace(
                item,
                consumer=consumer,
                delivered_ms=store.now_ms,
                deliveries=item.deliveries + int(not justid),
            )
        replacement = replace(
            group,
            consumers=consumers,
            pending=tuple(pending[identity] for identity in sorted(pending)),
        )
        updated = _replace(value, replacement, name)
    if replacement != group:
        _store(store, key, updated, config)
    return Array(tuple(replies))


def xautoclaim(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    key, name, consumer, raw_idle, raw_start = args[:5]
    check_elements((consumer,), config)
    try:
        minimum_idle = max(0, integer(raw_idle))
    except CommandError:
        raise CommandError(b"ERR Invalid min-idle-time argument for XAUTOCLAIM") from None
    start = parse_id(raw_start)
    count, justid, pos = 100, False, 5
    while pos < len(args):
        option = args[pos].upper()
        if option == b"COUNT" and pos + 1 < len(args):
            count = integer(args[pos + 1])
            if count <= 0:
                raise CommandError(b"ERR COUNT must be > 0")
            if count > MAX_PENDING_RESULTS:
                raise CommandError(b"ERR pending result count exceeds configured limit")
            pos += 2
        elif option == b"JUSTID":
            justid = True
            pos += 1
        else:
            raise CommandError(b"ERR syntax error")
    value = store.get_stream(key)
    group = _group(value, name) if value else None
    if value is None or group is None:
        raise _missing(key, name)
    if not INT64_MIN <= store.now_ms <= INT64_MAX:
        raise CommandError(b"ERR clock is out of range")
    with preparing():
        pending = {item.id: item for item in group.pending}
        consumers = tuple(sorted(set((*group.consumers, consumer))))
        offset = bisect_left(group.pending, start, key=lambda item: item.id)
        claimed: list[Reply] = []
        deleted: list[Reply] = []
        visited = 0
        while (
            offset < len(group.pending)
            and visited < min(count * 10, MAX_PENDING_VISITS)
            and len(claimed) + len(deleted) < count
        ):
            item = group.pending[offset]
            offset += 1
            visited += 1
            position = bisect_left(value.entries, item.id, key=lambda entry: entry.id)
            if position == len(value.entries) or value.entries[position].id != item.id:
                del pending[item.id]
                deleted.append(Bulk(encode_id(item.id)))
                continue
            if max(0, store.now_ms - item.delivered_ms) < minimum_idle:
                continue
            if not justid and item.deliveries == 2**64 - 1:
                raise CommandError(b"ERR delivery count exhausted")
            claimed.append(
                Bulk(encode_id(item.id)) if justid else _entry_reply(value.entries[position])
            )
            pending[item.id] = replace(
                item,
                consumer=consumer,
                delivered_ms=store.now_ms,
                deliveries=item.deliveries + int(not justid),
            )
        next_id = group.pending[offset].id if offset < len(group.pending) else (0, 0)
        reply = Array((Bulk(encode_id(next_id)), Array(tuple(claimed)), Array(tuple(deleted))))
        if reply_size(reply) > config.max_reply_bytes:
            raise CommandError(b"ERR reply exceeds configured maximum size")
        replacement = replace(
            group,
            consumers=consumers,
            pending=tuple(pending[identity] for identity in sorted(pending)),
        )
        updated = _replace(value, replacement, name)
    if replacement != group:
        _store(store, key, updated, config)
    return reply


@dataclass(frozen=True)
class GroupReadSpec:
    group: bytes
    consumer: bytes
    count: int
    block_ms: int | None
    noack: bool
    keys: tuple[bytes, ...]
    ids: tuple[bytes, ...]


def group_read_spec(
    args: tuple[bytes, ...], config: ServerConfig, now_ms: int | None = None
) -> GroupReadSpec:
    if len(args) < 6 or args[0].upper() != b"GROUP":
        raise CommandError(b"ERR syntax error")
    group_name, consumer = args[1:3]
    check_elements((group_name, consumer), config)
    pos, count, noack, block_ms = 3, config.max_collection_items, False, None
    while pos < len(args) and args[pos].upper() != b"STREAMS":
        option = args[pos].upper()
        if option == b"COUNT" and pos + 1 < len(args):
            number = integer(args[pos + 1])
            count = config.max_collection_items if number <= 0 else number
            pos += 2
        elif option == b"NOACK":
            noack = True
            pos += 1
        elif option == b"BLOCK" and pos + 1 < len(args):
            try:
                block_ms = integer(args[pos + 1])
            except CommandError:
                raise CommandError(b"ERR timeout is not an integer or out of range") from None
            if block_ms < 0:
                raise CommandError(b"ERR timeout is negative")
            if now_ms is not None and block_ms > INT64_MAX - now_ms:
                raise CommandError(b"ERR timeout is out of range")
            pos += 2
        else:
            raise CommandError(b"ERR syntax error")
    if pos >= len(args):
        raise CommandError(b"ERR syntax error")
    rest = args[pos + 1 :]
    if len(rest) < 2 or len(rest) % 2:
        raise CommandError(
            b"ERR Unbalanced 'xreadgroup' list of streams: for each stream key "
            b"an ID or '>' must be specified."
        )
    half = len(rest) // 2
    keys, ids = rest[:half], rest[half:]
    return GroupReadSpec(group_name, consumer, count, block_ms, noack, keys, ids)


def xreadgroup(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    spec = group_read_spec(args, config, store.now_ms)
    if not INT64_MIN <= store.now_ms <= INT64_MAX:
        raise CommandError(b"ERR clock is out of range")
    values: dict[bytes, StreamValue] = {}
    for key in spec.keys:
        value = store.get_stream(key)
        if value is None or _group(value, spec.group) is None:
            raise _missing(key, spec.group, read=True)
        values[key] = value
    for raw in spec.ids:
        if raw != b">":
            parse_id(raw)
    updates: dict[bytes, StreamValue] = {}
    output: list[Reply] = []
    size = 16
    with preparing():
        for key, raw in zip(spec.keys, spec.ids, strict=True):
            value = updates.get(key, values[key])
            group = _group(value, spec.group)
            assert group is not None
            identity = None if raw == b">" else parse_id(raw)
            consumers = tuple(sorted(set((*group.consumers, spec.consumer))))
            pending = {item.id: item for item in group.pending}
            selected: list[Reply] = []
            last = group.last_id
            if identity is None:
                for entry in value.entries:
                    if entry.id <= group.last_id:
                        continue
                    item_reply = _entry_reply(entry)
                    size += reply_size(item_reply)
                    if size > config.max_reply_bytes:
                        raise CommandError(b"ERR reply exceeds configured maximum size")
                    selected.append(item_reply)
                    last = entry.id
                    if not spec.noack:
                        pending[entry.id] = PendingEntry(entry.id, spec.consumer, store.now_ms, 1)
                    if len(selected) >= spec.count:
                        break
            else:
                entries = {entry.id: entry for entry in value.entries}
                for item in group.pending:
                    if item.consumer != spec.consumer or item.id <= identity:
                        continue
                    if item.deliveries == 2**64 - 1:
                        raise CommandError(b"ERR delivery count exhausted")
                    found_entry = entries.get(item.id)
                    item_reply = (
                        _entry_reply(found_entry)
                        if found_entry is not None
                        else Array((Bulk(encode_id(item.id)), Array(None)))
                    )
                    size += reply_size(item_reply)
                    if size > config.max_reply_bytes:
                        raise CommandError(b"ERR reply exceeds configured maximum size")
                    selected.append(item_reply)
                    pending[item.id] = replace(
                        item, delivered_ms=store.now_ms, deliveries=item.deliveries + 1
                    )
                    if len(selected) >= spec.count:
                        break
            if selected or identity is not None:
                size += len(key) + 32
                if size > config.max_reply_bytes:
                    raise CommandError(b"ERR reply exceeds configured maximum size")
                output.append(Array((Bulk(key), Array(tuple(selected)))))
            replacement = replace(
                group,
                last_id=last,
                consumers=consumers,
                pending=tuple(pending[item_id] for item_id in sorted(pending)),
            )
            if replacement != group:
                updated = _replace(value, replacement, spec.group)
                _validate_value(updated, config)
                updates[key] = updated
        reply: Reply = Array(tuple(output)) if output else Array(None)
        if reply_size(reply) > config.max_reply_bytes:
            raise CommandError(b"ERR reply exceeds configured maximum size")
    if updates:
        with store.memory_batch(updates):
            for key, updated in updates.items():
                store.put(key, updated, keep_ttl=True)
    return reply


COMMANDS = (
    Command(b"XGROUP", 1, None, xgroup, writes=True),
    Command(b"XREADGROUP", 6, None, xreadgroup, writes=True),
    Command(b"XACK", 3, None, xack, writes=True),
    Command(b"XCLAIM", 5, None, xclaim, writes=True),
    Command(b"XAUTOCLAIM", 5, None, xautoclaim, writes=True),
    Command(b"XPENDING", 2, None, xpending),
)
