"""Redis stream IDs and bounded immutable stream postimages."""

from __future__ import annotations

from dataclasses import dataclass

from ..config import ServerConfig
from ..engine.collections import StreamEntry, StreamValue, stream_metadata
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import INT64_MAX, Array, Bulk, Integer, Reply, reply_size
from .collection_helpers import check_elements
from .numbers import integer
from .registry import Command, CommandError, Handler

MAX_ID = 2**64 - 1
INVALID_ID = b"ERR Invalid stream ID specified as stream command argument"


def parse_id(raw: bytes, *, missing_seq: int = 0) -> tuple[int, int]:
    parts = raw.split(b"-")
    if len(parts) not in (1, 2) or any(
        not part or not part.isdigit() or len(part) > 20 for part in parts
    ):
        raise CommandError(INVALID_ID)
    ms, seq = int(parts[0]), int(parts[1]) if len(parts) == 2 else missing_seq
    if ms > MAX_ID or seq > MAX_ID:
        raise CommandError(INVALID_ID)
    return ms, seq


def encode_id(identity: tuple[int, int]) -> bytes:
    return str(identity[0]).encode() + b"-" + str(identity[1]).encode()


def _new_id(raw: bytes, last: tuple[int, int], now_ms: int) -> tuple[int, int]:
    if raw == b"*":
        ms = max(0, now_ms, last[0])
        identity = (ms, last[1] + 1) if ms == last[0] else (ms, 0)
    elif raw.endswith(b"-*"):
        ms, _ = parse_id(raw[:-2])
        identity = (ms, last[1] + 1) if ms == last[0] else (ms, 0)
    else:
        identity = parse_id(raw)
    if identity[0] > MAX_ID or identity[1] > MAX_ID:
        raise CommandError(
            b"ERR The stream has exhausted the last possible ID, unable to add more items"
        )
    if identity == (0, 0):
        raise CommandError(b"ERR The ID specified in XADD must be greater than 0-0")
    if identity <= last:
        raise CommandError(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
    return identity


def _limit(raw: bytes) -> int:
    value = integer(raw)
    if value < 0:
        raise CommandError(b"ERR value is out of range, must be positive")
    return value


def _maxlen(args: tuple[bytes, ...], pos: int) -> tuple[int, int]:
    if pos < len(args) and args[pos] == b"=":
        pos += 1
    if pos >= len(args):
        raise CommandError(b"ERR syntax error")
    return _limit(args[pos]), pos + 1


def xadd(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    pos, maximum = 1, None
    if len(args) > pos and args[pos].upper() == b"MAXLEN":
        maximum, pos = _maxlen(args, pos + 1)
    if len(args) < pos + 3 or (len(args) - pos - 1) % 2:
        raise CommandError(b"ERR wrong number of arguments for 'xadd' command")
    raw_id = args[pos]
    if raw_id.endswith(b"-*"):
        if b"-" in raw_id[:-2]:
            raise CommandError(INVALID_ID)
        parse_id(raw_id[:-2])
    elif raw_id != b"*":
        parse_id(raw_id)
    value = store.get_stream(args[0]) or StreamValue()
    identity = _new_id(raw_id, value.last_id, store.now_ms)
    fields = args[pos + 1 :]
    check_elements(fields, config)
    with preparing():
        pairs = tuple(zip(fields[::2], fields[1::2], strict=True))
        entries = (*value.entries, StreamEntry(identity, pairs))
        if maximum is not None:
            entries = entries[-maximum:] if maximum else ()
        byte_size = sum(
            len(field) + len(payload) for entry in entries for field, payload in entry.fields
        )
        field_count = sum(len(entry.fields) for entry in entries)
        units, raw_metadata, metadata_charge = stream_metadata(value)
        if (
            len(entries) + field_count + units > config.max_collection_items
            or byte_size + raw_metadata > config.max_collection_bytes
        ):
            raise CommandError(b"ERR collection exceeds configured maximum size")
        store.check_growth(args[0], b"stream", byte_size + 64 * field_count + metadata_charge,
                           len(entries))
        updated = StreamValue(entries, identity, byte_size, field_count, value.groups)
    store.put(args[0], updated, keep_ttl=True)
    return Bulk(encode_id(identity))


def xlen(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_stream(args[0])
    return Integer(len(value.entries) if value else 0)


def _range_bound(raw: bytes, *, high: bool) -> tuple[tuple[int, int], bool]:
    exclusive = raw.startswith(b"(")
    token = raw[1:] if exclusive else raw
    identity = (
        (0, 0)
        if token == b"-"
        else (MAX_ID, MAX_ID)
        if token == b"+"
        else parse_id(token, missing_seq=MAX_ID if high else 0)
    )
    return identity, exclusive


def _entry_reply(entry: StreamEntry) -> Array:
    return Array(
        (
            Bulk(encode_id(entry.id)),
            Array(tuple(Bulk(part) for field in entry.fields for part in field)),
        )
    )


def xrange(*, reverse: bool) -> Handler:
    def run(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
        if len(args) not in (3, 5) or len(args) == 5 and args[3].upper() != b"COUNT":
            raise CommandError(b"ERR syntax error")
        count = integer(args[4]) if len(args) == 5 else config.max_collection_items
        high, high_exclusive = _range_bound(args[1 if reverse else 2], high=True)
        low, low_exclusive = _range_bound(args[2 if reverse else 1], high=False)
        value = store.get_stream(args[0])
        if value is None:
            return Array(())
        if count <= 0:
            return Array(None)
        selected: list[Reply] = []
        size = 16
        for entry in reversed(value.entries) if reverse else value.entries:
            if (
                entry.id < low
                or entry.id > high
                or low_exclusive
                and entry.id == low
                or high_exclusive
                and entry.id == high
            ):
                continue
            result = _entry_reply(entry)
            size += reply_size(result)
            if size > config.max_reply_bytes:
                raise CommandError(b"ERR reply exceeds configured maximum size")
            selected.append(result)
            if len(selected) == count:
                break
        return Array(tuple(selected))

    return run


def xdel(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_stream(args[0])
    if value is None:
        return Integer(0)
    identities = {parse_id(item) for item in args[1:]}
    with preparing():
        entries = tuple(entry for entry in value.entries if entry.id not in identities)
        removed = len(value.entries) - len(entries)
        if removed:
            size = sum(
                len(field) + len(payload) for entry in entries for field, payload in entry.fields
            )
            fields = sum(len(entry.fields) for entry in entries)
            updated = StreamValue(entries, value.last_id, size, fields, value.groups)
    if removed:
        store.put(args[0], updated, keep_ttl=True)
    return Integer(removed)


def xtrim(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    if len(args) < 3 or args[1].upper() != b"MAXLEN":
        raise CommandError(b"ERR syntax error")
    maximum, pos = _maxlen(args, 2)
    if pos != len(args):
        raise CommandError(b"ERR syntax error")
    value = store.get_stream(args[0])
    if value is None or len(value.entries) <= maximum:
        return Integer(0)
    with preparing():
        entries = value.entries[-maximum:] if maximum else ()
        size = sum(
            len(field) + len(payload) for entry in entries for field, payload in entry.fields
        )
        fields = sum(len(entry.fields) for entry in entries)
        updated = StreamValue(entries, value.last_id, size, fields, value.groups)
    store.put(args[0], updated, keep_ttl=True)
    return Integer(len(value.entries) - len(entries))


@dataclass(frozen=True)
class ReadSpec:
    count: int
    block_ms: int | None
    keys: tuple[bytes, ...]
    ids: tuple[bytes, ...]
    ids_offset: int


def read_spec(args: tuple[bytes, ...], config: ServerConfig, now_ms: int | None = None) -> ReadSpec:
    count, block_ms, pos = config.max_collection_items, None, 0
    while pos < len(args) and args[pos].upper() != b"STREAMS":
        option = args[pos].upper()
        if option not in (b"COUNT", b"BLOCK") or pos + 1 >= len(args):
            raise CommandError(b"ERR syntax error")
        try:
            number = integer(args[pos + 1])
        except CommandError:
            raise CommandError(
                b"ERR timeout is not an integer or out of range"
                if option == b"BLOCK"
                else b"ERR value is not an integer or out of range"
            ) from None
        if option == b"BLOCK":
            if number < 0:
                raise CommandError(b"ERR timeout is negative")
            if now_ms is not None and number > INT64_MAX - now_ms:
                raise CommandError(b"ERR timeout is out of range")
            block_ms = number
        else:
            count = config.max_collection_items if number <= 0 else number
        pos += 2
    if pos >= len(args):
        raise CommandError(b"ERR syntax error")
    rest = args[pos + 1 :]
    if len(rest) < 2 or len(rest) % 2:
        raise CommandError(
            b"ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' "
            b"must be specified."
        )
    half = len(rest) // 2
    keys, ids = rest[:half], rest[half:]
    for raw in ids:
        if raw != b"$":
            parse_id(raw)
    return ReadSpec(count, block_ms, keys, ids, pos + 1 + half)


def xread(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    spec = read_spec(args, config, store.now_ms)
    output: list[Reply] = []
    size = 16
    for key, raw in zip(spec.keys, spec.ids, strict=True):
        value = store.get_stream(key)
        if value is None:
            continue
        identity = value.last_id if raw == b"$" else parse_id(raw)
        selected: list[Reply] = []
        for entry in value.entries:
            if entry.id <= identity:
                continue
            result = _entry_reply(entry)
            size += reply_size(result)
            if size + len(key) + 32 > config.max_reply_bytes:
                raise CommandError(b"ERR reply exceeds configured maximum size")
            selected.append(result)
            if len(selected) >= spec.count:
                break
        if selected:
            part = Array((Bulk(key), Array(tuple(selected))))
            size += len(key) + 32
            if size > config.max_reply_bytes:
                raise CommandError(b"ERR reply exceeds configured maximum size")
            output.append(part)
    return Array(tuple(output)) if output else Array(None)


COMMANDS = (
    Command(b"XADD", 4, None, xadd, writes=True),
    Command(b"XLEN", 1, 1, xlen),
    Command(b"XRANGE", 3, None, xrange(reverse=False)),
    Command(b"XREVRANGE", 3, None, xrange(reverse=True)),
    Command(b"XDEL", 2, None, xdel, writes=True),
    Command(b"XTRIM", 3, None, xtrim, writes=True),
    Command(b"XREAD", 3, None, xread),
)
