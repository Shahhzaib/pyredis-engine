"""Binary-safe bitmap operations on the existing immutable string value."""
from __future__ import annotations

from ..config import ServerConfig
from ..engine.store import Store, StringValue
from ..ops.memory import preparing
from ..protocol import Array, Bulk, Integer, Reply, reply_size
from .numbers import integer
from .registry import Command, CommandError

OFFSET_ERROR = b'ERR bit offset is not an integer or out of range'
BIT_ERROR = b'ERR bit is not an integer or out of range'
MAX_BIT = 2**32 - 1
INVERT = bytes.maketrans(bytes(range(256)), bytes(reversed(range(256))))


def offset(raw: bytes) -> int:
    try:
        value = integer(raw)
    except CommandError:
        raise CommandError(OFFSET_ERROR) from None
    if not 0 <= value <= MAX_BIT:
        raise CommandError(OFFSET_ERROR)
    return value


def bit(raw: bytes) -> int:
    if raw not in (b'0', b'1'):
        raise CommandError(BIT_ERROR)
    return int(raw)


def read_bit(data: bytes | bytearray, index: int) -> int:
    byte = index // 8
    return (data[byte] >> (7 - index % 8)) & 1 if byte < len(data) else 0


def getbit(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    index = offset(args[1])
    return Integer(read_bit(store.get_string(args[0]) or b'', index))


def setbit(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    index, value = offset(args[1]), bit(args[2])
    old = store.get_string(args[0]) or b''
    size = max(len(old), index // 8 + 1)
    if size > config.max_bulk_bytes:
        raise CommandError(b'ERR string exceeds configured maximum size')
    store.check_growth(args[0], b'string', size)
    previous = read_bit(old, index)
    with preparing():
        result = bytearray(old)
        result.extend(b'\0' * (size - len(result)))
        mask = 1 << (7 - index % 8)
        result[index // 8] = (result[index // 8] | mask if value else
                              result[index // 8] & ~mask)
        replacement = StringValue(bytes(result))
    store.put(args[0], replacement, keep_ttl=True)
    return Integer(previous)


def _range(args: tuple[bytes, ...], length: int, *, position: bool) -> tuple[int, int, bool]:
    # Returned indexes are inclusive bit positions. A missing end is distinguished
    # for BITPOS's implicit zero padding.
    base = 2 if position else 1
    rest = args[base:]
    if (not position and len(rest) not in (0, 2, 3)) or (position and len(rest) > 3):
        raise CommandError(b'ERR syntax error')
    unit = b'BYTE'
    if len(rest) == 3:
        unit = rest[2].upper()
        if unit not in (b'BYTE', b'BIT'):
            raise CommandError(b'ERR syntax error')
    count = length if unit == b'BYTE' else length * 8
    start = integer(rest[0]) if rest else 0
    end = integer(rest[1]) if len(rest) >= 2 else count - 1
    if start < 0:
        start += count
    if end < 0:
        end += count
        if position:
            end = max(0, end)
    if not position and start > end:
        return 1, 0, len(rest) >= 2
    start = max(start, 0)
    end = min(max(end, 0), count - 1)
    if unit == b'BYTE':
        start, end = start * 8, end * 8 + 7
    return start, end, len(rest) >= 2


def bitcount(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    _range(args, 0, position=False)  # Validate range syntax before type lookup.
    data = store.get_string(args[0]) or b''
    start, end, _ = _range(args, len(data), position=False)
    if start > end or start >= len(data) * 8:
        return Integer(0)
    first, last = start // 8, end // 8
    if first == last:
        mask = (0xff >> (start % 8)) & (0xff << (7 - end % 8))
        return Integer((data[first] & mask).bit_count())
    total = (data[first] & (0xff >> (start % 8))).bit_count()
    view = memoryview(data)
    total += sum(int.from_bytes(view[position:min(position + 65536, last)], 'big').bit_count()
                 for position in range(first + 1, last, 65536))
    total += (data[last] & (0xff << (7 - end % 8))).bit_count()
    return Integer(total)


def bitpos(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    wanted = integer(args[1])
    if wanted not in (0, 1):
        raise CommandError(b'ERR The bit argument must be 1 or 0.')
    _range(args, 0, position=True)
    data = store.get_string(args[0]) or b''
    start, end, bounded = _range(args, len(data), position=True)
    if start > end or start >= len(data) * 8:
        return Integer(0 if not data and wanted == 0 else -1)
    first, last = start // 8, end // 8
    for block_start in range(first, last + 1, 65536):
        chunk = data[block_start:min(block_start + 65536, last + 1)]
        if chunk.count(0 if wanted else 255) == len(chunk):
            continue
        for local, value in enumerate(chunk):
            index = block_start + local
            candidate = value if wanted else value ^ 0xff
            if index == first:
                candidate &= 0xff >> (start % 8)
            if index == last:
                candidate &= 0xff << (7 - end % 8)
            if candidate:
                return Integer(index * 8 + 8 - candidate.bit_length())
    return Integer(len(data) * 8 if wanted == 0 and not bounded else -1)


def bitop(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    op, dest, *keys = args
    op = op.upper()
    if op not in (b'AND', b'OR', b'XOR', b'NOT'):
        raise CommandError(b'ERR syntax error')
    if op == b'NOT' and len(keys) != 1:
        raise CommandError(b'ERR BITOP NOT must be called with a single source key.')
    values = [store.get_string(key) or b'' for key in keys]
    size = max(map(len, values), default=0)
    if size * len(values) > config.max_collection_bytes:
        raise CommandError(b'ERR bitmap operation exceeds configured work limit')
    if not size:
        store.delete(dest)
        return Integer(0)
    if size > config.max_bulk_bytes:
        raise CommandError(b'ERR string exceeds configured maximum size')
    store.check_growth(dest, b'string', size)
    with preparing():
        if op == b'NOT':
            result = values[0].translate(INVERT)
        else:
            result_int = int.from_bytes(values[0].ljust(size, b'\0'), 'big')
            for source in values[1:]:
                number = int.from_bytes(source.ljust(size, b'\0'), 'big')
                if op == b'AND':
                    result_int &= number
                elif op == b'OR':
                    result_int |= number
                else:
                    result_int ^= number
            result = result_int.to_bytes(size, 'big')
        replacement = StringValue(result)
    store.put(dest, replacement)
    return Integer(size)


Field = tuple[bytes, int, bool, int, int, bytes]


def field_plan(args: tuple[bytes, ...], *, readonly: bool = False) -> list[Field]:
    plan: list[Field] = []
    policy, pos = b'WRAP', 1
    while pos < len(args):
        op = args[pos].upper()
        if op == b'OVERFLOW' and not readonly and pos + 1 < len(args):
            policy = args[pos + 1].upper()
            if policy not in (b'WRAP', b'SAT', b'FAIL'):
                raise CommandError(b'ERR Invalid OVERFLOW type specified')
            pos += 2
            continue
        if op not in ((b'GET',) if readonly else (b'GET', b'SET', b'INCRBY')):
            if readonly:
                raise CommandError(b'ERR BITFIELD_RO only supports the GET subcommand')
            raise CommandError(b'ERR syntax error')
        need = 3 if op == b'GET' else 4
        if pos + need > len(args):
            raise CommandError(b'ERR syntax error')
        encoding = args[pos + 1].lower()
        if len(encoding) < 2 or encoding[:1] not in (b'i', b'u'):
            raise CommandError(b'ERR Invalid bitfield type. Use something like i16 u8. '
                               b'Note that u64 is not supported but i64 is.')
        try:
            width = integer(encoding[1:])
        except CommandError:
            width = 0
        signed = encoding[:1] == b'i'
        if not 1 <= width <= (64 if signed else 63):
            raise CommandError(b'ERR Invalid bitfield type. Use something like i16 u8. '
                               b'Note that u64 is not supported but i64 is.')
        raw = args[pos + 2]
        index = offset(raw[1:] if raw.startswith(b'#') else raw)
        if raw.startswith(b'#'):
            index *= width
        if index + width - 1 > MAX_BIT:
            raise CommandError(OFFSET_ERROR)
        number = integer(args[pos + 3]) if need == 4 else 0
        plan.append((op, width, signed, index, number, policy))
        pos += need
    return plan


def bitfield(store: Store, args: tuple[bytes, ...], config: ServerConfig, *,
             readonly: bool = False) -> Reply:
    plan = field_plan(args, readonly=readonly)
    old = store.get_string(args[0]) or b''
    data: bytes | bytearray = old
    replies: list[Reply] = []
    changed = False
    for op, width, signed, index, number, policy in plan:
        current = 0
        for bit_index in range(index, index + width):
            current = (current << 1) | read_bit(data, bit_index)
        if signed and current & (1 << (width - 1)):
            current -= 1 << width
        if op == b'GET':
            replies.append(Integer(current))
            continue
        candidate = number if op == b'SET' else current + number
        low = -(1 << (width - 1)) if signed else 0
        high = (1 << (width - int(signed))) - 1
        if not low <= candidate <= high:
            if policy == b'FAIL':
                replies.append(Bulk(None))
                continue
            if policy == b'SAT':
                candidate = min(high, max(low, candidate))
            else:
                candidate &= (1 << width) - 1
                if signed and candidate > high:
                    candidate -= 1 << width
        replies.append(Integer(current if op == b'SET' else candidate))
        size = max(len(data), (index + width + 7) // 8)
        if size > config.max_bulk_bytes:
            raise CommandError(b'ERR string exceeds configured maximum size')
        store.check_growth(args[0], b'string', size)
        with preparing():
            if isinstance(data, bytes):
                data = bytearray(data)
            data.extend(b'\0' * (size - len(data)))
            raw_value = candidate & ((1 << width) - 1)
            for shift in range(width):
                position = index + shift
                mask = 1 << (7 - position % 8)
                if raw_value & (1 << (width - shift - 1)):
                    data[position // 8] |= mask
                else:
                    data[position // 8] &= ~mask
        changed = True
    reply = Array(tuple(replies))
    if reply_size(reply) > config.max_reply_bytes:
        raise CommandError(b'ERR reply exceeds configured maximum size')
    if changed:
        with preparing():
            replacement = StringValue(bytes(data))
        store.put(args[0], replacement, keep_ttl=True)
    return reply


def bitfield_ro(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return bitfield(store, args, config, readonly=True)


COMMANDS = (
    Command(b'GETBIT', 2, 2, getbit),
    Command(b'SETBIT', 3, 3, setbit, writes=True),
    Command(b'BITCOUNT', 1, 4, bitcount),
    Command(b'BITPOS', 2, 5, bitpos),
    Command(b'BITOP', 3, None, bitop, writes=True),
    Command(b'BITFIELD', 1, None, bitfield, writes=True),
    Command(b'BITFIELD_RO', 1, None, bitfield_ro),
)
