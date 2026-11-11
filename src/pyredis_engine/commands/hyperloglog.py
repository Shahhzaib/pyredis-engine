"""Bounded HyperLogLog sketches stored as ordinary binary strings."""
from __future__ import annotations

import math

from ..config import ServerConfig
from ..engine.store import Store, StringValue
from ..ops.memory import preparing
from ..protocol import Integer, Reply, Simple
from .registry import Command, CommandError

MAGIC = b'PYHLL\x01\x00\x00'
P = 14
REGISTERS = 1 << P
SIZE = len(MAGIC) + REGISTERS
MASK64 = (1 << 64) - 1
INVALID = b'WRONGTYPE Key is not a valid HyperLogLog string value.'


def hash64(data: bytes) -> int:
    """MurmurHash64A with the fixed seed used by Redis sketches."""
    multiplier = 0xc6a4a7935bd1e995
    value = (0xadc83b19 ^ (len(data) * multiplier)) & MASK64
    end = len(data) - len(data) % 8
    for pos in range(0, end, 8):
        block = int.from_bytes(data[pos:pos + 8], 'little')
        block = (block * multiplier) & MASK64
        block ^= block >> 47
        block = (block * multiplier) & MASK64
        value = ((value ^ block) * multiplier) & MASK64
    tail = data[end:]
    if tail:
        value ^= int.from_bytes(tail, 'little')
        value = (value * multiplier) & MASK64
    value ^= value >> 47
    value = (value * multiplier) & MASK64
    return value ^ (value >> 47)


def _load(store: Store, key: bytes) -> bytes | None:
    data = store.get_string(key)
    if data is not None and (len(data) != SIZE or not data.startswith(MAGIC)
                             or any(value > 51 for value in memoryview(data)[len(MAGIC):])):
        raise CommandError(INVALID)
    return data


def _admit(store: Store, key: bytes, config: ServerConfig) -> None:
    if SIZE > config.max_bulk_bytes:
        raise CommandError(b'ERR string exceeds configured maximum size')
    store.check_growth(key, b'string', SIZE)


def _registers(data: bytes | None) -> bytearray:
    return bytearray(REGISTERS) if data is None else bytearray(data[len(MAGIC):])


def _estimate(registers: bytearray) -> int:
    zeros = registers.count(0)
    total = sum(math.ldexp(1.0, -value) for value in registers)
    estimate = (0.7213 / (1.0 + 1.079 / REGISTERS)) * REGISTERS**2 / total
    if zeros and estimate <= 2.5 * REGISTERS:
        estimate = REGISTERS * math.log(REGISTERS / zeros)
    result = math.floor(estimate + 0.5)
    if result > 2**63 - 1:
        raise CommandError(b'ERR HyperLogLog estimate out of range')
    return result


def pfadd(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    old = _load(store, args[0])
    _admit(store, args[0], config)
    with preparing():
        registers = _registers(old)
        changed = old is None
        for item in args[1:]:
            value = hash64(item)
            index = value & (REGISTERS - 1)
            remaining = value >> P
            rank = ((remaining & -remaining).bit_length() if remaining else 51)
            if rank > registers[index]:
                registers[index] = rank
                changed = True
        if changed:
            replacement = StringValue(MAGIC + bytes(registers))
    if changed:
        store.put(args[0], replacement, keep_ttl=True)
    return Integer(int(changed))


def _union(store: Store, keys: tuple[bytes, ...], config: ServerConfig) -> bytearray:
    if len(keys) * REGISTERS > config.max_collection_bytes:
        raise CommandError(b'ERR HyperLogLog operation exceeds configured work limit')
    with preparing():
        result = bytearray(REGISTERS)
        for key in keys:
            data = _load(store, key)
            if data is not None:
                for index, value in enumerate(memoryview(data)[len(MAGIC):]):
                    if value > result[index]:
                        result[index] = value
    return result


def pfcount(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    return Integer(_estimate(_union(store, args, config)))


def pfmerge(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    registers = _union(store, args, config)
    _admit(store, args[0], config)
    with preparing():
        replacement = StringValue(MAGIC + bytes(registers))
    store.put(args[0], replacement, keep_ttl=True)
    return Simple(b'OK')


COMMANDS = (
    Command(b'PFADD', 1, None, pfadd, writes=True),
    Command(b'PFCOUNT', 1, None, pfcount),
    Command(b'PFMERGE', 1, None, pfmerge, writes=True),
)
