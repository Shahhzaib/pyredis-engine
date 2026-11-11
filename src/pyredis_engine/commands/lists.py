"""M04 deque-backed lists with bounded count/range replies and atomic pops."""
from __future__ import annotations

import math
import re
from collections import deque
from itertools import islice

from ..config import ServerConfig
from ..engine.collections import ListValue
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import INT64_MAX, Array, Bulk, Integer, Reply, reply_size
from .collection_helpers import check_elements, check_growth, checked_array, checked_bulk
from .numbers import integer
from .registry import Command, CommandError, Handler

_TIMEOUT = re.compile(rb"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_HEX_TIMEOUT = re.compile(rb"[+-]?0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)"
                          rb"(?:[pP][+-]?[0-9]+)?\Z")


def blocking_timeout(raw: bytes, now_ms: int) -> float:
    if len(raw) > 128:
        raise CommandError(b"ERR timeout is not a float or out of range")
    if raw.lower() in (b"inf", b"+inf", b"infinity", b"+infinity"):
        raise CommandError(b"ERR timeout is out of range")
    if raw.lower() in (b"-inf", b"-infinity"):
        raise CommandError(b"ERR timeout is negative")
    hexadecimal = bool(_HEX_TIMEOUT.fullmatch(raw))
    if not hexadecimal and not _TIMEOUT.fullmatch(raw):
        raise CommandError(b"ERR timeout is not a float or out of range")
    try:
        value = float.fromhex(raw.decode('ascii')) if hexadecimal else float(raw)
    except OverflowError:
        raise CommandError(b"ERR timeout is negative" if raw.startswith(b'-') else
                           b"ERR timeout is out of range") from None
    if value < 0:
        raise CommandError(b"ERR timeout is negative")
    if not math.isfinite(value) or value * 1000 > INT64_MAX - now_ms:
        raise CommandError(b"ERR timeout is out of range")
    return value


def blocking_pop(*, left: bool) -> Handler:
    def pop(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
        blocking_timeout(args[-1], store.now_ms)
        for key in args[:-1]:
            value = store.get_list(key)
            if value is None or not value.items:
                continue
            item = value.items[0] if left else value.items[-1]
            result = Array((Bulk(key), Bulk(item)))
            if reply_size(result) > config.max_reply_bytes:
                raise CommandError(b"ERR reply exceeds configured maximum size")
            pop_handler(left=left)(store, (key,), config)
            return result
        return Array(None)
    return pop


def push_handler(*, left: bool) -> Handler:
    def push(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
        value = store.get_list(args[0])
        if value is None:
            value = ListValue()
        elements = args[1:]
        check_elements(elements, config)
        count = len(value.items) + len(elements)
        size = value.byte_size + sum(len(element) for element in elements)
        check_growth(count, size, config)
        store.check_growth(args[0], b'list', size, count)
        with preparing():
            value = ListValue(deque(value.items), size)
            if left:
                value.items.extendleft(elements)
            else:
                value.items.extend(elements)
        store.put(args[0], value, keep_ttl=True)
        return Integer(count)
    return push


def pop_handler(*, left: bool) -> Handler:
    def pop(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
        counted = len(args) == 2
        try:
            count = integer(args[1]) if counted else 1
        except CommandError:
            raise CommandError(b"ERR value is out of range, must be positive") from None
        if count < 0:
            raise CommandError(b"ERR value is out of range, must be positive")
        value = store.get_list(args[0])
        if value is None:
            return Array(None) if counted else Bulk(None)
        count = min(count, len(value.items))
        if counted:
            selected = iter(value.items) if left else reversed(value.items)
            reply: Reply = checked_array(islice(selected, count), count, config)
        else:
            reply = checked_bulk(value.items[0] if left else value.items[-1], config)
        if count == 0:
            return reply
        with preparing():
            value = ListValue(deque(value.items), value.byte_size)
            for _ in range(count):
                element = value.items.popleft() if left else value.items.pop()
                value.byte_size -= len(element)
        if value.items:
            store.put(args[0], value, keep_ttl=True)
        else:
            store.delete(args[0])
        return reply
    return pop


def lrange(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    start, stop = integer(args[1]), integer(args[2])
    value = store.get_list(args[0])
    if value is None:
        return Array(())
    length = len(value.items)
    start = max(0, start + length if start < 0 else start)
    stop = min(length - 1, stop + length if stop < 0 else stop)
    if start > stop:
        return Array(())
    return checked_array(islice(value.items, start, stop + 1), stop - start + 1, config)


def llen(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_list(args[0])
    return Integer(0 if value is None else len(value.items))


def lindex(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    index = integer(args[1])
    value = store.get_list(args[0])
    if value is None or not -len(value.items) <= index < len(value.items):
        return Bulk(None)
    return checked_bulk(value.items[index], config)


COMMANDS = (
    Command(b"LPUSH", 2, None, push_handler(left=True), writes=True),
    Command(b"RPUSH", 2, None, push_handler(left=False), writes=True),
    Command(b"LPOP", 1, 2, pop_handler(left=True), writes=True, queue_variadic=True),
    Command(b"RPOP", 1, 2, pop_handler(left=False), writes=True, queue_variadic=True),
    Command(b"BLPOP", 2, None, blocking_pop(left=True), writes=True),
    Command(b"BRPOP", 2, None, blocking_pop(left=False), writes=True),
    Command(b"LRANGE", 3, 3, lrange),
    Command(b"LLEN", 1, 1, llen),
    Command(b"LINDEX", 2, 2, lindex),
)
