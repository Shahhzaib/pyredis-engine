"""Base sorted-set commands, ordered by binary64 score then raw member bytes."""
from __future__ import annotations

from bisect import bisect_left

from ..config import ServerConfig
from ..engine.collections import SortedSetValue
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import Array, Bulk, Integer, Reply, reply_size
from .collection_helpers import check_elements, check_growth, checked_array
from .numbers import integer
from .registry import Command, CommandError
from .scores import parse_score, score_bytes


def zadd(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    check_elements(args[1:], config)
    pairs = [(member, parse_score(raw))
             for raw, member in zip(args[1::2], args[2::2], strict=True)]
    value = store.get_sorted_set(args[0])
    if value is None:
        value = SortedSetValue()
    added = {member for member, _ in pairs if member not in value.scores}
    size = value.byte_size + sum(len(member) + 8 for member in added)
    check_growth(len(value.scores) + len(added), size, config)
    store.check_growth(args[0], b'zset', size, len(value.scores) + len(added))
    changed = False
    with preparing():
        value = SortedSetValue(value.scores.copy(), value.ranked.copy(), value.byte_size)
        for member, score in pairs:
            changed = value.update(member, score) or changed
    if changed:
        store.put(args[0], value, keep_ttl=True)
    return Integer(len(added))


def zrem(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    if value is None:
        return Integer(0)
    removed = {member for member in args[1:] if member in value.scores}
    if removed:
        with preparing():
            value = SortedSetValue(value.scores.copy(), value.ranked.copy(), value.byte_size)
            for member in removed:
                value.remove(member)
        if value.scores:
            store.put(args[0], value, keep_ttl=True)
        else:
            store.delete(args[0])
    return Integer(len(removed))


def zscore(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    score = None if value is None else value.scores.get(args[1])
    return Bulk(None if score is None else score_bytes(score))


def zmscore(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    replies: list[Reply] = []
    size = len(str(len(args) - 1)) + 3
    for member in args[1:]:
        score = None if value is None else value.scores.get(member)
        item = Bulk(None if score is None else score_bytes(score))
        size += reply_size(item)
        if size > config.max_reply_bytes:
            raise CommandError(b"ERR reply exceeds configured maximum size")
        replies.append(item)
    return Array(tuple(replies))


def zrange(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    with_scores = len(args) == 4
    if with_scores and args[3].upper() != b"WITHSCORES":
        raise CommandError(b"ERR syntax error")
    start, stop = integer(args[1]), integer(args[2])
    value = store.get_sorted_set(args[0])
    if value is None:
        return Array(())
    length = len(value.ranked)
    start = max(0, start + length if start < 0 else start)
    stop = min(length - 1, stop + length if stop < 0 else stop)
    if start > stop:
        return Array(())
    selected = (value.ranked[index] for index in range(start, stop + 1))
    if with_scores:
        return checked_array((part for score, member in selected
                              for part in (member, score_bytes(score))),
                             2 * (stop - start + 1), config)
    return checked_array((member for _, member in selected), stop - start + 1, config)


def zcard(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    return Integer(0 if value is None else len(value.scores))


def zrank(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    score = None if value is None else value.scores.get(args[1])
    if value is None or score is None:
        return Bulk(None)
    return Integer(bisect_left(value.ranked, (score, args[1])))


COMMANDS = (
    Command(b"ZADD", 3, None, zadd, step=2, writes=True, shape_error=b"ERR syntax error"),
    Command(b"ZREM", 2, None, zrem, writes=True),
    Command(b"ZSCORE", 2, 2, zscore),
    Command(b"ZMSCORE", 2, None, zmscore),
    Command(b"ZRANGE", 3, 4, zrange, queue_variadic=True, shape_error=b"ERR syntax error"),
    Command(b"ZCARD", 1, 1, zcard),
    Command(b"ZRANK", 2, 2, zrank),
)
