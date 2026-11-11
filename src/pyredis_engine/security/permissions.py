"""Explicit key specifications and non-backtracking binary Redis glob matching."""

from __future__ import annotations

from dataclasses import dataclass, field

# A new registry entry must explicitly join this table before startup succeeds.
NO_KEYS = frozenset(
    b"PING ECHO QUIT HELLO CLIENT SELECT SCAN MULTI EXEC DISCARD UNWATCH DBSIZE FLUSHDB "
    b"SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH AUTH ACL".split()
)
ALL_KEYS = frozenset(b"DEL EXISTS MGET WATCH PFCOUNT PFMERGE".split())
FIRST_KEY = frozenset(
    b"TYPE SET GET GETDEL GETRANGE SETNX APPEND STRLEN INCR DECR INCRBY DECRBY "
    b"GETBIT SETBIT BITCOUNT BITPOS BITFIELD BITFIELD_RO PFADD "
    b"GEOADD GEODIST GEOHASH GEOPOS GEOSEARCH "
    b"EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST GETEX HSET HSETNX HGET HMGET "
    b"HSTRLEN HDEL HEXISTS HGETALL HLEN HKEYS HVALS HSCAN SSCAN ZSCAN "
    b"LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX SADD SREM SISMEMBER SMISMEMBER SMEMBERS SCARD "
    b"ZADD ZREM ZSCORE ZMSCORE ZRANGE ZCARD ZRANK XADD XLEN XRANGE XREVRANGE XDEL XTRIM "
    b"XACK XPENDING XCLAIM XAUTOCLAIM".split()
)
KEY_COMMANDS = NO_KEYS | ALL_KEYS | FIRST_KEY | {b"MSET", b"BITOP"}
BLOCKING_KEYS = frozenset((b"BLPOP", b"BRPOP"))
KEY_COMMANDS = KEY_COMMANDS | BLOCKING_KEYS | {b"XREAD", b"XGROUP", b"XREADGROUP"}
ACL_SUBCOMMANDS = frozenset(b"SETUSER DELUSER USERS WHOAMI DRYRUN".split())


def _class_byte(source: bytes, index: int) -> tuple[int, int]:
    value = source[index]
    index += 1
    if value == 92:
        if index == len(source):
            raise ValueError("invalid ACL pattern")
        value = source[index]
        index += 1
    return value, index


def command_keys(args: list[bytes]) -> list[bytes]:
    name = args[0].upper()
    if name == b"BITOP":
        return args[2:]
    if name in ALL_KEYS:
        return args[1:]
    if name == b"MSET":
        return args[1::2]
    if name in BLOCKING_KEYS:
        return args[1:-1]
    if name == b"XGROUP":
        return args[2:3]
    if name == b"XREADGROUP":
        if len(args) < 4 or args[1].upper() != b"GROUP":
            return []
        position = 4
        while position < len(args) and args[position].upper() != b"STREAMS":
            option = args[position].upper()
            if option in (b"COUNT", b"BLOCK"):
                position += 2
            elif option == b"NOACK":
                position += 1
            else:
                return []
        rest = args[position + 1 :] if position < len(args) else []
        return rest[: len(rest) // 2] if len(rest) >= 2 and len(rest) % 2 == 0 else []
    if name == b"XREAD":
        for index, item in enumerate(args[1:], 1):
            if item.upper() == b"STREAMS":
                rest = args[index + 1 :]
                return rest[: len(rest) // 2] if len(rest) >= 2 and len(rest) % 2 == 0 else []
        return []
    if name in FIRST_KEY:
        return args[1:2]
    if name not in NO_KEYS and name != b"INFO":
        raise RuntimeError("missing command key specification")
    return []


@dataclass
class MatchBudget:
    remaining: int = 1_000_000


@dataclass(frozen=True, slots=True)
class Pattern:
    source: bytes
    literals: dict[int, int] = field(repr=False)
    stars: int = field(repr=False)
    count: int = field(repr=False)
    prefix: bytes | None = field(repr=False)

    @classmethod
    def compile(cls, source: bytes) -> Pattern:
        if not 1 <= len(source) <= 128:
            raise ValueError("invalid ACL pattern")
        tokens: list[int | None] = []
        index = 0
        while index < len(source):
            value = source[index]
            index += 1
            if value == 42:  # collapse consecutive stars for one-step epsilon closure
                if not tokens or tokens[-1] is not None:
                    tokens.append(None)
                continue
            if value == 63:
                mask = (1 << 256) - 1
            elif value == 92:
                if index == len(source):
                    raise ValueError("invalid ACL pattern")
                mask = 1 << source[index]
                index += 1
            elif value == 91:
                negate = index < len(source) and source[index] == 94
                index += int(negate)
                mask = 0
                while index < len(source) and source[index] != 93:
                    start, index = _class_byte(source, index)
                    if index + 1 < len(source) and source[index] == 45 and source[index + 1] != 93:
                        end, index = _class_byte(source, index + 1)
                        low, high = sorted((start, end))
                        mask |= ((1 << (high - low + 1)) - 1) << low
                    else:
                        mask |= 1 << start
                if index == len(source) or not mask:
                    raise ValueError("invalid ACL pattern")
                index += 1
                if negate:
                    mask ^= (1 << 256) - 1
            else:
                mask = 1 << value
            tokens.append(mask)
        literals: dict[int, int] = {}
        stars = 0
        for position, token in enumerate(tokens):
            if token is None:
                stars |= 1 << position
            else:
                for byte in range(256):
                    if token & (1 << byte):
                        literals[byte] = literals.get(byte, 0) | (1 << position)
        prefix = (
            source[:-1]
            if source.endswith(b"*") and not any(c in source[:-1] for c in b"*?[\\")
            else None
        )
        return cls(source, literals, stars, len(tokens), prefix)

    def matches(self, value: bytes, budget: MatchBudget) -> bool:
        cost = len(self.prefix) if self.prefix is not None else len(value)
        if cost > budget.remaining:
            return False
        budget.remaining -= cost
        if self.prefix is not None:
            return value.startswith(self.prefix)
        states = 1 | ((1 & self.stars) << 1)
        for byte in value:
            states = ((states & self.literals.get(byte, 0)) << 1) | (states & self.stars)
            states |= (states & self.stars) << 1
            if not states:
                return False
        return bool(states & (1 << self.count))
