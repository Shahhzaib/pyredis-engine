"""Bounded geospatial commands backed by existing sorted sets."""
from __future__ import annotations

import math

from ..config import ServerConfig
from ..engine.collections import SortedSetValue
from ..engine.store import Store
from ..ops.memory import preparing
from ..protocol import Array, Bulk, Integer, Reply, reply_size
from .collection_helpers import check_elements, check_growth
from .numbers import integer
from .registry import Command, CommandError
from .scores import parse_score

LAT_MAX = 85.05112878
EARTH_RADIUS = 6372797.560856
UNITS = {b'M': 1.0, b'KM': 1000.0, b'FT': 0.3048, b'MI': 1609.34}
ALPHABET = b'0123456789bcdefghjkmnpqrstuvwxyz'


def coordinate(lon_raw: bytes, lat_raw: bytes) -> tuple[float, float]:
    lon, lat = parse_score(lon_raw), parse_score(lat_raw)
    if not math.isfinite(lon) or not math.isfinite(lat) or not (-180 <= lon <= 180
                                                                 and -LAT_MAX <= lat <= LAT_MAX):
        raise CommandError(f'ERR invalid longitude,latitude pair {lon:.6f},{lat:.6f}'.encode())
    return lon, lat


def encode(lon: float, lat: float, latitude_limit: float = LAT_MAX) -> int:
    scale = 1 << 26
    # Redis 7.4.12 retains the extra high bit at the inclusive upper bounds.
    # Clamping to scale - 1 changes ZSCORE, GEOPOS and GEOHASH at those bounds.
    x = int((lon + 180.0) / 360.0 * scale)
    y = int((lat + latitude_limit) / (2 * latitude_limit) * scale)
    result = 0
    for shift in range(26, -1, -1):
        result = (result << 2) | (((x >> shift) & 1) << 1) | ((y >> shift) & 1)
    return result


def decode(score: float) -> tuple[float, float]:
    bits = int(score)
    x = y = 0
    for shift in range(26, -1, -1):
        x = (x << 1) | ((bits >> (shift * 2 + 1)) & 1)
        y = (y << 1) | ((bits >> (shift * 2)) & 1)
    lon_step = 360.0 / (1 << 26)
    lat_step = 2 * LAT_MAX / (1 << 26)
    return ((min(180.0, -180.0 + lon_step * x)
             + min(180.0, -180.0 + lon_step * (x + 1))) / 2,
            (min(LAT_MAX, -LAT_MAX + lat_step * y)
             + min(LAT_MAX, -LAT_MAX + lat_step * (y + 1))) / 2)


def distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)
    h = (math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2)
         * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_RADIUS * math.asin(min(1.0, math.sqrt(h)))


def _unit(raw: bytes) -> float:
    result = UNITS.get(raw.upper())
    if result is None:
        raise CommandError(b'ERR unsupported unit provided. please use M, KM, FT, MI')
    return result


def _coords(value: SortedSetValue | None, member: bytes) -> tuple[float, float] | None:
    if value is None or member not in value.scores:
        return None
    score = value.scores[member]
    if not math.isfinite(score) or not 0 <= score < 2**54:
        return None
    return decode(score)


def _coord_reply(point: tuple[float, float]) -> Array:
    return Array(tuple(Bulk(f'{value:.17f}'.rstrip('0').rstrip('.').encode('ascii'))
                       for value in point))


def geoadd(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    pos, nx, xx, ch = 1, False, False, False
    while pos < len(args) and args[pos].upper() in (b'NX', b'XX', b'CH'):
        option = args[pos].upper()
        nx, xx, ch = nx or option == b'NX', xx or option == b'XX', ch or option == b'CH'
        pos += 1
    if nx and xx or len(args[pos:]) < 3 or len(args[pos:]) % 3:
        raise CommandError(b'ERR syntax error')
    check_elements(args[pos:], config)
    points = [(args[i + 2], float(encode(*coordinate(args[i], args[i + 1]))))
              for i in range(pos, len(args), 3)]
    value = store.get_sorted_set(args[0]) or SortedSetValue()
    new = {member for member, _ in points if member not in value.scores} if not xx else set()
    projected_size = value.byte_size + sum(len(member) + 8 for member in new)
    check_growth(len(value.scores) + len(new), projected_size, config)
    store.check_growth(args[0], b'zset', projected_size, len(value.scores) + len(new))
    with preparing():
        updated = SortedSetValue(value.scores.copy(), value.ranked.copy(), value.byte_size)
        added = changed = 0
        for member, score in points:
            present = member in updated.scores
            if nx and present or xx and not present:
                continue
            changed += int(updated.update(member, score))
            added += int(not present)
    if changed:
        store.put(args[0], updated, keep_ttl=True)
    return Integer(changed if ch else added)


def geopos(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    result = Array(tuple(_coord_reply(point) if (point := _coords(value, member)) is not None
                         else Array(None) for member in args[1:]))
    if reply_size(result) > config.max_reply_bytes:
        raise CommandError(b'ERR reply exceeds configured maximum size')
    return result


def geohash(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    replies: list[Reply] = []
    for member in args[1:]:
        point = _coords(value, member)
        if point is None:
            replies.append(Bulk(None))
            continue
        bits = encode(*point, latitude_limit=90.0) << 3
        replies.append(Bulk(bytes(ALPHABET[(bits >> shift) & 31]
                                  for shift in range(50, 0, -5)) + b'0'))
    result = Array(tuple(replies))
    if reply_size(result) > config.max_reply_bytes:
        raise CommandError(b'ERR reply exceeds configured maximum size')
    return result


def geodist(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    factor = _unit(args[3]) if len(args) == 4 else 1.0
    value = store.get_sorted_set(args[0])
    first, second = _coords(value, args[1]), _coords(value, args[2])
    if first is None or second is None:
        return Bulk(None)
    return Bulk(f'{distance(first, second) / factor:.4f}'.encode('ascii'))


def geosearch(store: Store, args: tuple[bytes, ...], config: ServerConfig) -> Reply:
    value = store.get_sorted_set(args[0])
    pos = 1
    center: tuple[float, float] | None
    if pos < len(args) and args[pos].upper() == b'FROMMEMBER' and pos + 1 < len(args):
        center = _coords(value, args[pos + 1])
        pos += 2
    elif pos < len(args) and args[pos].upper() == b'FROMLONLAT' and pos + 2 < len(args):
        center = coordinate(args[pos + 1], args[pos + 2])
        pos += 3
    else:
        raise CommandError(b'ERR syntax error')
    if pos >= len(args):
        raise CommandError(b'ERR syntax error')
    shape = args[pos].upper()
    if shape == b'BYRADIUS' and pos + 2 < len(args):
        radius, factor = parse_score(args[pos + 1]), _unit(args[pos + 2])
        width = height = 0.0
        pos += 3
    elif shape == b'BYBOX' and pos + 3 < len(args):
        width, height = parse_score(args[pos + 1]), parse_score(args[pos + 2])
        factor = _unit(args[pos + 3])
        radius = 0.0
        pos += 4
    else:
        raise CommandError(b'ERR syntax error')
    if not all(math.isfinite(v) and v >= 0 for v in (radius, width, height)):
        raise CommandError(b'ERR radius or width and height cannot be negative')
    order, count = b'', None
    withdist = withhash = withcoord = any_flag = False
    while pos < len(args):
        option = args[pos].upper()
        if option in (b'ASC', b'DESC'):
            order = option
        elif option == b'COUNT' and pos + 1 < len(args):
            count = integer(args[pos + 1])
            if count <= 0:
                raise CommandError(b'ERR COUNT must be > 0')
            pos += 1
        elif option == b'ANY':
            any_flag = True
        elif option == b'WITHDIST':
            withdist = True
        elif option == b'WITHHASH':
            withhash = True
        elif option == b'WITHCOORD':
            withcoord = True
        else:
            raise CommandError(b'ERR syntax error')
        pos += 1
    if any_flag and count is None:
        raise CommandError(b'ERR syntax error')
    if value is None or center is None:
        return Array(())
    if (len(value.scores) > config.max_collection_items
            or value.byte_size > config.max_collection_bytes):
        raise CommandError(b'ERR geospatial search exceeds configured work limit')
    matches: list[tuple[float, bytes, int, tuple[float, float]]] = []
    for score, member in value.ranked:
        point = _coords(value, member)
        if point is None:
            continue
        dist = distance(center, point)
        inside = (dist <= radius * factor if shape == b'BYRADIUS' else
                  distance((center[0], point[1]), point) <= width * factor / 2
                  and distance((center[0], center[1]), (center[0], point[1]))
                  <= height * factor / 2)
        if inside:
            matches.append((dist, member, int(score), point))
            if any_flag and count is not None and len(matches) >= count:
                break
    if order:
        matches.sort(key=lambda item: (item[0], item[1]), reverse=order == b'DESC')
    if count is not None:
        matches = matches[:count]
    replies: list[Reply] = []
    size = len(str(len(matches))) + 3
    for dist, member, score, point in matches:
        parts: list[Reply] = [Bulk(member)]
        if withdist:
            parts.append(Bulk(f'{dist / factor:.4f}'.encode('ascii')))
        if withhash:
            parts.append(Integer(score))
        if withcoord:
            parts.append(_coord_reply(point))
        item = parts[0] if len(parts) == 1 else Array(tuple(parts))
        size += reply_size(item)
        if size > config.max_reply_bytes:
            raise CommandError(b'ERR reply exceeds configured maximum size')
        replies.append(item)
    reply = Array(tuple(replies))
    if reply_size(reply) > config.max_reply_bytes:
        raise CommandError(b'ERR reply exceeds configured maximum size')
    return reply


COMMANDS = (
    Command(b'GEOADD', 4, None, geoadd, writes=True),
    Command(b'GEODIST', 3, 4, geodist),
    Command(b'GEOHASH', 2, None, geohash),
    Command(b'GEOPOS', 2, None, geopos),
    Command(b'GEOSEARCH', 4, None, geosearch),
)
