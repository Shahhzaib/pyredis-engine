"""Canonical Redis signed 64-bit numeric arguments, separate from RESP integers."""
from ..protocol import INT64_MAX, INT64_MIN
from .registry import CommandError


def integer(raw: bytes) -> int:
    invalid = b"ERR value is not an integer or out of range"
    if raw == b"0":
        return 0
    if not raw or len(raw) > 20:
        raise CommandError(invalid)
    digits = raw[1:] if raw.startswith(b"-") else raw
    if (not digits or not 49 <= digits[0] <= 57
            or any(ch < 48 or ch > 57 for ch in digits) or len(digits) > 19):
        raise CommandError(invalid)
    value = int(raw)
    if not INT64_MIN <= value <= INT64_MAX:
        raise CommandError(invalid)
    return value
