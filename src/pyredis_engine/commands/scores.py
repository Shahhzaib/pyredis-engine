"""ASCII binary64 score parsing and RESP2 decimal rendering, independent of locale."""
from __future__ import annotations

import math
import re

from .registry import CommandError
from .score_format import format_score

_DECIMAL = re.compile(rb"[+-]?([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_HEX = re.compile(rb"[+-]?0[xX]([0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)"
                  rb"(?:[pP][+-]?[0-9]+)?")
_INVALID = b"ERR value is not a valid float"


def parse_score(raw: bytes) -> float:
    if raw.lower() in (b"inf", b"+inf", b"infinity", b"+infinity", b"-inf", b"-infinity"):
        return -math.inf if raw.startswith(b"-") else math.inf
    match = _DECIMAL.fullmatch(raw)
    hexadecimal = False
    if match is None:
        match = _HEX.fullmatch(raw)
        hexadecimal = True
    if match is None:
        raise CommandError(_INVALID)
    nonzero = any(digit not in b"0." for digit in match[1])
    if not nonzero:
        return 0.0
    try:
        score = float.fromhex(raw.decode("ascii")) if hexadecimal else float(raw)
    except (ValueError, OverflowError):
        raise CommandError(_INVALID) from None
    if not math.isfinite(score):
        raise CommandError(_INVALID)
    if score == 0:
        # Do not silently turn a nonzero, unrepresentable input into zero.
        raise CommandError(_INVALID)
    return score


def score_bytes(score: float) -> bytes:
    return format_score(score)
