"""Redis 7.4 RESP2 binary64 formatting using conservative decimal intervals.

This independent interval search uses the rounded 64-bit scaling described by
Loitsch's Grisu paper. Powers are computed, not imported from another engine.
All arithmetic is bounded by the binary64 domain; no input-sized caches exist.
See ADR 0010 for reference traces and the deliberately conservative boundaries.
"""
from __future__ import annotations

import math
from fractions import Fraction
from functools import lru_cache


def _power(base: int, exponent: int) -> Fraction:
    return Fraction(base**exponent) if exponent >= 0 else Fraction(1, base**-exponent)


def _normalized(value: Fraction) -> tuple[int, int]:
    """Round a positive rational to a 64-bit significand and binary exponent."""
    exponent = value.numerator.bit_length() - value.denominator.bit_length() - 63
    if value / _power(2, exponent) < 2**63:
        exponent -= 1
    scaled = value / _power(2, exponent)
    whole, remainder = divmod(scaled.numerator, scaled.denominator)
    return whole + (2 * remainder >= scaled.denominator), exponent


@lru_cache(maxsize=1)
def _scaling_powers() -> tuple[tuple[int, int, int], ...]:
    return tuple((decimal, *_normalized(_power(10, decimal)))
                 for decimal in range(-348, 341, 8))


def _multiply(significand: int, factor: int) -> int:
    return (significand * factor + 2**63) >> 64


def _interval(value: float) -> tuple[Fraction, Fraction, Fraction]:
    """Scale adjacent-float midpoints, reserving one rounding unit at each end."""
    exact = Fraction(value)
    lower = (exact + Fraction(math.nextafter(value, 0))) / 2
    successor = math.nextafter(value, math.inf)
    upper = (exact + (Fraction(successor) if math.isfinite(successor)
                      else Fraction(2**1024))) / 2
    high, exponent = _normalized(upper)
    low = int(lower / _power(2, exponent))
    middle = int(exact / _power(2, exponent))
    for decimal, factor, factor_exponent in _scaling_powers():
        output_exponent = exponent + factor_exponent + 64
        if -60 <= output_exponent <= -32:
            unit = _power(2, output_exponent) / _power(10, decimal)

            return ((_multiply(low, factor) + 1) * unit,
                    (_multiply(high, factor) - 1) * unit, _multiply(middle, factor) * unit)
    raise ArithmeticError("binary64 scaling power unavailable")


def _digits(value: float) -> tuple[str, int]:
    lower, upper, middle = _interval(value)
    magnitude = len(str(middle.numerator)) - len(str(middle.denominator))
    if middle < _power(10, magnitude):
        magnitude -= 1
    # Python's shortest round-trip representation provides a lower bound on
    # digit count. Our narrower interval occasionally needs another digit.
    shortest = repr(value).partition("e")[0].replace(".", "").lstrip("0").rstrip("0")
    for precision in range(len(shortest), 18):
        exponent = magnitude - precision + 1
        unit = _power(10, exponent)
        scaled = middle / unit
        whole, remainder = divmod(scaled.numerator, scaled.denominator)
        coefficient = whole + (2 * remainder > scaled.denominator)
        if lower < coefficient * unit < upper:
            while coefficient % 10 == 0:
                coefficient //= 10
                exponent += 1
            return str(coefficient), exponent
    raise ArithmeticError("binary64 decimal interval has no representation")


@lru_cache(maxsize=4096)
def format_score(value: float) -> bytes:
    if math.isnan(value):
        raise ValueError("NaN is not a sorted-set score")
    if value == 0:
        return b"0"
    if math.isinf(value):
        return b"-inf" if value < 0 else b"inf"
    if value.is_integer() and abs(value) <= 2**62:
        return str(int(value)).encode("ascii")
    sign = "-" if value < 0 else ""
    digits, exponent = _digits(abs(value))
    magnitude = exponent + len(digits) - 1
    fixed = (0 <= exponent <= 7 or exponent < 0
             and (exponent >= -6 or -3 <= magnitude <= 3))
    if fixed:
        if exponent >= 0:
            text = digits + "0" * exponent
        elif len(digits) + exponent > 0:
            text = digits[:exponent] + "." + digits[exponent:]
        else:
            text = "0." + "0" * (-exponent - len(digits)) + digits
    else:
        text = (digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
                + ("e+" if magnitude >= 0 else "e-") + str(abs(magnitude)))
    return (sign + text).encode("ascii")
