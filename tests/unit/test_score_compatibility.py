"""Golden Redis 7.4.12 strings from independent raw socket probes (ADR 0010)."""
from __future__ import annotations

import math
import random
import struct

import pytest

from pyredis_engine.commands.registry import CommandError
from pyredis_engine.commands.score_format import format_score
from pyredis_engine.commands.scores import parse_score, score_bytes


def test_score_format_cache_is_bounded_and_preserves_special_values() -> None:
    format_score.cache_clear()
    try:
        assert format_score(.1) == b'0.1'
        assert format_score(.1) == b'0.1'
        assert format_score.cache_info().hits == 1
        for index in range(5000):
            score_bytes(index + .25)
        assert format_score.cache_info().currsize == format_score.cache_info().maxsize == 4096
        assert score_bytes(-0.0) == score_bytes(0.0) == b'0'
        assert score_bytes(math.inf) == b'inf' and score_bytes(-math.inf) == b'-inf'
        with pytest.raises(ValueError, match='NaN'):
            score_bytes(math.nan)
        assert score_bytes(.1) == b'0.1'  # still exact after eviction
    finally:
        format_score.cache_clear()


@pytest.mark.parametrize("raw,expected", [
    (b"0.1", b"0.1"), (b"5e-324", b"5e-324"), (b"-5e-324", b"-5e-324"),
    (b"1e-7", b"1e-7"), (b"1e-6", b"0.000001"), (b"1.2e-6", b"1.2e-6"),
    (b"1.23e-5", b"1.23e-5"), (b"1.234567e-3", b"0.001234567"),
    (b"1e18", b"1000000000000000000"), (b"1e19", b"1e+19"),
    (b"1e23", b"99999999999999990000000"),
    (b"1.2345678901234567e5", b"1.2345678901234567e+5"),
    (b"1.2345678901234567e15", b"1234567890123456.7"),
    (b"4611686018427387904", b"4611686018427387904"),
    (b"4611686018427388928", b"4611686018427389000"),
    (b"-9223372036854775808", b"-9223372036854776000"),
    (b"-0x1.3fdb53222e08cp-183", b"-1.0191223617218131e-55"),
    (b"-0", b"0"), (b"-0.0", b"0"), (b"+inf", b"inf"), (b"-inf", b"-inf"),
    (b"0x1p2", b"4"), (b"0x1", b"1"), (b"0X1.P+2", b"4"),
    (b"0x.8", b"0.5"), (b"0x1.", b"1"), (b"+0x1p+2", b"4"),
    (b"0x1p-1074", b"5e-324"), (b"-0x0p0", b"0"),
    (b"0x0p999999", b"0"), (b"0x0p-999999", b"0"),
])
def test_exact_reference_score_strings(raw: bytes, expected: bytes) -> None:
    assert score_bytes(parse_score(raw)) == expected


@pytest.mark.parametrize("raw", [
    b"0x1p-1075", b"0x1p1024", b"0x1p999999", b"0xp1", b"0x", b"1p2",
    b"0x1_0", b"0x1p+2 ", b"0x1p2\x00", b"0x.p1", b"0x1p+", b"NaN()",
])
def test_hex_parser_does_not_expose_broader_python_grammar(raw: bytes) -> None:
    with pytest.raises(CommandError, match="valid float"):
        parse_score(raw)


def test_score_rendering_roundtrips_generated_binary64_values() -> None:
    randomizer = random.Random(7412)
    for _ in range(10_000):
        value = struct.unpack(">d", randomizer.getrandbits(64).to_bytes(8, "big"))[0]
        if math.isfinite(value):
            encoded = score_bytes(value)
            assert float(encoded) == value
            assert b"e-0" not in encoded and b"e+0" not in encoded
