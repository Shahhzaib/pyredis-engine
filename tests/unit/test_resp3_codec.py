from __future__ import annotations

import pytest

from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.resp3 import (
    ArrayReply,
    Boolean,
    Double,
    Map,
    Null,
    Push,
    SetReply,
    encode,
    encoded_size,
)


@pytest.mark.parametrize(("reply", "wire"), [
    (Null(), b"_\r\n"),
    (Bulk(b"\x00\r\n\xff"), b"$4\r\n\x00\r\n\xff\r\n"),
    (Simple(b"OK"), b"+OK\r\n"),
    (Error(b"ERR bad"), b"-ERR bad\r\n"),
    (Integer(-42), b":-42\r\n"),
    (Boolean(True), b"#t\r\n"),
    (Boolean(False), b"#f\r\n"),
    (Double(1.5), b",1.5\r\n"),
    (Double(-0.0), b",-0\r\n"),
    (Double(float("inf")), b",inf\r\n"),
    (Double(float("-inf")), b",-inf\r\n"),
    (Double(float("nan")), b",nan\r\n"),
    (ArrayReply((Integer(1), Null())), b"*2\r\n:1\r\n_\r\n"),
    (Map(((Bulk(b"key"), Integer(1)),)), b"%1\r\n$3\r\nkey\r\n:1\r\n"),
    (SetReply((Bulk(b"a"), Bulk(b"b"))), b"~2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    (Push((Bulk(b"message"), Bulk(b"\xff"))), b">2\r\n$7\r\nmessage\r\n$1\r\n\xff\r\n"),
])
def test_explicit_resp3_wire_values(reply: object, wire: bytes) -> None:
    assert encode(reply, max_bytes=len(wire), max_depth=4) == wire
    assert encoded_size(reply, max_bytes=len(wire), max_depth=4) == len(wire)
    with pytest.raises(ValueError, match="size"):
        encode(reply, max_bytes=len(wire) - 1, max_depth=4)


def test_depth_and_local_value_validation() -> None:
    nested = ArrayReply((Map(((Bulk(b"k"), SetReply((Integer(1),))),)),))
    assert encode(nested, max_bytes=100, max_depth=3)
    with pytest.raises(ValueError, match="depth"):
        encode(nested, max_bytes=100, max_depth=2)
    for reply in (Simple(b"unsafe\r\n"), Error(b"bad\n"), Integer(2**63),
                  Bulk(None), Array(None), Double(1), True):
        with pytest.raises((TypeError, ValueError)):
            encode(reply, max_bytes=100, max_depth=4)


def test_nested_score_reply_can_exceed_resp2_wire_size() -> None:
    # Changing the shape after EXEC's existing RESP2 preflight is unsafe.
    from pyredis_engine.protocol import reply_size

    old = Array((Bulk(b"a"), Bulk(b"1.25")))
    new = ArrayReply((ArrayReply((Bulk(b"a"), Double(1.25))),))
    assert encoded_size(new, max_bytes=100, max_depth=4) > reply_size(old)
    with pytest.raises(ValueError, match="size"):
        encode(new, max_bytes=reply_size(old), max_depth=4)
