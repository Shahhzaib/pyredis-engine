from __future__ import annotations

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple


def test_core_commands_preserve_ttl_and_wrong_type_state() -> None:
    executor = Executor(ServerConfig())
    try:

        def run(*args: bytes):
            return executor.execute(list(args)).reply

        assert run(b"SETNX", b"key", b"\x00\xff") == Integer(1)
        assert run(b"GETRANGE", b"key", b"-1", b"-1") == Bulk(b"\xff")
        assert run(b"GETRANGE", b"key", b"0", b"-99") == Bulk(b"\x00")
        assert run(b"PEXPIRE", b"key", b"60000") == Integer(1)
        assert run(b"SETNX", b"key", b"new") == Integer(0)
        assert run(b"PTTL", b"key").payload >= 0
        assert run(b"GETDEL", b"key") == Bulk(b"\x00\xff")
        assert run(b"GETDEL", b"key") == Bulk(None)
        assert run(b"HSETNX", b"hash", b"field", b"one") == Integer(1)
        assert run(b"HSETNX", b"hash", b"field", b"two") == Integer(0)
        assert run(b"HMGET", b"hash", b"field", b"missing") == Array((Bulk(b"one"), Bulk(None)))
        assert run(b"HSTRLEN", b"hash", b"field") == Integer(3)
        assert run(b"HKEYS", b"hash") == Array((Bulk(b"field"),))
        assert run(b"HVALS", b"hash") == Array((Bulk(b"one"),))
        assert isinstance(run(b"GETDEL", b"hash"), Error)
        assert run(b"HGET", b"hash", b"field") == Bulk(b"one")
        assert run(b"DECRBY", b"n", b"3") == Integer(-3)
        assert run(b"DECRBY", b"n", b"-2") == Integer(-1)
        assert run(b"DECRBY", b"n", b"-9223372036854775808") == Error(
            b"ERR decrement would overflow"
        )
        assert run(b"LPUSH", b"list", b"a", b"b") == Integer(2)
        assert run(b"LINDEX", b"list", b"-1") == Bulk(b"a")
        assert run(b"SADD", b"set", b"a") == Integer(1)
        assert run(b"SMISMEMBER", b"set", b"a", b"b") == Array((Integer(1), Integer(0)))
        assert run(b"ZADD", b"z", b"1.5", b"a") == Integer(1)
        assert run(b"ZMSCORE", b"z", b"a", b"missing") == Array((Bulk(b"1.5"), Bulk(None)))
        assert run(b"ZRANK", b"z", b"a") == Integer(0)
    finally:
        executor.close()


def test_resp2_handshake_and_connection_metadata_are_per_client() -> None:
    executor = Executor(ServerConfig())
    first, second = executor.new_client(), executor.new_client()
    try:

        def run(client, *args: bytes):
            return executor.execute(list(args), client).reply

        hello = run(first, b"HELLO", b"2")
        assert isinstance(hello, Array) and hello.payload is not None
        assert hello.payload[4:6] == (Bulk(b"proto"), Integer(2))
        assert run(first, b"HELLO", b"4") == Error(b"NOPROTO unsupported protocol version")
        assert run(first, b"CLIENT", b"SETNAME", b"first") == Simple(b"OK")
        assert run(first, b"CLIENT", b"GETNAME") == Bulk(b"first")
        assert run(second, b"CLIENT", b"GETNAME") == Bulk(None)
        assert run(first, b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-py") == Simple(b"OK")
        assert run(first, b"SELECT", b"0") == Simple(b"OK")
        assert isinstance(run(first, b"SELECT", b"1"), Error)
    finally:
        executor.close()
