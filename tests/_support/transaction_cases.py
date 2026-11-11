"""M06 exact traces established against independent Redis 7.4.12 probes."""
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Reply, Simple

OK = Simple(b"OK")
QUEUED = Simple(b"QUEUED")
ABORT = Error(b"EXECABORT Transaction discarded because of previous errors.")
Step = tuple[tuple[bytes, ...], Reply]
TRACES: dict[str, tuple[Step, ...]] = {
    "empty": (((b"MULTI",), OK), ((b"EXEC",), Array(()))),
    "ordered_binary": (
        ((b"MULTI",), OK), ((b"SET", b"\x00\xff", b"\r\n\x00"), QUEUED),
        ((b"GET", b"\x00\xff"), QUEUED),
        ((b"EXEC",), Array((OK, Bulk(b"\r\n\x00")))),
        ((b"GET", b"\x00\xff"), Bulk(b"\r\n\x00"))),
    "nested_multi": (
        ((b"MULTI",), OK), ((b"MULTI",), Error(b"ERR MULTI calls can not be nested")),
        ((b"PING",), QUEUED), ((b"EXEC",), Array((Simple(b"PONG"),)))),
    "watch_in_multi": (
        ((b"MULTI",), OK), ((b"WATCH", b"k"), Error(b"ERR WATCH inside MULTI is not allowed")),
        ((b"PING",), QUEUED), ((b"EXEC",), Array((Simple(b"PONG"),)))),
    "queue_arity": (
        ((b"MULTI",), OK), ((b"SET", b"k", b"v"), QUEUED),
        ((b"GET",), Error(b"ERR wrong number of arguments for 'get' command")),
        ((b"EXEC",), ABORT), ((b"GET", b"k"), Bulk(None))),
    "exec_arity_aborts": (
        ((b"MULTI",), OK), ((b"SET", b"k", b"v"), QUEUED),
        ((b"EXEC", b"extra"), Error(
            b"EXECABORT Transaction discarded because of: "
            b"wrong number of arguments for 'exec' command")),
        ((b"EXEC",), Error(b"ERR EXEC without MULTI")), ((b"GET", b"k"), Bulk(None))),
    "runtime_errors_no_rollback": (
        ((b"MULTI",), OK), ((b"SET", b"k", b"v"), QUEUED), ((b"INCR", b"k"), QUEUED),
        ((b"SET", b"other", b"ok"), QUEUED), ((b"GET", b"other"), QUEUED),
        ((b"EXEC",), Array((OK, Error(b"ERR value is not an integer or out of range"),
                             OK, Bulk(b"ok")))), ((b"GET", b"k"), Bulk(b"v"))),
    "runtime_syntax": (
        ((b"MULTI",), OK), ((b"SET", b"k", b"v", b"BOGUS"), QUEUED),
        ((b"PING",), QUEUED),
        ((b"EXEC",), Array((Error(b"ERR syntax error"), Simple(b"PONG"))))),
    "runtime_pairs": (
        ((b"MULTI",), OK), ((b"HSET", b"h", b"f", b"v", b"dangling"), QUEUED),
        ((b"MSET", b"k", b"v", b"dangling"), QUEUED),
        ((b"EXEC",), Array((Error(b"ERR wrong number of arguments for 'hset' command"),
                             Error(b"ERR wrong number of arguments for 'mset' command")))),
        ((b"DBSIZE",), Integer(0))),
    "discard": (
        ((b"WATCH", b"k"), OK), ((b"MULTI",), OK), ((b"SET", b"k", b"v"), QUEUED),
        ((b"DISCARD",), OK), ((b"GET", b"k"), Bulk(None)), ((b"SET", b"k", b"new"), OK),
        ((b"MULTI",), OK), ((b"EXEC",), Array(()))),
    "unwatch": (
        ((b"WATCH", b"k"), OK), ((b"SET", b"k", b"v"), OK), ((b"UNWATCH",), OK),
        ((b"MULTI",), OK), ((b"GET", b"k"), QUEUED), ((b"EXEC",), Array((Bulk(b"v"),)))),
    "queued_unwatch_cannot_rescue_conflict": (
        ((b"WATCH", b"k"), OK), ((b"SET", b"k", b"v"), OK), ((b"MULTI",), OK),
        ((b"UNWATCH",), QUEUED), ((b"EXEC",), Array(None))),
    "queued_unwatch": (
        ((b"WATCH", b"k"), OK), ((b"MULTI",), OK), ((b"UNWATCH",), QUEUED),
        ((b"SET", b"k", b"v"), QUEUED), ((b"EXEC",), Array((OK, OK)))),
    "watch_aba": (
        ((b"WATCH", b"k"), OK), ((b"SET", b"k", b"v"), OK), ((b"DEL", b"k"), Integer(1)),
        ((b"MULTI",), OK), ((b"SET", b"other", b"v"), QUEUED), ((b"EXEC",), Array(None)),
        ((b"GET", b"other"), Bulk(None))),
    "unpaired_exec": (((b"EXEC",), Error(b"ERR EXEC without MULTI")),),
    "unpaired_discard": (((b"DISCARD",), Error(b"ERR DISCARD without MULTI")),),
}

# (setup, mutation, conflict): distinguish actual writes from no-op/failed writes.
WATCH_CASES = [
    ((b"SET", b"k", b"v"), (b"SET", b"k", b"v"), True),
    ((b"SET", b"k", b"v"), (b"SET", b"k", b"x", b"NX"), False),
    ((b"SET", b"k", b"v"), (b"APPEND", b"k", b""), True),
    ((b"SET", b"k", b"1"), (b"INCRBY", b"k", b"0"), True),
    ((b"SET", b"k", b"v"), (b"INCR", b"k"), False),
    ((b"HSET", b"k", b"f", b"v"), (b"HSET", b"k", b"f", b"v"), True),
    ((b"HSET", b"k", b"f", b"v"), (b"HDEL", b"k", b"absent"), False),
    ((b"SADD", b"k", b"a"), (b"SADD", b"k", b"a"), False),
    ((b"SADD", b"k", b"a"), (b"SREM", b"k", b"absent"), False),
    ((b"ZADD", b"k", b"1", b"a"), (b"ZADD", b"k", b"1", b"a"), False),
    ((b"ZADD", b"k", b"1", b"a"), (b"ZADD", b"k", b"2", b"a", b"1", b"a"), True),
    ((b"RPUSH", b"k", b"a"), (b"LPOP", b"k", b"0"), False),
    ((b"SET", b"k", b"v"), (b"PEXPIREAT", b"k", b"4000000000000"), True),
    ((b"SET", b"k", b"v"), (b"PERSIST", b"k"), False),
    ((b"DEL", b"k"), (b"DEL", b"k"), False),
    ((b"DEL", b"k"), (b"FLUSHDB",), False),
    ((b"SET", b"other", b"v"), (b"FLUSHDB",), False),
    ((b"SET", b"k", b"v", b"PX", b"60000"), (b"PERSIST", b"k"), True),
    ((b"SET", b"k", b"v", b"PX", b"60000"), (b"GETEX", b"k", b"PERSIST"), True),
    ((b"SET", b"k", b"v"), (b"GETEX", b"k"), False),
    ((b"SET", b"k", b"v"), (b"GETEX", b"k", b"PX", b"0"), False),
    ((b"SET", b"k", b"v"), (b"DEL", b"k"), True),
    ((b"SET", b"k", b"v"), (b"SADD", b"k", b"a"), False),
    ((b"HSET", b"k", b"f", b"v"), (b"HDEL", b"k", b"f"), True),
    ((b"RPUSH", b"k", b"a"), (b"RPOP", b"k"), True),
    ((b"SADD", b"k", b"a"), (b"SREM", b"k", b"a"), True),
    ((b"ZADD", b"k", b"1", b"a"), (b"ZREM", b"k", b"a"), True),
]

for name, command, error in [
    ("ping", (b"PING", b"a", b"b"), b"ERR wrong number of arguments for 'ping' command"),
    ("pop", (b"LPOP", b"k", b"1", b"x"), b"ERR wrong number of arguments for 'lpop' command"),
    ("zadd", (b"ZADD", b"z", b"1", b"a", b"2"), b"ERR syntax error"),
    ("zrange", (b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES", b"x"), b"ERR syntax error"),
]:
    TRACES["runtime_variadic_" + name] = (
        ((b"MULTI",), OK), (command, QUEUED), ((b"EXEC",), Array((Error(error),))))
