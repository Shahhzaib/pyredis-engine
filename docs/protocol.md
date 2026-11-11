# M01 RESP2 and connection contract

## RESP3 development status

RESP3 client support is **partial**. Connections start in RESP2 and may select
RESP3 using HELLO 3, or return to RESP2 using HELLO 2. HELLO can authenticate with
`AUTH username password` and set a client name with `SETNAME name`. Failed parsing,
authentication or reply admission leaves the prior identity, protocol and client
name unchanged. Each connection has
its own mode; PyRedis peer replication still uses its original private protocol.

The executor converts replies to explicit RESP3 values before sizing. EXEC checks
each selected-protocol reply inside its savepoint before publishing the transaction;
standalone RESP3 writes likewise stage mutations before admitting their replies.
This preserves the E00 distinction between a locally rejected oversized reply and
an uncertain result after a network failure. Maps for HELLO/HGETALL/stream reads,
sets for SMEMBERS, sorted-set doubles and scored ranges, nulls and subscription
pushes have focused tests. Other command reply shapes and floating-point spellings
have not been comprehensively audited against Redis and may differ.

RESP3 channel and pattern notifications use the existing bounded output FIFO;
ordinary commands are allowed while subscribed. Subscription changes inside a
RESP3 MULTI are currently rejected. The RESP2 restriction remains unchanged.

The codec in `src/pyredis_engine/protocol.py` follows the
[official RESP specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).
`read_value` and `serialize_reply` support `Simple`, `Error`, signed 64-bit `Integer`,
`Bulk` (including null), and nested `Array` (including empty and null). Payloads are
bytes; bulk lengths count bytes, including NUL, CRLF and invalid UTF-8. Simple strings
and errors cannot contain CR or LF. Integers accept an optional sign. Lengths use
unsigned decimal digits, with exactly `-1` reserved for null bulk strings and arrays.
Other negative lengths, whitespace and nondecimal lengths are invalid.

`read_value` returns Python `None` only on EOF before a new frame. RESP nulls are
`Bulk(None)` and `Array(None)`. Mid-frame EOF raises `ProtocolError`. A caller using
the codec outside the server must configure its StreamReader header limit and apply
a whole-frame deadline, as the server does.

TCP requests remain nonempty arrays of non-null bulk strings. Empty argument bytes
are valid. Other RESP2 values can be decoded as values, but cannot be used in command
frames. Inline requests are unsupported. Commands execute and replies drain
in connection order. Incomplete messages wait for more input; a read deadline covers
the entire next command, including idle time, rather than restarting on each packet.
QUIT replies `+OK` and discards any following pipelined requests on that connection.

Malformed framing, invalid command structure and resource violations produce the
fixed reply `-ERR Protocol error\r\n` where the socket is writable, followed by close.
The connection does not try to resynchronize. Already completed pipeline replies are
preserved unless the hard input cap was exceeded before that data could be processed.
Command errors (unknown command or wrong arity) leave the connection usable. Read or
write timeouts close the connection; broken sockets may prevent any error reply.

## Limits

`ServerConfig` exposes the following limits. Only existing host, port, max-client and
read-timeout CLI flags are exposed; other limits are configured through Python.

| Configuration | Default | Enforcement |
|---|---|---|
| `max_bulk_bytes` | 8 MiB | Declared bulk bodies and M02 stored string values |
| `max_frame_bytes` | 16 MiB | All headers, bodies and terminators in one frame |
| `max_args` | 1,024 | Elements in each array, including command arguments |
| `max_depth` | 64 | Nested arrays; root array counts as one; configurable 1–128 |
| `max_header_bytes` | 8,192 | Complete header including prefix and CRLF |
| `max_input_bytes` | 16 MiB | Unconsumed bytes retained by one connection reader |
| `max_output_bytes` | 64 KiB | Maximum transport write chunk; drain before next chunk |
| `max_reply_bytes` | 16 MiB | M02 response preflight before serialization; minimum 128 bytes |
| `max_clients` | 256 | Concurrent admitted clients |
| `read_timeout` | 10 seconds | Idle plus whole-command deadline |
| `write_timeout` | 10 seconds | Whole-reply deadline and separate close deadline |
| `shutdown_timeout` | 10 seconds | Client-task grace before cancellation during stop |
| `maxmemory_bytes` | 256 MiB | Accounted committed datastore budget; no eviction or RSS ceiling |

The input cap must fit the configured maximum bulk body plus CRLF and the maximum
header. Asyncio also pauses reads at its header watermark, resuming when a bulk read
needs more data. The hard input cap is enforced before retaining each transport
delivery. Even valid pipeline bursts are rejected if they exceed available buffer
capacity. Declared arrays grow only as elements arrive; prefixes never preallocate
lists or bodies. Frame accounting limits the total nested contents.

The transport output watermark is zero so every write chunk drains before the next.
A serialized reply is still held in application memory, bounded by `max_reply_bytes`
through the M02 executor, in addition to stored data, input, parsed arguments and
operating-system socket buffers. `reply_size` counts wire bytes without encoding the
payload first; repeated-key MGET cannot bypass this cap.
These limits are not a total process memory guarantee. A close that exceeds its
deadline aborts the transport. Ordinary clients cannot accumulate a reply queue.

M11 adds a bounded queue specifically for activated Pub/Sub connections, as described
in [Pub/Sub](pubsub.md). Confirmations, messages and later replies use one writer/FIFO;
queued plus in-flight bytes/frames are capped. Exceeding either cap disconnects the
subscriber. Idle subscribed connections wait for their first input byte, then retain
the whole-command deadline. Ordinary idle timeout resumes after final unsubscribe.
All framing, byte-length, input and transport-chunk limits above remain enforced.
M12 bounds rejection work as well as admitted clients and adds type-checked limits,
metrics and signal shutdown; see [resources and operations](resources-and-operations.md).

M02 strings/keyspace and provisional M03 expiration extend PING, ECHO and QUIT; see
the matrix, `docs/storage-and-strings.md` and `docs/expiration-and-clock.md` for exact
subsets and verification status. Authentication, persistence and private replication
are described in the current security, persistence and replication documents.
ManagedServer closes clients and joins its owned connection and expiration tasks.

M04 adds hash/list commands without changing RESP framing. Counted pops return arrays;
scalar pops return bulk/null. HGETALL is a flat field/value array. Collection replies
are preflighted against the existing max_reply_bytes, and pops check before mutation.
Reply arrays may exceed the inbound command-argument ceiling while remaining within
collection/reply limits. See `docs/hashes-and-lists.md` for the supported subset.

M05 adds set/sorted-set commands without changing framing or limits. ZSCORE returns
a bulk/null; ZRANGE WITHSCORES emits alternating member/score bulks. SMEMBERS has
reproducible byte order locally without a Redis set-order promise. Both array
commands use the bounded reply builder. See `docs/sets-and-sorted-sets.md`.
