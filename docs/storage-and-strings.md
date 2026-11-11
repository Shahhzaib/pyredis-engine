# M02 storage, strings and execution

M12 adds a documented no-eviction accounting budget across all stored types. MSET
checks whole-batch replacement credits; rejected growth preserves state/revisions.
See [resources and operations](resources-and-operations.md) for exact charges and
transient-memory exclusions. This is not a Python RSS ceiling or Redis memory parity.

M02 implements one in-memory database with bytes keys and immutable `StringValue`
payloads. An absent key returns `Bulk(None)`; an existing empty value returns
`Bulk(b"")`. Only command names are ASCII case-insensitive. Keys are case-sensitive,
may be empty, and may contain any byte sequence. No value is decoded as UTF-8.
Selected command forms pass local and Redis 7.4.12 tests; see [validation](validation-and-limitations.md).
M03 layers expiration onto this engine; where behavior is extended,
`docs/expiration-and-clock.md` specifies the current TTL/options contract.
M04 adds HashValue/ListValue and the listed hash/list commands; see
`docs/hashes-and-lists.md` for current collection ownership and behavior.
M05 adds SetValue/SortedSetValue and the listed set/sorted-set commands; see
`docs/sets-and-sorted-sets.md` for forms, score policy, ownership and limits.

`engine/store.py` owns typed values and keyspace access. `commands/registry.py` owns
metadata, arity, pair-count validation and expected command errors. `commands/strings.py`,
`commands/keyspace.py` and `commands/smoke.py` implement the registered commands.
`engine/executor.py` is the single execution entry point shared by all connections
to one listener. Each listener has independent state; reconnecting preserves state
until that listener's process ends or FLUSHDB is requested.

The executor is synchronous and checks its owning thread. It has no awaits, networking
or disk I/O. Once a complete request enters execution, validation and mutation finish
before another connection can execute. MSET validates all pairs before writing; MGET
observes one consistent state. Each connection executes requests and writes replies
in order. This does not promise fairness or arrival order across separate connections,
or thread-safe direct store access. See [transactions](transactions.md) for transaction behavior.

## Command subset

| Syntax | Result and behavior |
|---|---|
| `SET key value` | `+OK`, create or overwrite any stored kind; no options |
| `GET key` | Bulk value, null if absent, WRONGTYPE for another kind |
| `DEL key [key ...]` | Integer number removed; repeated keys only removed once |
| `EXISTS key [key ...]` | Integer number present; repeated existing keys count each time |
| `TYPE key` | Simple `string` or `none` for states creatable through M02 commands |
| `DBSIZE` | Integer count of keys, including keys holding empty strings |
| `FLUSHDB` | `+OK`, synchronously clear this database; no options |
| `INCR key`, `DECR key` | Integer result; missing starts from zero |
| `INCRBY key increment` | Integer result after signed 64-bit addition |
| `APPEND key value` | Integer byte length; creates a missing key, even for an empty append |
| `STRLEN key` | Integer byte length; zero when missing; WRONGTYPE for another kind |
| `MGET key [key ...]` | Array in requested order, preserving duplicates; missing/non-string values are null |
| `MSET key value [key value ...]` | `+OK`, atomic overwrite; duplicate keys take the final supplied value |

Official command specifications are linked from `docs/command-matrix.md`. Local
contract tests cover every command and exact expected reply types/content. Real
Redis comparison remains a separate acceptance condition; a skipped comparison does
not establish Redis parity or mark M02 complete.

Stored integers are byte strings. Numeric commands accept canonical base-10 signed
64-bit values: `0`, positive digits without leading zeroes, or `-` followed by a
nonzero digit and remaining digits. A plus sign, whitespace, `-0`, leading zeroes,
underscores, binary bytes, empty values and out-of-range magnitudes fail with
`ERR value is not an integer or out of range`. Arithmetic overflow returns
`ERR increment or decrement would overflow`. Both leave state unchanged, including
not creating an absent key when validation fails.

Registry arity failures use `ERR wrong number of arguments for '<name>' command`.
SET with extra tokens and FLUSHDB with any tokens return `ERR syntax error`; those
option forms are outside the supported subset. MSET rejects unmatched key/value
arguments before changing any key. WRONGTYPE replies use
`WRONGTYPE Operation against a key holding the wrong kind of value`. M02's original
tests inject typed fixtures; M04/M05 now provide wire commands to create real
collection values for cross-type comparisons.

## Resource and capability limits

M01's input, frame, header, client, deadline and transport-output ceilings are retained.
Strings, including APPEND results and numeric results, cannot exceed `max_bulk_bytes`.
Rejected growth returns `ERR string exceeds configured maximum size` before mutation.
MSET checks all value sizes before making writes.

`max_reply_bytes` defaults to 16 MiB and must be at least 128 bytes. `reply_size`
computes RESP wire length without allocating encoded payload copies. The executor
checks it before the server serializes a result, preventing repeated-key MGET
amplification. Excess returns `ERR reply exceeds configured maximum size`; the
connection remains usable. Mutation control replies fit the minimum ceiling, so a
committed mutation cannot be reported as a reply-size rejection. These are deliberate
custom limits, not Redis size-limit parity. Total database memory remains unbounded
until M12; do not use this development server for untrusted or production workloads.

M07 adds opt-in custom AOF persistence (docs/persistence.md); replication and
multiple selected databases remain unimplemented. M06 adds transactions/WATCH;
M04/M05 add listed collection commands; unlisted collection
commands remain unavailable. M03 adds EX/PX/NX/XX/KEEPTTL and expiration;
FLUSHDB ASYNC/SYNC, SELECT and CLIENT handshake commands remain unavailable. Unsupported
commands keep the generic `ERR unknown command` response documented since M00.

## Acceptance commands

During development, target `tests/unit/test_storage_strings.py`,
`tests/integration/test_storage_tcp.py`, `tests/property/test_string_model.py` and
`tests/differential/test_m02.py`. The latter always executes 14 local command contract
traces, plus optional private official-Redis comparisons and wrong-type comparisons.
Set `PYREDIS_REDIS_SERVER` to an installed executable or put `redis-server` on PATH.
The fixture starts its own loopback process with disabled persistence and a temporary
directory; it never connects to an existing Redis service or installs a binary.

The concurrency acceptance uses a barrier for 100 live clients, each issuing 20 INCRs
against one key, checks every reply for exact coverage of 1..2000, and reads the final
value. A separate test observes MSET pairs while two writers run, rejecting any mixed
pair. Fragmentation tests check every split of a binary multi-key request before and
after completion. Final quick and full gates are mandatory for this shared execution
change; see [validation](validation-and-limitations.md) for observed results and limits.
