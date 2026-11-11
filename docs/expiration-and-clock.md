# M03 expiration and clock contract

Selected expiration forms have passed local tests and comparisons with Redis 7.4.12.

## Commands and boundaries

Implemented forms are EXPIRE/PEXPIRE key duration, EXPIREAT/PEXPIREAT key timestamp,
TTL/PTTL key, PERSIST key, SET key value with EX/PX/NX/XX/KEEPTTL, and GETEX key with
no option or EX/PX/EXAT/PXAT/PERSIST. Options are ASCII case-insensitive. Base expiration
commands reject optional NX/XX/GT/LT conditions; SET GET/EXAT/PXAT and other unlisted
forms remain unsupported. GETEX supports absolute EXAT/PXAT, but SET does not.

Reference contracts: [EXPIRE](https://redis.io/docs/latest/commands/expire/),
[SET](https://redis.io/docs/latest/commands/set/),
[GETEX](https://redis.io/docs/latest/commands/getex/),
[TTL](https://redis.io/docs/latest/commands/ttl/),
[PTTL](https://redis.io/docs/latest/commands/pttl/),
[PERSIST](https://redis.io/docs/latest/commands/persist/).

Deadlines are absolute signed-64-bit UNIX milliseconds. A relative duration is
converted once using a single command-time snapshot. Seconds multiplication and
addition are checked before mutation; Python's unbounded integers do not silently
extend the supported deadline range. Numeric arguments retain canonical signed
decimal validation from M02. Invalid numbers return `ERR value is not an integer or
out of range`; invalid/overflowing expiration values return
`ERR invalid expire time in '<command>' command`.

SET EX/PX and all GETEX numeric options require positive arguments. EXPIRE/PEXPIRE
with nonpositive durations and EXPIREAT/PEXPIREAT with deadlines at/before command
time immediately remove an existing key, returning 1, or return 0 if missing. A valid
GETEX absolute deadline at/before now returns the old value and removes the key.
GETEX with no option leaves the current deadline/revision unchanged.

Lazy and active checks remove a key when now_ms > expires_at_ms. Thus a key at exact
deadline equality has PTTL 0 until the next millisecond. TTL returns (remaining_ms +
500) // 1000; TTL/PTTL return -1 without expiration and -2 when missing/expired. Exact
local boundaries use an injected clock; reference precision samples pass measured
Redis TIME brackets without depending on identical wall-clock scheduling.

SET and MSET clear expiration on replacement. SET KEEPTTL retains a live deadline,
while APPEND and integer updates preserve it. SET NX/XX treats an expired key as
absent; an unmet condition returns a null bulk and does not change live metadata.
Conflicting expiry units/conditions are syntax errors. Repeated identical SET flags
are accepted; repeated identical numeric units use the last value. GETEX similarly
allows repetition of its same option kind but rejects conflicting kinds.

GETEX validates option syntax, then key absence/type, then the final numeric timeout
for an existing string. A missing/expired key returns null even for `PX bad`/`PX 0`;
a live non-string returns WRONGTYPE. Invalid syntax still errors for every key kind.
Repeated identical timeout options validate only the final supplied value (so
`GETEX key PX bad PX 60000` succeeds on a string). Live-string numeric errors
preserve contents, deadline and revision. GETEX preflights its bulk reply before
changing a deadline.
A reply-size rejection leaves live value, deadline and revision unchanged. This
extends M02's atomic error behavior to a command that returns data while mutating TTL.
Expiry metadata works for all internal value kinds; GETEX remains string-only and
raises WRONGTYPE without changing a non-string value's deadline.

## Storage, execution and bounded work

`engine/clock.py` supplies Clock, SystemClock (time.time_ns divided into milliseconds),
and ManualClock. Executor.execute and Executor.expire_batch freeze one time reading
through Store.at_time. Existing command handler signatures are unchanged. Every
network command and active-expiration batch mutates through the same synchronous,
thread-confined executor, without awaits or network I/O during state changes.

Store owns values, live-key revisions and an ExpirationIndex. The index is a minimum
heap plus key-to-position map: precisely one node per expiring key, O(log N) update
and removal, no stale-node buildup when a far-future deadline is repeatedly changed.
Replacement, PERSIST, DEL, expiration and FLUSHDB clean up associated metadata.

Active passes remove at most expiry_batch_size keys (default 128), once per
expiry_interval (default 0.1 seconds). Both settings are validated. Worker waits use
asyncio's monotonic scheduling; the due-key decision uses absolute wall time. A large
backlog takes multiple ticks. Passive checks preserve logical absence immediately
for the accessed keys, independent of cleanup backlog. A backward clock jump cannot
resurrect a key already removed. No database-wide memory ceiling is added before M12.

DBSIZE counts logically live keys in O(N) without deleting the whole due backlog.
This deliberate project behavior can differ from Redis's physical count before its
cleanup runs, and changes the M02 O(1) count cost. Ordinary commands do not scan the
whole database. MGET reads all requested keys at the same command-time snapshot.

Every actual write/deadline update/removal advances the store mutation sequence;
live key revisions use that sequence. A synchronous optional on_change(key, revision)
hook observes writes, expiration and deletes, including expire/recreate ABA cases.
Callbacks are trusted internal observers: they must not await, perform I/O, raise,
or reenter mutation. Deleted keys retain no revision tombstones. M06 WATCH subscribes
subscribe to changes for watched absent keys; key_revision(missing) == 0 alone is
not a complete WATCH token. See docs/transactions.md for the M06 implementation.

## Server lifecycle and future persistence

start_server now returns ManagedServer, preserving the used sockets, close,
wait_closed, is_serving, serve_forever and async-context interfaces. It also accepts
an injected clock and an expiry_wait callable for deterministic scheduler tests.
It is not an asyncio.Server subclass. The owner tracks and closes client writers,
joins connection tasks and cancels/joins its single expiration task on close or
serve_forever cancellation. Write/close waits remain bounded. Unexpected background
errors are reported to the event-loop exception handler and close the server;
they are not silently ignored. See [persistence](persistence.md) for shutdown and durability.

The journal and snapshots preserve absolute expiration deadlines, not remaining
durations. Recovery compares those deadlines to the current clock before exposing
keys. See [persistence](persistence.md) for recovery and durability guarantees.
