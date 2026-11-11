# Core Streams subset

E05A adds a bounded Redis 7.4-oriented subset: `XADD`, `XLEN`, `XRANGE`,
`XREVRANGE`, `XREAD`, `XDEL` and `XTRIM`. Keys, fields and values are bytes;
duplicate fields and their order are preserved. `XADD` accepts explicit `ms-seq`
or `ms`, `*`, and `ms-*` IDs. Automatic IDs use the injectable millisecond clock,
advance the sequence in the same millisecond, and keep advancing when the clock
moves backward. IDs are unsigned 64-bit pairs and never repeat or wrap. Removing
the last entry preserves the stream key and its last generated ID.

`XADD` and `XTRIM` implement exact `MAXLEN [=] count`; approximate trimming, MINID,
LIMIT, NOMKSTREAM and other unlisted options are unsupported. XRANGE/XREVRANGE
support inclusive or `(` exclusive boundaries and optional COUNT. XREAD supports
COUNT, BLOCK in milliseconds, multiple streams and `$`. Blocked readers share the
bounded waiter infrastructure, receive no provisional transaction state and do not
consume entries. They are released on timeout, disconnect, ACL revocation or replica
link loss. Within MULTI/EXEC, XREAD never waits. Network loss after a committed write
still leaves the client uncertain about a retry.

Each stream's entry count plus field-pair count is limited by `max_collection_items`;
raw field/value bytes are limited by `max_collection_bytes`. Memory accounting adds
128 units per entry and 64 per field pair, plus raw bytes and the normal key charge;
this is not a process RSS limit. Input, per-bulk, response and blocked-reader limits
still apply. Stream mutations copy the ordered tuple and may take O(N) time and
temporary memory; range and read operations can also scan O(N) entries. These costs
are bounded by configured collection limits, not a latency guarantee.

The journal/snapshot postimage uses typed kind 6 and an explicit stream payload
version 1, storing every entry ID, field pair and last generated ID. Existing effects
and snapshots remain readable. Unknown stream versions and unsupported typed values
fail closed. An older PyRedis peer unable to decode kind 6 must disconnect/fail; this
is not Redis AOF, RDB or PSYNC interoperability. Replication transfers committed
postimages and does not regenerate IDs. Focused tests cover restart, process kill,
compaction, full/partial resync and blocked replica reads; the final full gate and
release audit remain pending.

## Partial consumer groups (E05B)

`XGROUP CREATE key group id [MKSTREAM]`, `XGROUP DESTROY key group`,
`XGROUP SETID key group id`, `XGROUP CREATECONSUMER key group consumer`,
`XGROUP DELCONSUMER key group consumer`, and `XACK key group id [id ...]` are
implemented. Deleting a consumer removes its pending records and returns their
count; applications must not assume those records will be transferred. The summary
form `XPENDING key group` and extended form `XPENDING key group [IDLE ms] start end
count [consumer]` are supported, including exclusive `(` bounds. `XREADGROUP GROUP
group consumer [COUNT n] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]`
supports multiple streams. `>` delivers new entries in order; an explicit ID reads
that consumer's pending entries and updates delivery metadata. A deleted or trimmed
pending entry is returned with a null array for its fields. Every requested stream
and group is checked before delivery, and combined reply size and memory growth are
admitted before publishing group updates. Consumers remain until deleted or the group
is destroyed; pending records remain until acknowledged, claimed after the stream
entry was deleted, or their consumer/group is deleted.

BLOCK uses the shared bounded waiter queue and never waits inside the serialized
executor. A zero timeout waits indefinitely; timeout returns a null RESP2 array.
The timeout starts after the initial serialized availability check completes.
ACL permissions and group state are checked again before delivery. Waiting clients
are served in registration order when eligible, and disconnect, cancellation,
shutdown, group destruction and active expiration release the waiter. Blocking
requests permit at most 64 keys and 65,536 retained argument bytes; the total waiter
limit is the lesser of 256 and max_clients. An initial blocked read may create a
consumer even if it later times out. On replicas XREADGROUP returns READONLY because
delivery updates metadata. In MULTI/EXEC, XREADGROUP is queued and BLOCK is
nonblocking; the existing reply admission can roll back the whole transaction.
Journal preflight conservatively accounts for earlier queued XADD operations, so a
transaction can be rejected even when its eventual effect might fit. XINFO and some
claim options remain unsupported, so E05B remains partial.

`XCLAIM key group consumer min-idle-ms id [id ...] [JUSTID]` transfers eligible
pending entries, updates delivery time and, unless JUSTID is used, increments the
delivery count. It removes stale pending records whose stream entries were deleted.
Other Redis XCLAIM options, including IDLE, TIME, RETRYCOUNT, FORCE and LASTID,
are explicitly unsupported. `XAUTOCLAIM key group consumer min-idle-ms start
[COUNT count] [JUSTID]` returns `[next-id, claimed, deleted-ids]`; callers continue
from the returned ID until `0-0`. It removes stale pending IDs even when their idle
threshold is unmet. Both commands can create the named consumer when nothing is
claimed. They execute as mutations, including within MULTI/EXEC, and use the
existing reply-admission and journal/replication path.

Extended XPENDING and XAUTOCLAIM reject result counts over 1,024. XPENDING examines
at most 4,096 pending records per call and returns an error if the work cap is reached
before finding the requested result or end of range. XAUTOCLAIM examines at most
`min(10 * COUNT, 4096)` pending records and returns a continuation cursor when more
remain. Claim operations prepare a copy of the pending metadata, so their total work
and temporary memory can still be O(N) in the bounded collection size. Oversized
replies and failed growth checks are rejected before committing the group changes.
`XINFO STREAM`, `XINFO GROUPS` and `XINFO CONSUMERS` are unsupported; the existing
format does not retain all Redis consumer activity metadata needed for their replies.

Group names, consumers and entries are binary-safe. ACL checks apply to group keys.
Group, consumer and pending records count against collection and datastore budgets;
copying and historical pending reads can take O(N) time and temporary memory.
Version 2 of stream kind 6 stores group last IDs, consumers, pending owners, delivery
counts and timestamps with each typed postimage. Version 1 remains readable and is
still used when no groups exist. Older peers that cannot decode version 2 fail closed;
upgrade both ends before using groups. Group mutations follow the configured journal
fsync policy, snapshot and PyRedis replication effect ordering. Replicas are read-only
for XREADGROUP because delivery updates metadata. Tests exercise compaction, real
process termination, full/partial sync and failure injection; they do not establish
end-to-end exactly-once processing. Applications must handle redelivery and uncertain
outcomes after network loss, and this subset does not implement claiming for recovery
without the documented claim forms. Applications must handle duplicate delivery and
acknowledge successful processing.
