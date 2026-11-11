# Pub/Sub

SUBSCRIBE, UNSUBSCRIBE, PUBLISH and a bounded pattern subscription subset are
implemented. Messages are transient and node-local. See [validation](validation-and-limitations.md)
for the scope of tested compatibility.

The design follows the official [Pub/Sub contract](https://redis.io/docs/latest/develop/pubsub/),
[SUBSCRIBE](https://redis.io/docs/latest/commands/subscribe/),
[UNSUBSCRIBE](https://redis.io/docs/latest/commands/unsubscribe/) and
[PUBLISH](https://redis.io/docs/latest/commands/publish/). Scoped independent Redis
7.4.12 probes checked wire details; strict differential acceptance subsequently
passed at the post-M11 checkpoint (193 total mandatory comparisons, zero skips).

## Commands and wire values

Channels and messages are arbitrary bytes, including empty bytes, NUL, CRLF and
invalid UTF-8. Channels are separate from database keys; FLUSHDB does not remove
subscriptions. Command names remain case-insensitive; channel names are case-sensitive.

| Form | RESP2 result |
|---|---|
| SUBSCRIBE channel [channel ...] | One array `[bulk subscribe, bulk channel, integer count]` per argument |
| UNSUBSCRIBE [channel ...] | One array `[bulk unsubscribe, bulk channel, integer count]` per argument/removal |
| PSUBSCRIBE pattern [pattern ...] | One `[bulk psubscribe, bulk pattern, integer count]` per argument |
| PUNSUBSCRIBE [pattern ...] | One `[bulk punsubscribe, bulk pattern, integer count]` per argument/removal |
| PUBLISH channel message | Integer matching direct and pattern registrations; direct messages are `[bulk message, bulk channel, bulk message]`, pattern messages are `[bulk pmessage, bulk pattern, bulk channel, bulk message]` |
| PING [payload] while subscribed | Array `[bulk pong, bulk payload]`; absent payload is empty bulk |
| QUIT while subscribed | Simple OK, then close |

Counts combine distinct channel and pattern registrations on that connection. Duplicate SUBSCRIBE still
acknowledges each argument without adding another registration/delivery. Explicit
UNSUBSCRIBE acknowledges duplicates and absent channels. No-argument UNSUBSCRIBE
removes all current channels; when none exist it returns a null bulk channel and
the remaining pattern count. PUNSUBSCRIBE follows the analogous pattern rule.
Local enumeration uses registration order; no Redis enumeration-order
promise is made for unsubscribe-all.

Pattern subscriptions match binary channel bytes. A client registered for both a
channel and matching pattern receives both messages; each matching pattern adds a
delivery and contributes to PUBLISH's count. Channel ACLs require an exact pattern
grant for PSUBSCRIBE/PUNSUBSCRIBE, or the all-channels `&*` grant. Revoking a grant
disconnects affected subscribers. Patterns use the documented ACL glob subset and
are limited to 128 bytes; invalid patterns are rejected rather than silently widened.

While at least one subscription exists, recognized ordinary commands are rejected
with Redis's subscribed-context error. SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE,
PUNSUBSCRIBE, PING and QUIT are
allowed; unknown commands and invalid arity keep registry validation precedence.
After the final unsubscribe, normal commands and simple PING responses resume.
Shard subscriptions, RESET, PUBSUB introspection and RESP3 are unsupported.
The Redis-compatible context error lists Redis's broader command families; that
text does not advertise support for those unimplemented commands.

Pub/Sub commands can queue inside MULTI. EXEC applies them in the existing
indivisible execution path. Queue errors, DISCARD and WATCH conflicts release no
Pub/Sub effects. Variadic subscription commands emit one confirmation per argument
even inside EXEC: Redis declares the queued command count, so extra confirmation/
command values can follow the EXEC array. Preserve this observed RESP2 sequence;
clients must consume the whole push/reply stream. E00 reply admission includes all
these values in the aggregate cap. Rejected EXEC restores subscriptions/output mode
and releases no publications; admitted effects still wait for the commit barrier.

## Ordering, persistence and replication

Each executor owns a broker and byte-channel index. Subscription changes and
recipient selection run synchronously in serialized command execution. Each active
Pub/Sub connection owns one FIFO/writer for confirmations, pushes and later replies.
Publish order is identical for clients continuously subscribed to the same channel;
different publishers are ordered by server execution, not by client send timestamps.
Unsubscribe confirmations follow already queued messages.

Publications are staged until the configured local persistence barrier succeeds.
Mixed EXEC datastore effects commit as a whole before notifications are released.
There is no await of network I/O in datastore mutations or recipient selection.
The existing none/everysec/always policies retain their documented guarantees.

Subscriptions/publications are transient and node-local. They are absent from AOF,
snapshots, replication effects, backlog, offsets and mutation/WATCH revisions. A
read-only or temporarily disconnected replica can serve local Pub/Sub; full state
replacement preserves those live connections. Cross-peer Pub/Sub propagation is
not implemented. Reconnect starts without subscriptions or retained messages.
Delivery is at most once with no replay, acknowledgement or durable receipt.

## Bounds and cleanup

M12 adds server admission/metrics/shutdown hardening and a datastore accounting
budget. Pub/Sub queues retain their own limits and do not count as datastore bytes;
see docs/resources-and-operations.md. Ordinary rejected datastore growth does not
disrupt subscriptions or existing publications. Unexpected allocation failure stops
service and releases connection state.

| ServerConfig field | Default | Meaning |
|---|---:|---|
| max_subscriptions | 1,024 | Distinct channels per connection |
| max_subscription_bytes | 1 MiB | Sum of distinct channel bytes per connection |
| pubsub_queue_bytes | 16 MiB | Encoded queued plus in-flight bytes per connection |
| pubsub_queue_frames | 1,024 | Queued plus in-flight output frames per connection |

The existing subscription count/byte budgets include both channels and patterns.
Globally, the number of distinct patterns is capped at `max_subscriptions`; one
PSUBSCRIBE can compile at most 4,096 bytes of new distinct pattern text. PUBLISH
has a shared 1,000,000-byte matching-work budget and a preflight aggregate encoded
publication limit of `max(max_reply_bytes, pubsub_queue_bytes)`. Exceeding these
limits returns an error before any publication is delivered. Pattern compiled data
and transient matching memory are outside the datastore accounting budget.

All four limits require positive integers, excluding bool. They are Python
configuration fields, with no new CLI flags. Existing client/input/argument/bulk/
frame/reply/transport-chunk bounds remain. Variadic confirmations preflight their
aggregate reply size before changing subscriptions; oversize messages are rejected
before delivery. A confirmation batch consumes one FIFO frame. These limits do not
constitute a total Python heap ceiling.

On output overflow remove the affected subscriber immediately, log the reason and
abort its transport; do not block other clients. PUBLISH counts matches at execution,
including a client disconnected during subsequent queue admission. The count is not
a delivery receipt. Failed writers, malformed frames, QUIT, cancellation, client EOF
and server shutdown remove registrations and release queued/in-flight ownership.
Unexpected programming errors retain their task-boundary report.

Idle subscribed connections wait for input without an idle read deadline. After the
first input byte the existing whole-command deadline still applies; the final
unsubscribe restores ordinary idle timeout. Write deadlines always apply. The FIFO
writer retains socket ownership after unsubscribing. Such a connection cannot later
be repurposed for PYREDIS.SYNC: a dedicated new replication connection is required.
