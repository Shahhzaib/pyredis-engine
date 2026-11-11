# M06 transactions and optimistic locking

Forms: MULTI, EXEC, DISCARD, WATCH key [key ...], UNWATCH. Keys and queued
arguments remain bytes. Official [transaction semantics](https://redis.io/docs/latest/develop/using-commands/transactions/)
and measured Redis 7.4.12 traces govern the supported subset.

Each TCP connection has its own ClientState. MULTI returns OK; validated commands
return QUEUED without executing. EXEC returns an ordered array of command replies,
or an empty array for an empty queue. Registry existence and Redis metadata arity
are checked at queue time; unknown commands/arity failures poison EXEC, which
returns EXECABORT and executes nothing. Incorrect EXEC arity inside MULTI aborts
immediately with a reason and clears the transaction. Nested MULTI and WATCH inside
MULTI return their control errors without poisoning an otherwise valid queue.

Syntax/value/type/pair-shape errors are runtime replies inside EXEC. Later commands
still execute and successful effects remain; no rollback is provided. Variadic
metadata validation differs from a handler's supported forms: for example, malformed
HSET/MSET pairs queue then return arity errors at runtime; malformed ZADD pairs and
excess ZRANGE options return syntax errors. PING/pop excess arguments queue then
return runtime arity errors. Unsupported options remain unsupported.

DISCARD clears the queue and watches. EXEC/DISCARD without MULTI error. UNWATCH
outside MULTI clears watches; inside MULTI it is queued, so it cannot rescue a
pre-existing conflict. QUIT executes immediately and discards pending state. EOF,
invalid framing, timeout, disconnect and server shutdown release queues and reverse
watch registrations; none implicitly commits a transaction.

WATCH uses the existing observable-write revision hook with a reverse key/client
index, including watched absent keys. Successful EXEC, aborted EXEC, DISCARD,
UNWATCH and disconnect remove registrations. No absent-key tombstones accumulate.
Before EXEC, passive expiry checks watched keys at the frozen command time, even
when no sweeper has run. Missing at initial WATCH is valid; expiration or create/
delete/recreate after registration conflicts. Watches are removed before executing
the accepted batch, so its own writes do not retroactively abort it.

Redis 7.4.12 comparisons establish: identical SET/HSET, empty APPEND, INCRBY zero,
deadline updates (including unchanged deadlines), actual PERSIST, collection writes,
deletion and expiration conflict. Failed writes, unmet SET conditions, duplicate
SADD, unchanged ZADD, missing removals, count-zero pops, GETEX without options and
PERSIST without expiry do not conflict. ZADD duplicate pairs that change and restore
a score do conflict. FLUSHDB does not conflict for watched keys absent throughout.
Other keys do not conflict. Internal revisions remain implementation unit invariants.

One synchronous Executor runs the whole EXEC at one clock snapshot, without await,
network/disk I/O or interleaving active-expiry batches. Thread ownership is enforced
for execution and client lifecycle. This is isolation, not concurrent thread access.
M07 captures the complete atomic effect group, including successful siblings around
runtime errors, with absolute deadlines; its commit barrier precedes replies.
See docs/persistence.md for policy guarantees and failure handling.

Additional ServerConfig limits (positive integers) bound each connection:

| Setting | Default | Accounting |
|---|---:|---|
| max_transaction_commands | 1,024 | queued commands, including zero-length arguments |
| max_transaction_bytes | 1 MiB | total byte lengths of queued command names/arguments |
| max_watched_keys | 1,024 | distinct watched keys |
| max_watch_bytes | 1 MiB | byte lengths of distinct watched keys |

Queue exhaustion errors poison EXEC; effects remain unexecuted. WATCH additions
preflight both limits atomically. Argument/frame/client limits also bound metadata.
Individual handler limits and reply preflight remain in force. Aggregate replies, including
the array header and extra Pub/Sub confirmations, must fit max_reply_bytes exactly.
An exact-fit reply is accepted. Overflow returns
`EXECABORT Transaction discarded because reply exceeds configured maximum size`
without committing queued effects. The transaction and its WATCH registrations
are cleared; later commands on the connection remain usable.

EXEC executes once at frozen command time in a synchronous savepoint, retaining one
original value/deadline/revision/charge per touched key and ordered observer events.
Collection handlers already prepare private replacements. No other execution or
network/disk I/O interleaves. Build the complete bounded CommandResult, then admit
effects to WATCH/capture observers. On overflow, discard provisional datastore and
subscription changes and publications. Earlier writes changing later reads are
therefore included in the actual size, rather than guessed from initial values.
WATCH expiration checks occur before admission; those ordinary expiration effects
can still commit even when WATCH aborts. Expiration encountered only inside a
rejected batch is discarded with that batch and remains eligible for the sweeper.

Normal runtime errors remain elements of an accepted array; successful siblings
still commit. Reply admission is a custom resource rejection, not general Redis
transaction rollback. Preimages/observer events use transient memory bounded by
existing datastore/transaction limits, outside committed accounting; no total RSS
ceiling or recovery from arbitrary allocator failure is promised. Unexpected
allocation/observer failures retain existing fail-stop handling.

Persistence still journals admitted effects as one group and applies the configured
fsync barrier before responding or releasing publications. No partial reply array
is sent. A failed disk operation or a connection lost after admission can leave an
unacknowledged transaction committed; connection loss is not an EXECABORT and must
not be blindly retried. This correction eliminates the deliberate local size-error
replacement after commitment, not the unavoidable uncertainty of network failures.

Tests cover exact traces, state models, 128-client atomic increments and contended
WATCH, fragmentation, invalid frames, expiry, disconnect/shutdown and resource caps.
The pre-existing generic unknown-command text remains a documented difference.

E01A checks permissions at queue time and again before executing the batch. Revoked
queued commands produce Redis-compatible NOPERM elements without their effects;
authorized siblings retain runtime-error semantics. Revoked WATCH key access aborts
before passive expiry. E00 admission still bounds the whole result, including these
errors. AUTH/ACL controls cannot queue in this subset, avoiding credential retention
and in-batch identity/policy changes. See [security](operations-and-security.md).
Transactions across listeners or databases and complete Redis command support are not provided.
