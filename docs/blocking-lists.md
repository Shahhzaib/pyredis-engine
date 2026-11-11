# Blocking list pops

`BLPOP key [key ...] timeout` and `BRPOP key [key ...] timeout` first check keys in
argument order and return `[bulk key, bulk value]`; an empty timed wait returns a
null array. Timeout is seconds, with zero meaning indefinite. Wrong types and invalid
timeouts return errors. Valid numeric syntax includes decimal and hexadecimal forms;
timeout argument length is capped at 128 bytes. Reply size is checked before removal.
List writes preserve their normal expiration semantics, and an empty list key is
removed. Inside MULTI/EXEC the commands never wait: an unavailable list returns a
null array in the EXEC response.

The server registers a bounded FIFO waiter without waiting inside the executor.
After a complete committed mutation or transaction, it checks ready lists, rechecks
authorization and serves waiters in registration order. The first eligible key in a
request is selected. Each pop uses the existing serialized executor, effect journal,
configured append/fsync barrier and replication stream before its response is sent.
Expiration and WATCH revisions use the normal store path. Waiters are transient,
not persisted or replicated, and replicas reject pops as writes.

At most 256 waiters (or the configured max clients, whichever is smaller), 64 keys
and 65,536 key bytes per waiter are allowed. A connection has at most one waiter.
Timeout, cancellation, EOF, input-buffer failure, ACL revocation and shutdown release
registrations. One coalesced task services initial checks under the persistence lock,
so disconnecting repeatedly while the lock is busy does not accumulate tasks. While
blocked, pipelined input remains subject to the normal input limit. An ACL change is
checked before a not-yet-admitted pop. Timeout begins after the initial serialized
availability check; a pop already admitted to the durability barrier completes even
if its client disconnects. As with any committed write, a lost response can leave a
client uncertain about a retry. No exactly-once delivery is claimed.

These bounded policies are PyRedis-specific. BLPOP/BRPOP are not an implementation
of blocking sorted-set operations, BLMOVE, Streams or Redis replication interoperability.
See the [command matrix](command-matrix.md) and [validation](validation-and-limitations.md).
