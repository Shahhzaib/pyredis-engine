# Bounded SCAN subset

## E03 derived collection indexes

For hash and set values, prepare a sorted tuple of field/member references before
publishing each copy-on-write value. A cursor keeps only the last examined member;
each page seeks with binary search and examines at most the configured step cap.
Thus a scan does not retain old collection versions, and continuously present
members remain reachable across ordinary mutations to an existing collection. The
index is derived state, rebuilt during restore/replica installation, charged under
maxmemory and prepared before publishing a value. Ordinary mutations update the
previous sorted tuple from changed members instead of re-sorting the collection.
A value-only HSET reuses the tuple. Single-member membership changes copy O(N)
references; multi-member updates cost O(N + K log K) for K additions. Full recovery
reconstruction still costs O(N log N). Scans seek in O(log N) and then do O(COUNT)
indexed visits. Keyspace indexing requires a separate design that handles
EXEC rollback, FLUSHDB and whole-state replacement; it remains open.

`SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`, and `HSCAN`, `SSCAN`
and `ZSCAN key cursor [MATCH pattern] [COUNT count]`, return RESP2 arrays containing
a bulk cursor and an array of bulk values. Hash scans return field/value pairs;
sorted-set scans return member/score pairs. TYPE is supported only for SCAN.
MATCH is binary-safe and uses the documented ACL glob subset, with a 128-byte
pattern limit; malformed or oversized patterns return an error. COUNT is a positive
hint, capped at 1,024 yielded iterator entries per call. Pages may be empty and an
extra empty page may be needed to reach cursor `0`.

Each connection owns at most one active cursor. Starting a cursor at zero replaces
its prior cursor. The cursor must be resumed on the same connection and with the
same command and collection key; stale, foreign and malformed cursors return errors.
The iterator does not copy or sort the collection. ZSCAN walks the existing ranked
list, so each traversal step is an indexed-list step even when the score hash table
contains deleted slots. The emitted order is an implementation detail, not a
Redis ordering guarantee. A keyspace cursor continues across value-only updates,
but is invalidated by key creation, deletion, expiration, clearing, state replacement,
or even an aborted transaction that temporarily changes keyspace structure: those
operations can invalidate Python's underlying dictionary iterator. ZSCAN is
invalidated by changes to its key. HSCAN and SSCAN continue across changes
to an existing collection of the same type, seeking after the last examined member.
Insertions behind the cursor may be missed, and an inserted-then-deleted member has
no inclusion guarantee. Deleting, expiring or changing the type of the target, or
clearing the database, invalidates its cursor. Invalidation releases references; a
subsequent continuation returns an error. These differences from Redis remain.
These commands are rejected inside MULTI to avoid advancing connection state during
a transaction whose reply may be rolled back.

Stable, bounded collections eventually return cursor `0`. Missing or expired target
collections return an empty page and `0`; expired keyspace entries are omitted.
SCAN returns only keys allowed by the current user's key ACL; collection scan keys
are checked before access. Iteration rechecks current authorization on each call.

The number of examined entries per call, matching budget, response size and retained
collection key (4,096 bytes) are bounded. HSCAN/SSCAN also retain one last-examined
member, up to the configured maximum bulk size per active connection; this transient
retention is outside datastore accounting. Byte comparisons during the binary seek
depend on member length. SCAN still uses a Python dictionary iterator whose single
step can traverse internal deleted slots, so its entry cap is not a hard work bound.
ZSCAN uses the ranked list but still invalidates on mutations. Python overhead and
matching/formatting costs prevent an exact CPU-time or RSS guarantee.

Each hash field and set member is charged 72 accounting units plus payload bytes,
including eight additional units for an index reference. A prior persisted dataset
near its maxmemory limit can therefore require a higher budget after upgrade; it is
rejected rather than silently truncated. Retain the original journal/snapshot,
calculate at least eight additional units per recovered hash field and set member,
increase maxmemory with appropriate headroom, then retry recovery; do not discard
records or enable tail repair merely to bypass an accounting rejection. Indexes are
not persisted. Each restored
effect group prepares its indexes before mutating live state; snapshots and replica
installation build the same derived indexes. EXEC rollback restores the previous
copy-on-write value and index. Transient index copies are outside the accounting
budget and remain subject to the existing collection and connection limits.

## Mutation cost measurement

`scripts/benchmark_scan_mutations.py` measures in-process HSET, HDEL, SADD and
SREM with a warmup, repeated samples, post-operation checks and process peak RSS.
The 2026-11-11 comparison uses isolated source archives, 1,000/5,000/20,000-member
collections and a fixed Python hash seed. The full sorted-index rebuild caused a
large regression; delta updates reduced it substantially, though HDEL and other
membership changes still pay for tuple copies. Measurements and their limitations are in the
[machine-readable results](../benchmarks/results/E03-mutation-comparison.json).
They are not network throughput or exact per-index RSS measurements.
