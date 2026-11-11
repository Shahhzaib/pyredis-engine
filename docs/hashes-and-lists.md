# M04 hashes and lists

Selected hash and list forms have passed local tests and Redis 7.4.12 comparisons.
See [sets and sorted sets](sets-and-sorted-sets.md) for other collection types.

## Implemented command forms

| Form | Reply and behavior | Reference |
|---|---|---|
| `HSET key field value [field value ...]` | Integer count of distinct newly added fields; updates return zero; final duplicate wins | [HSET](https://redis.io/docs/latest/commands/hset/) |
| `HGET key field` | Bulk value or null for missing key/field | [HGET](https://redis.io/docs/latest/commands/hget/) |
| `HDEL key field [field ...]` | Integer count removed; duplicate fields count once; delete the key after its final field | [HDEL](https://redis.io/docs/latest/commands/hdel/) |
| `HEXISTS key field` | Integer 1 or 0, including fields holding empty values | [HEXISTS](https://redis.io/docs/latest/commands/hexists/) |
| `HGETALL key` | Flat RESP2 array alternating fields and values; missing is an empty array | [HGETALL](https://redis.io/docs/latest/commands/hgetall/) |
| `HLEN key` | Field count; missing is zero | [HLEN](https://redis.io/docs/latest/commands/hlen/) |
| `LPUSH key element [element ...]` | New length; each argument is pushed left, reversing argument order at the head | [LPUSH](https://redis.io/docs/latest/commands/lpush/) |
| `RPUSH key element [element ...]` | New length; append arguments in supplied order | [RPUSH](https://redis.io/docs/latest/commands/rpush/) |
| `LPOP key [count]` | Scalar bulk without count; array with count, in left-pop order | [LPOP](https://redis.io/docs/latest/commands/lpop/) |
| `RPOP key [count]` | Scalar bulk without count; array with count, in right-pop order | [RPOP](https://redis.io/docs/latest/commands/rpop/) |
| `LRANGE key start stop` | Array with inclusive/clamped bounds; negative indexes count from the tail | [LRANGE](https://redis.io/docs/latest/commands/lrange/) |
| `LLEN key` | Element count; missing is zero | [LLEN](https://redis.io/docs/latest/commands/llen/) |

Keys, fields, values and elements are raw bytes and may be empty or contain NUL,
CRLF or invalid UTF-8. Only command names are ASCII case-insensitive. Hash fields
remain case-sensitive. HGETALL emits deterministic insertion order in PyRedis,
but ordering is not a Redis compatibility promise. Differential tests compare all
unique field/value pairs without depending on order; other arrays compare in order.

Pop count is a canonical signed-64-bit nonnegative integer. Zero on an existing list
returns an empty array without mutation. A missing list returns a null bulk without
count and a null array with count, including count zero. Negative, malformed and
out-of-range counts return `ERR value is out of range, must be positive` before
key/type lookup, without mutation. Other commands retain their generic integer error.
Counts larger than the length remove the whole list and are clamped before iteration.
LRANGE clamps signed-64-bit indexes before iterating, returns an empty array for
missing/out-of-range/reversed intervals, and never allocates from a declared index.
Removing the final list element deletes the key, not an empty persistent list.

Registry arity and HSET pair-count failures use the existing exact arity error form.
Wrong-type operations return `WRONGTYPE Operation against a key holding the wrong
kind of value`. There is no implicit conversion between strings, hashes and lists.
MGET returns null for non-strings; SET/MSET can overwrite a collection as before.
Hash field expiration, blocking pops, LINDEX/LSET/LTRIM, HMGET/HMSET/HSETNX and all
other unlisted commands remain unsupported.

## Ownership, atomicity and expiration

engine/collections.py supplies HashValue (dict) and ListValue (deque). StringValue
remains immutable. Collection payloads are mutable internal objects confined to the
synchronous Executor: no handler yields or performs I/O, and consumers must not mutate
them outside that execution path. Store.get_hash/get_list perform typed, expiry-aware
lookup. No persistence or snapshot copy/ownership scheme is implemented here.

HSET validates every argument and projected final growth before updating fields.
Pushes validate the entire argument batch before extending the deque. HDEL performs
work proportional to requested fields; pushes and pops avoid whole-list copying.
LRANGE scans a deque prefix and emits selected elements; its cost can be O(N).

Successful updates use Store.put(keep_ttl=True), preserving the existing absolute
deadline while advancing the mutation revision/hook. Final field/element deletion
uses Store.delete, cleaning expiry and revision metadata. Missing HDEL, count-zero
pops and reads do not advance live revisions. Expired keys act as absent and may be
recreated with another type and no inherited deadline. M03's time snapshot, passive
checks, indexed active batches and managed worker lifecycle are unchanged.

Before a pop mutates a list, its entire scalar/count reply is checked against
max_reply_bytes. Rejected replies preserve contents, TTL and revision. HGETALL/LRANGE
also accumulate wire size while constructing bounded arrays, stopping on overflow.
The error remains `ERR reply exceeds configured maximum size`, and the connection
stays usable. These custom limits intentionally differ from Redis size limits.

## Collection resource limits

ServerConfig adds positive-integer ceilings:

- max_collection_items: 100,000 fields per hash or elements per list.
- max_collection_bytes: 16 MiB payload per collection; hash field plus value bytes,
  or list element bytes. Key bytes and Python object overhead are not counted here.

Each field/value/element also obeys max_bulk_bytes. Excess element payload returns
`ERR collection element exceeds configured maximum size`; aggregate growth beyond
either ceiling returns `ERR collection exceeds configured maximum size`. No partial
live mutation or empty key is committed on these validation failures. Metadata
counts bound growth from empty elements. Updates/deletes/pops adjust byte accounting
so freed capacity can be reused.

Existing frame/header/argument/client/buffer/deadline/reply limits are retained. The
inbound max_args ceiling does not limit reply-array element counts: a bounded stored
collection may yield a larger array. Total process/database memory remains unbounded
until M12; these are per-collection limits, not maxmemory enforcement. Removing a
large collection can free O(N) objects; the M03 active budget counts keys, not elements.
No latency, throughput or production-readiness claim follows from these limits.

## Acceptance evidence

Unit and generated models cover binary data, missing/empty collections, duplicates,
update counts, wrong types, canonical integers, range boundaries, growth/response
limits, TTL/revision retention, last-member deletion and active cleanup. TCP tests
cover fragmentation at every split, pipelined ordering, truncated/oversized writes,
healthy reconnects, concurrent mutation and expiration behavior.

The concurrency scenario uses 100 clients, each adding 20 unique hash fields and 20
list elements. It checks all 2,000 fields/elements and contiguous push batches. A
separate 100-client scenario pops each of 100 elements once. Concurrent hash-pair
readers reject partially observed updates. See [validation](validation-and-limitations.md) for observed outcomes.

tests/differential/test_m04.py has 14 always-on local command traces, 14 corresponding
official-Redis comparisons (including counted pops and wrong types) and two reference
TTL-retention/deletion scenarios. The existing isolated oracle fixture is retained.
An installed Redis >=6.2 is needed for counted pops. Missing software produces
explicit unavailable comparisons; it does not establish acceptance or waive tests.
The 16 M04 reference scenarios and all cross-feature edge cases passed on
2026-11-10; see [validation](validation-and-limitations.md) for results.
