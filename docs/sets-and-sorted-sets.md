# M05 sets and sorted sets

Selected set and sorted-set forms have passed local tests and Redis 7.4.12 comparisons;
compatibility applies only to the listed subset.

## Command forms

| Form | Local contract | Official reference |
|---|---|---|
| `SADD key member [member ...]` | Count distinct newly added members; ignore duplicates; missing creates a set | [SADD](https://redis.io/docs/latest/commands/sadd/) |
| `SREM key member [member ...]` | Count removed members once; delete key after its final member | [SREM](https://redis.io/docs/latest/commands/srem/) |
| `SISMEMBER key member` | Integer 1 for present, otherwise 0 | [SISMEMBER](https://redis.io/docs/latest/commands/sismember/) |
| `SMEMBERS key` | All members as bulk strings; missing is an empty array | [SMEMBERS](https://redis.io/docs/latest/commands/smembers/) |
| `SCARD key` | Cardinality; missing is zero | [SCARD](https://redis.io/docs/latest/commands/scard/) |
| `ZADD key score member [score member ...]` | Count newly added members; existing scores update without increasing the count; final duplicate wins | [ZADD](https://redis.io/docs/latest/commands/zadd/) |
| `ZREM key member [member ...]` | Count removed members once; delete key after its final member | [ZREM](https://redis.io/docs/latest/commands/zrem/) |
| `ZSCORE key member` | RESP2 score bulk; missing key/member is null | [ZSCORE](https://redis.io/docs/latest/commands/zscore/) |
| `ZRANGE key start stop [WITHSCORES]` | Inclusive ranks in ascending score order; ties compare raw member bytes; missing is empty | [ZRANGE](https://redis.io/docs/latest/commands/zrange/) |
| `ZCARD key` | Cardinality; missing is zero | [ZCARD](https://redis.io/docs/latest/commands/zcard/) |

Keys/members are raw bytes, including empty, NUL, CRLF and non-UTF-8 data. TYPE returns
`set`/`zset`. SMEMBERS sorts bytes locally for reproducibility; Redis promises no set
enumeration order. Only that reply compares independent of order, with complete
unique membership checked. ZRANGE order is exact, including prefix/binary tie-breaks.
WITHSCORES returns a flat RESP2 array alternating member and score bulks.

Ranks are canonical signed-64-bit integers; negative indexes count from the end.
Clamp bounds to the stored length before iteration. Missing, reversed or entirely
out-of-range intervals are empty. Invalid ranks use the existing integer error;
invalid arity/unmatched ZADD pairs use the command-specific registry arity error.
All ten commands reject other stored kinds with the existing WRONGTYPE error.
SET/MSET can still overwrite collections; MGET returns null for non-strings.

WITHSCORES is case-insensitive. ZADD NX/XX/GT/LT/CH/INCR and ZRANGE REV/BYSCORE/BYLEX/
LIMIT are outside this subset and fail explicitly. Unlisted commands, GEO,
and other unlisted forms remain unsupported. M06 adds transactions and WATCH.

## Scores and precision

Scores are derived binary64 numbers; member payloads stay bytes. The official
[ZADD contract](https://redis.io/docs/latest/commands/zadd/) specifies double
precision and permits signed infinity. PyRedis accepts ASCII decimal/scientific
forms, leading plus/zeroes, `.5` and `1.`. Strict C-style hexadecimal forms with a
`0x` prefix and optional binary exponent are also accepted: `0x1p2` is 4, `0x.8`
is 0.5. Explicit case-insensitive `inf`/`infinity` with optional sign are accepted;
negative zero becomes zero. No locale or UTF-8 decoding participates. NaN, numeric
overflow to infinity, nonzero underflow to zero, whitespace, NUL, underscores,
malformed decimal/hexadecimal syntax and invalid bytes return
`ERR value is not a valid float`, an error category in the official
[Redis error reference](https://redis.io/docs/latest/develop/reference/errors/all/redis/).
All pairs are parsed before writes, even invalid pairs later replaced by duplicates.

RESP2 score rendering follows measured Redis 7.4.12 strings: `0.1` stays `0.1`,
the smallest positive subnormal is `5e-324`, zero is `0`, and infinities are
`inf`/`-inf`. Exponents have no zero padding (`1e-7`). An independent bounded
decimal-interval search preserves Redis's conservative rounding and fixed/scientific
layout, including `1e23` becoming `99999999999999990000000`; plain Python repr is
insufficient. The numeric method is covered by independent reference probes.
Exact integral scores of magnitude at most 2**62 use integer spelling. Other
integers may round (9007199254740993 becomes 9007199254740992). M05 adds no
floating-point arithmetic command. These rules target Redis 7.4.12 RESP2, not a
claim of exhaustive numeric parity with every Redis version/platform.

## Ownership, expiration and limits

SetValue owns a bytes set. SortedSetValue owns a member-to-score dictionary and a
sorted (score, member) list with exactly one entry per member. Score updates remove
the old entry before insertion, so no stale-entry accumulation occurs. Values are
internal mutable objects confined to synchronous executor handlers. Validate all
arguments and projected growth before mutating either index; failures preserve live
contents, TTL and revision. No new task or execution path is introduced.

Changed membership/scores use Store.put(keep_ttl=True), retaining absolute expiry
and notifying revision hooks. Duplicate SADD, identical-score ZADD, missing removals
and reads do not touch revisions. Intermediate duplicate-pair score changes still
trigger a revision if the final score returns to the original. Final-member removal
uses Store.delete, cleaning expiry/revision metadata. Passive/active expiration,
strict deadline boundaries, per-command time snapshots and shutdown are unchanged.
Expired keys may be recreated with any kind and no inherited TTL.

Existing limits apply without increases:

- max_collection_items: 100,000 distinct members per collection.
- max_collection_bytes: 16 MiB per collection; sum of member bytes for sets,
  sum of member bytes plus eight bytes per score for sorted sets.
- max_bulk_bytes: members and incoming score arguments remain bounded.
- max_reply_bytes: SMEMBERS/ZRANGE accumulate RESP wire size while constructing
  replies, including score bulks, and stop on overflow without changing state.

Growth/element/reply errors reuse M04's exact errors. Duplicates consume no extra
capacity; removals release capacity. Python object/index/key overhead is excluded;
total database maxmemory remains M12. All protocol/client/buffer limits are retained.

Set membership uses expected O(1) lookups; sorted local SMEMBERS is O(N log N).
Sorted-set updates use bisect plus O(N) list shifts per changed member, unlike
Redis's logarithmic updates. Rank reads access the ordered list directly and visit
only selected entries. Index storage is O(N). Large mutations/deletions can hold
the event loop; active-expiry budgets count keys, not elements. No benchmark is claimed.
Score formatting uses bounded-size Fraction arithmetic and 87 computed scaling
powers cached once. It creates no per-score cache, task or datastore mutation.
Its cost can exceed native floating-point formatting; throughput is unbenchmarked.

## Acceptance evidence

Unit tests cover every command, score boundaries, wrong types across five kinds,
duplicates, rank extremes, limits, expiry/revisions and last-member cleanup. Two
deterministic Hypothesis models verify generated membership/update/removal traces,
byte accounting and ranked-index invariants. TCP tests exercise every fragment
split, truncation, binary pipelines, bounded errors/reconnects, expiry and shutdown.

Barrier-driven concurrency tests use 100 clients adding 20 unique members plus one
shared member per kind (2,001 total), 100 removing shared members exactly once, and
100 recreating expired keys without inherited TTL. Score-pair readers must never
observe partial updates. Active expiry uses events instead of TTL sleeps.

tests/differential/test_m05.py has 14 always-on local traces, 14 corresponding Redis
comparisons and two TTL/final-deletion comparisons. Missing Redis explicitly skips
external checks. All 16 M05 comparisons plus the cross-feature edge cases passed
against Redis 7.4.12 on 2026-11-10. See [validation](validation-and-limitations.md)
for the scope and remaining limitations.
