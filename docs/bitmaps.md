# Bitmap and bitfield commands

E06A implements GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP AND/OR/XOR/NOT,
BITFIELD and BITFIELD_RO over ordinary binary string values. Bit zero is the
most significant bit of the first byte. BITCOUNT and BITPOS accept the documented
inclusive BYTE and BIT ranges; BITPOS may find an implicit zero after the stored
string when its end is omitted. BITFIELD supports signed i1–i64 and unsigned
u1–u63, GET/SET/INCRBY, absolute or `#` multiplied offsets and WRAP/SAT/FAIL.
BITFIELD_RO permits GET only.

SETBIT and successful BITFIELD writes preserve a key's TTL. BITOP replaces the
destination and clears its TTL; overlapping sources are read before publication.
Every source and destination is checked against the existing ACL key patterns.
The ACL subset has one key-pattern permission and does not implement separate
read-versus-write key patterns. Transactions use the existing EXEC savepoint,
and journal, snapshot and replication encode string postimages and absolute TTLs.

The configured `max_bulk_bytes`, `maxmemory_bytes`, reply/argument limits and
persistence record/dataset limits apply. Offsets are checked before extension;
provisional changes are published only after size, budget and reply checks.
BITFIELD prepares one private mutable copy and commits one immutable string.
SETBIT and BITFIELD can copy O(N) bytes; counting/searching and each BITOP source
take O(N) time, with bounded transient copies. BITOP rejects requests where the
longest source length multiplied by source count exceeds `max_collection_bytes`;
this limits serialized work and is a PyRedis-specific restriction. Datastore accounting is not an RSS
or Python heap ceiling. Large operations still run on the serialized executor.

**Intentional Redis 7.4.12 difference:** on an `OVERFLOW FAIL` write beyond the
end, Redis may create or extend a zero-filled string even though the field write
fails. PyRedis extends strings only for successful writes,
so PyRedis leaves the field and string length unchanged. Focused reference tests
assert both outcomes. Additional command edge cases, directional ACLs and broad
release validation remain open; do not claim complete Redis compatibility.

Selected redis-py 7.4.1 sync and asyncio workflows pass with RESP2 and the existing
partial RESP3 support. See [validation](validation-and-limitations.md) for the tested scope.
