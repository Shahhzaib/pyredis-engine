# Command compatibility matrix

The registry contains **109 command names**. A listed name does not imply every Redis
option, error string or RESP3 response is supported. Unlisted commands are unsupported.
See the linked references for supported forms and documented differences.

| Family | Registered commands | Scope and limitations |
|---|---|---|
| Connection | `PING`, `ECHO`, `QUIT`, `HELLO`, `CLIENT`, `SELECT` | [Protocol](protocol.md), [clients](client-compatibility.md); RESP3 subset, SELECT 0 |
| Security | `AUTH`, `ACL` | [Authentication and ACL subset](operations-and-security.md); TLS optional |
| Keys and strings | `GET`, `SET`, `GETDEL`, `GETEX`, `GETRANGE`, `SETNX`, `DEL`, `EXISTS`, `TYPE`, `DBSIZE`, `FLUSHDB`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MGET`, `MSET` | [Strings](storage-and-strings.md), [expiration](expiration-and-clock.md); options are limited |
| Expiration | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST` | [Expiration](expiration-and-clock.md); no EXPIRE-family conditions |
| Hashes | `HSET`, `HSETNX`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HGETALL`, `HLEN`, `HSTRLEN`, `HKEYS`, `HVALS` | [Hashes and lists](hashes-and-lists.md) |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `BLPOP`, `BRPOP` | [Lists](hashes-and-lists.md), [blocking operations](blocking-lists.md) |
| Sets | `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SCARD` | [Sets](sets-and-sorted-sets.md) |
| Sorted sets | `ZADD`, `ZREM`, `ZSCORE`, `ZRANGE`, `ZCARD`, `ZMSCORE`, `ZRANK` | [Sorted sets](sets-and-sorted-sets.md); selected options |
| Iteration | `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` | [Iteration](scan-iteration.md); cursor and mutation limits |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` | [Transactions](transactions.md); bounded reply and queue limits |
| Messaging | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE`, `PUNSUBSCRIBE` | [Pub/Sub](pubsub.md); transient, node-local messages |
| Streams | `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`, `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM` | [Streams](streams.md); consumer groups and options are partial |
| Bitmaps | `GETBIT`, `SETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD`, `BITFIELD_RO` | [Bitmaps](bitmaps.md); documented failed-overflow difference |
| HyperLogLog | `PFADD`, `PFCOUNT`, `PFMERGE` | [HyperLogLog](probabilistic-and-geographic.md); private representation and estimator |
| Geographic | `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`, `GEOSEARCH` | [Geographic](probabilistic-and-geographic.md); exact dateline search differs |

Persistence, snapshots and replication are configuration/API capabilities, not
native Redis disk or replication protocols. PYREDIS.SYNC and INFO replication are
private connection paths; see [replication](replication.md).

Strict Redis 7.4.12 differential testing covers selected forms, not full Redis parity.
See [validation and limitations](validation-and-limitations.md) for evidence.
