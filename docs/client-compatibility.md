# Client compatibility

PyRedis implements the command forms in [the command matrix](command-matrix.md),
not the full Redis API. It is loopback-only and is not production certified. Its
private replication protocol is for PyRedis peers only; client compatibility does
not imply Redis PSYNC/RDB interoperability.

The tested redis-py 7.4.1 synchronous and asyncio clients use RESP2, username and
password authentication, binary keys/values, health checks, pipelines, transactions,
WATCH, Pub/Sub and pool reconnection. The synchronous test also verifies CA- and
hostname-validated TLS. Tests cover selected workflows, not all versions, APIs or
frameworks. In isolated Docker, Redis 7.4.12 `redis-cli` was exercised against
PyRedis for PING, SET, GET and SELECT 0. The host's optional native redis-cli test
was unavailable on the Windows test host. See [validation](validation-and-limitations.md).

Use `redis.Redis(..., protocol=2)` or `redis.asyncio.Redis(..., protocol=2)` with
the loopback address and configured credentials; set the redis-py TLS options and
trust a suitable CA when the server has TLS enabled. Do not put passwords in shell
history, source control or log output. See [security operations](operations-and-security.md)
for private bootstrap and TLS provisioning.

Selected redis-py 7.4.1 sync and asyncio workflows have also been tested with
`protocol=3`, including authentication, binary data, hashes, sets, transactions,
Streams and Pub/Sub; the sync client was tested with verified TLS. This is partial
RESP3 coverage, not a claim that all supported commands or client APIs match Redis.

Only database 0 exists. HELLO 2 returns a RESP2 array and HELLO 3 returns a RESP3
map; HELLO AUTH and SETNAME are supported. Nonzero SELECT and most CLIENT
administration commands are unsupported.
CLIENT SETNAME/GETNAME/ID and SETINFO LIB-NAME/LIB-VER are supported, with
bounded metadata; CLIENT inside MULTI is explicitly rejected. Resource ceilings,
ACL grants and command-subset restrictions continue to apply to real clients.

`scan_iter`, `hscan_iter`, `sscan_iter` and `zscan_iter` have a bounded subset:
cursors are tied to one connection and invalidated by relevant mutations; use a
dedicated connection and restart after an invalid-cursor error. They cannot promise
Redis's mutation-tolerant iteration guarantees. Other omissions are listed in the
[compatibility limitations](validation-and-limitations.md). Network loss after a committed
write can still leave a client uncertain whether retrying is safe.
