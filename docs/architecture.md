# Architecture

PyRedis is a single-process, byte-oriented server. Compatibility is limited to the
[documented command subset](command-matrix.md); replication and disk formats are
private to PyRedis. The server is not production-certified.

```text
RESP clients -> bounded TCP parser -> command registry -> ordered executor
                                                  |          |
                                             connection    typed store + expiry
                                               state          |
                                           Pub/Sub       effect journal
                                                              |
                                                   snapshots / replica stream
```

`server.py` owns connection lifecycle, timeouts, output and client tasks. `protocol.py`
implements bounded incremental RESP2 framing; `resp3.py` handles the supported RESP3
reply types. Connections start in RESP2 and may negotiate RESP3 independently. Keys,
values and command arguments remain bytes.

`commands/registry.py` validates command metadata and routes operations. The single
synchronous `engine/executor.py` owns the store and serializes mutations; it does not
await network I/O while executing them. `engine/transactions.py` owns per-client
MULTI/EXEC/WATCH state. EXEC stages effects and admits the complete bounded reply
before publishing a mutation. A connection loss after commitment can still leave
its client uncertain about the result.

`engine/store.py`, `collections.py` and `expiration.py` manage typed values,
absolute expiration deadlines and revisions. A clock can be injected for tests.
Collection and scan bounds and their limitations are described in
[iteration](scan-iteration.md) and [resource management](resources-and-operations.md).

`persistence/` writes ordered effects and checksummed snapshot generations under
explicit fsync policies. Recovery rejects invalid records; the exact durability and
Windows limitations are in [persistence](persistence.md). `replication/` transfers
private PyRedis snapshots and ordered effects between PyRedis peers, not Redis PSYNC.
Its resynchronization, authentication and trust boundaries are in
[replication](replication.md). Pub/Sub is transient, node-local and bounded; see
[Pub/Sub](pubsub.md). Security configuration and optional TLS are described in the
[security guide](operations-and-security.md).

The configured datastore memory budget is accounting-based, not an operating-system
or Python RSS ceiling. See [performance](performance.md) for measured workloads.
