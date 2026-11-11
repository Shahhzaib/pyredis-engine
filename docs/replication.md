# M09/M10 synchronization and recovery between PyRedis peers

E05A transfers versioned stream postimages and exact generated IDs through the same
effect stream and snapshots. Older peers that cannot decode stream kind 6 fail closed;
this does not add protocol negotiation or native Redis replication. Blocked XREAD
requests on a replica wake after its effect/offset advances and fail with MASTERDOWN
on link loss. See [core Streams](streams.md).
E05B group/PEL updates use ordered version-2 stream postimages, including delivery
timestamps and ownership; replicas do not independently re-execute deliveries.
Both peers must understand the new stream version or synchronization fails closed.

Full regression includes replication/recovery tests. Official Redis comparisons cover
client commands, not peer wire interoperability. See [validation](validation-and-limitations.md).

Redis's asynchronous snapshot-plus-stream and read-only concepts inform this
implementation. It is **not compatible with official Redis PSYNC, RDB or replica
wire protocols**; connect PyRedis peers only. Version 1 retains M09's full-sync-only, one-shot behavior. M10's
explicit version-2 recovery mode adds backlog, ACKs and reconnect below. Neither
mode implements WAIT, cascading replication, promotion, Sentinel or election.

## Configure and observe

Both peers require an explicit journal path in an existing private directory and
snapshot mode. Persistence stays opt-in; replication does not invent a disk path.
Use different files and ports on the same development host:

```powershell
python -m pyredis_engine --port 6380 --appendonly .pyredis-data/primary.aof --appendfsync always --snapshots --replication
python -m pyredis_engine --port 6381 --appendonly .pyredis-data/replica.aof --appendfsync always --snapshots --replicaof 127.0.0.1 6380
```

--replicaof implies replication_enabled, but still requires --snapshots and
--appendonly. Only loopback IPs are allowed for listeners and upstreams. The examples
above use legacy trusted-loopback plaintext mode. For protected TLS, AUTH, replica
authorization, certificate pinning and configuration examples, see
[operations and security](operations-and-security.md). Both wire versions share the
same secure transport when enabled. A replica cannot accept downstream full-sync requests.

In embedded use, configure replication_enabled=True on the primary; configure that
plus replica_host/replica_port on the replica. start_server returns with an upstream
task in progress. Await server.replication.wait_ready() to observe initial readiness,
or wait_offset(primary.replication.offset) in deterministic tests. These local APIs
are not network ACKs or a durability promise for primary replies.

INFO replication, outside MULTI, returns a bounded RESP bulk containing the subset:
role (master/slave), master_replid and master_repl_offset or slave_repl_offset.
Primary adds connected_slaves. Replica adds master_host, master_port,
master_link_status and master_sync_in_progress. Other INFO sections and queued INFO
are explicit errors; this is not full Redis INFO compatibility.

Initial database reads return LOADING until an entire snapshot is verified and
installed. A failed/lost upstream returns MASTERDOWN for database reads. PING/QUIT
and INFO replication remain available. Local writes return READONLY, including
write commands queued inside MULTI; such a queue aborts with EXECABORT. Read-only
transactions and WATCH are supported after initial synchronization.

## Snapshot and stream boundary

One persistence commit lock still serializes all commands, whole EXEC batches,
expiration effects and local journal barriers. The primary publishes an immutable
encoded effect only after its configured M07 append/fsync barrier, before replying.
It advances a uint64 byte offset by that frame's size even with no connected peers.
An ID is 40 random lowercase hexadecimal bytes, renewed on every primary process
start. It identifies that process's history. Local journal sequences are separate.

Under the lock, register a bounded peer queue at byte offset B and pin committed
journal sequence S. Under a maintenance lease excluding M08 compaction, reconstruct
S in an isolated worker/executor and write/verify a real M08 snapshot in an owned
temporary directory. Live writes continue and enqueue every following effect. The
lease ends when the file is built; its private copy survives later compaction.

Send the snapshot at S and then the queued effects starting immediately after B.
A whole EXEC is one frame; final postimages/clear/deletion effects are never unsafe
command re-execution. The queued stream remains live after the initial transfer.
Snapshot bytes and heartbeats do not advance mutation offsets. No network I/O is
awaited while holding the commit lock or executing datastore mutations.

## Private protocol version 1

A dedicated peer sends the RESP2 bulk-array command `PYREDIS.SYNC 1`. It is admitted
only on a configured primary, outside MULTI/WATCH, within peer/client caps. Invalid
versions/roles/state receive a bounded error then close. This is not Redis SYNC.

The reply is a simple string with space-separated fields:

```text
PYREDIS-FULLSYNC 1 <40-byte history ID> <B> <store-ID hex> <generation-ID hex> <S> <snapshot byte length>
```

The next exactly declared bytes are a complete M08 checksummed snapshot. All
identities, versions, numbers and lengths are validated before accepting its state.
Then frames consist of big-endian kind byte, uint64 end offset, uint32 payload
length, payload and SHA-256 over header+payload. An effect (`E`) carries one bounded
M07 effect group and advances the offset by 45 + payload length. An idle heartbeat
(`H`) has zero payload and the last sent offset; it does not acknowledge application.
Version 1 rejects unexpected reverse traffic; version 2 accepts bounded ACKs below.

Reject unknown kinds, oversize lengths, schema/checksum errors, duplicate/gapped
offsets and truncated streams. Read TCP incrementally; split frames and multiple
frames per delivery retain order. Snapshot/effect payloads use bounded chunks and
their own existing disk-record limits, not an enlarged RESP bulk limit. Ordinary
RESP2 framing/resource protections remain unchanged.

## Replica installation and durability

Download to an owned temporary file with an absolute receive deadline. Verify every
entry and full checksum in a private executor before preparing a local-identity
snapshot. Load private staged state, then activate its synced/verified snapshot and
empty generation journal through M08's manifest machinery. Preserve the previous
generation until the same safe retirement policy permits cleanup. Synchronously
transfer validated state under the commit lock; no client observes a partial load.

Installation advances the local journal epoch by one; it does not equate local
journal sequence to the primary's byte offset. Startup replays that generation
normally, but configured replicas still require a new full sync before reads.
No durable resumable replication metadata is claimed in M09.

Every later effect is validated against record and aggregate dataset limits, applied
atomically under the same lock, and journaled according to the replica's fsync policy
before its reported applied offset advances. Primary replies remain asynchronous
with respect to replicas. Local persistence failure fail-stops the affected node.

Replicas preserve absolute deadlines and hide expired keys on reads, but do not
delete them independently or journal local expiry. Primary-ordered deletion effects
remove their physical state and invalidate WATCH. A clock rollback cannot resurrect
an authoritative deletion; differing peer clocks can hide a key at different times.

## Failure handling and limits

| ServerConfig field | Default |
|---|---:|
| replication_enabled | False; requires snapshot_enabled/aof_path |
| replica_host / replica_port | None / None; configure together |
| max_replicas | 4 per primary, also subject to max_clients |
| replication_queue_bytes | 64 MiB per peer, including in-flight effect |
| replication_queue_frames | 4,096 per peer, including in-flight effect |
| replication_timeout | 10 seconds; connect, greeting, whole snapshot receive, each stream frame |
| replication_heartbeat_interval | 1 second; strictly less than replication_timeout |

Existing snapshot/key/dataset/record/bulk/collection limits and bounded input/output
streams apply to both peers. There is one snapshot worker per store and a finite
number of admitted peer jobs. Count a session until its owned disk work is drained,
even after network disconnect. Slow-write timeout, queue byte/count exhaustion or
malformed traffic closes only the affected session; never silently discard a gap
and keep streaming. A failed replica serves no stale database reads by default.

Cancellation during resource acquisition explicitly closes descriptors/temp files;
shutdown drains owned journal/snapshot jobs before closing the lease/workers. A
stalled disk can delay shutdown. Active temporary exports/downloads are removed on
normal/error/cancel paths; an actual process kill may leave unselected temp files
for operator inspection. No automatic orphan reaper is introduced.

Windows retains old persistent generations without a portable directory-fsync
guarantee; all M07/M08 fsync/retention and power-loss limitations still apply. Full
sync uses additional bounded datasets and files, not a comprehensive heap/disk cap.
Large snapshots or local installation can exceed configured deadlines and require
operator tuning. Version 1 requires explicit replica restart and a fresh full sync.
See [validation](validation-and-limitations.md) for the tested scope.

## M10 recovery mode

Add **--replication-recovery on both peers** (or replication_recovery=True in
ServerConfig). This implies --replication at the CLI and still requires explicit
snapshot-enabled persistence. Existing version-1 clients and behavior remain
supported; recovery is not silently negotiated with a version-1-only primary.

```powershell
python -m pyredis_engine --port 6380 --appendonly .pyredis-data/primary.aof --appendfsync always --snapshots --replication-recovery
python -m pyredis_engine --port 6381 --appendonly .pyredis-data/replica.aof --appendfsync always --snapshots --replication-recovery --replicaof 127.0.0.1 6380
```

The primary retains whole immutable effect frames in a byte/count-bounded backlog,
even with no connected replica. Each exact end boundary has a rolling SHA-256
history proof. The initial proof hashes the process history ID; each following
proof hashes its predecessor plus the effect frame digest. Evict whole frames;
never retain an unsafe partial transaction. A record larger than the entire backlog
evicts the suffix, but the current end boundary/proof remains known for zero-tail
resume. This proof checks history continuity, not authentication or a full heap hash.

The replica keeps a process-local resume token: history ID, last completely applied
byte offset/proof, plus local journal sequence and store mutation revision. Under
the commit lock the primary checks that ID/offset/proof identifies a retained exact
boundary, queues the following suffix and a catch-up barrier, then subscribes live
effects. There is no race between checking history and joining new writes. If the
suffix exceeds the peer queue capacity, use a fresh full snapshot instead.

Evicted history, wrong ID, invalid/inside-frame/ahead offsets or wrong proofs fall
back to full sync. The replica also requires full replacement after unexpected local
mutation or protocol/schema/checksum/offset/proof divergence. Validate its local
continuity guard again under the effect-apply lock before touching state. Ordinary
EOF, a truncated transport frame or timeout preserves the last complete committed
prefix. An uncertain canceled apply invalidates its local guard and cannot be
silently replayed using an obsolete offset.

Both full and partial recovery remain unavailable for database reads until the
fixed catch-up target and its proof are verified. Writes during snapshot creation,
disconnection, replay and after the target barrier follow in order. Full recovery
continues to use M08's generation activation, preserving old recoverable files.
Compaction cannot change the independent in-memory byte history or retire an
export's pinned source prematurely. Transactions and expiration remain whole effects.

## Version-2 wire extension

Send a RESP bulk-array request:

```text
PYREDIS.SYNC 2 <history-ID or ?> <applied-offset or -1> <proof-hex or ?>
```

Wrong bounded resume values request full fallback; malformed request shapes or
overlong metadata are errors. Full greeting extends the version-1 fields with
version 2, a 64-character boundary-proof hex and a target offset. Partial greeting:

```text
PYREDIS-CONTINUE 2 <history-ID> <requested-offset> <requested-proof-hex> <target-offset>
```

E framing/offset accounting stays unchanged, preserving version-1 stream semantics.
Version-2 H heartbeats contain the 32-byte current proof without advancing offsets.
An R barrier contains the 32-byte target proof and requires exactly the current
applied offset to equal the admitted target. Unknown/duplicate/reordered barriers,
bad proofs, gaps and duplicate effects close the link; never continue after a gap.
Heartbeats alone cannot keep initial catch-up alive beyond its absolute deadline.

Reverse A frames use the same checksummed header, with exactly 73 payload bytes:
40-byte history ID, 32-byte proof and one policy byte (N=none, E=everysec, A=always).
They report the current applied offset. Validate the entire frame before changing
primary progress. ACKs must identify an exact offered boundary/proof, stay within
the offered prefix and never move backward. Duplicate ACKs at the same boundary
are valid liveness reports. Changing policy inside a connection is rejected.

Register a boundary before awaiting its network send so a fast peer's ACK during
the final drain is valid. This is an offered-prefix bound, not proof the peer really
stored data. Bound outstanding ACK boundaries independently of the live queue;
deadline/pending-cap violations disconnect only the offending peer. Sessions retain
their admission slot through owned cleanup. ACKs do not pin the global backlog.

## What an ACK guarantees

The replica sends its ACK only after successful local application and its configured
journal barrier. With always, each effect's file fsync completed; guarantees still
depend on OS/storage honoring fsync and existing Windows namespace limitations.
With everysec or none, the ACK reports application and append to the OS, **not a
durable copy**. Full snapshot installation can sync more than a weak policy requires;
the reported policy remains conservative. ACKs are self-reports from unauthenticated
private peers, never a cryptographic proof of storage. Primary replies do not wait
for any replica ACK. No WAIT, quorum or synchronous durability is introduced.

## Reconnection, restart and bounds

One owned supervisor attempts recovery with capped exponential backoff and a finite
consecutive-failure budget. Reset the budget only after the configured healthy
interval; a briefly successful handshake cannot reset an endless flapping loop.
Stop on exhaustion, local persistence failure or shutdown. Restart the replica to
make a new attempt after exhaustion. Backoff sleep is injectable for deterministic
tests; production uses monotonic asyncio scheduling without blocking the event loop.

Resume tokens/backlog are **not persisted**. Replica process restart conservatively
requires a fresh full snapshot, reconciling recovered local data without assuming
that sidecar metadata and journal commits were atomic. Primary restart generates
a new history ID and also forces full sync. There is no secondary history ID,
election or promotion. Normal link recovery within a running process can resume
without replacing its state. Mutation-version guards do not detect arbitrary memory
edits bypassing storage APIs; persisted corruption is checked by M07/M08 recovery.

| Additional ServerConfig field | Default |
|---|---:|
| replication_recovery | False (explicit version-2 opt-in) |
| replication_backlog_bytes / replication_backlog_frames | 64 MiB / 4,096 |
| replication_retry_attempts | 8 consecutive attempts; allowed 1..1024 |
| replication_retry_delay / replication_retry_max_delay | 0.1 / 5 seconds |
| replication_retry_reset_after | 30 healthy seconds |
| replication_ack_timeout | 30 seconds per ACK frame |
| replication_ack_max_pending | 4,096 unacknowledged boundaries plus one accepted anchor |
| replication_catchup_timeout | 30 seconds for target barrier after snapshot installation/partial greeting |

Retry delays follow min(initial * 2**n, maximum), with no retry after exhaustion.
Transport/handshake/snapshot receive and write deadlines from M09 remain. Disk I/O
is drained safely and cannot be forcibly canceled; storage stalls can delay shutdown.
Existing input/output, peer/client, snapshot/record/dataset and queue limits remain.
They are not total Python heap/disk caps; per-frame hashing and replay can pause
processing, with benchmarking deferred. Queue/ACK exhaustion may require full sync
or operator tuning under sustained overload. Windows retained generations and
process-kill orphan limitations still apply.

INFO replication adds recovery_enabled, recovery_attempts, recovery_full_syncs and
recovery_partial_syncs. Counts are admitted transfers on the primary and completed
barriers on the replica. Primary adds backlog start boundary, first following byte,
retained bytes/frame count, plus per-current-session ACK offset/policy (unknown=-1/?).
ACK offsets from different history IDs cannot be compared. Session-indexed INFO
fields are diagnostic, not stable replica identities. wait_ready/wait_offset on a
recovering replica wait through the target barrier; compare offsets only within
the same history. See [validation](validation-and-limitations.md) for remaining limits.

## M11 interaction

M12 enforces the accounted datastore budget during snapshot restore and effect
application. An over-budget effect stops the replica before store/dataset/journal
changes; no applied offset or ACK advances. Peer queue/history caps remain separate
from maxmemory_bytes. See docs/resources-and-operations.md for policy and exclusions.

Pub/Sub is transient and local to each listener, including read-only or disconnected
replicas. Subscription changes and messages do not enter effect groups/backlog or
advance offsets/proofs. Full synchronization replaces datastore state while preserving
local subscriptions. Datastore commands remain restricted while subscribed, before
INFO/read-only/stale-link handling. A connection activated for Pub/Sub retains its
FIFO writer after unsubscribe and cannot become a private replication transport;
PYREDIS.SYNC requires a fresh dedicated socket. See [Pub/Sub](pubsub.md).
