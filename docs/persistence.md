# Append-only effects and M08 snapshot generations

E05A extends typed postimages with versioned stream kind 6, including ordered entries
and last-generated ID even after all entries are deleted. New readers retain support
for prior types/generations; unknown stream versions fail closed. See
[core Streams](streams.md). Existing framing and fsync policies apply.
E05B's version 2 additionally records group, consumer and pending-entry state in
the same atomic postimage; older versions remain readable, while older binaries
that do not understand version 2 must not be used to open such data.

M12 enforces the accounted datastore budget during replay, compaction staging and
replica installation. Lower incompatible limits refuse recovery without dropping
valid records; over-budget replica effects fail closed before account/journal/store
changes. See [resources and operations](resources-and-operations.md). Allocation
failure outside private preparation stops service. Existing fsync guarantees remain;
shutdown client deadlines cannot forcibly cancel uninterruptible disk work.

Persistence is opt-in. Use a **new filename in an existing private directory**:

```powershell
python -m pyredis_engine --port 6380 --appendonly .pyredis-data/engine.aof --appendfsync always
```

Create the directory yourself before starting. Missing directories, access errors,
nonregular files and a competing writer fail startup. An existing zero-length file
is treated as truncated data, not silently initialized. With no --appendonly path,
the server remains in-memory. No automatic system/software installation occurs.
This custom format is not native Redis AOF/RDB. Snapshot generations and journal
compaction require explicit M08 opt-in below. M09 provisionally adds private peer
replication with that persistence enabled; backup management remains unsupported.

## Ownership and effects

Persistence.open streams validated records into a private executor before TCP binds.
Only the event-loop-owned executor restores or mutates the live store. One dedicated
file thread owns ordered open/read/append/fsync/close calls; no worker accesses live
values. An async commit lock covers synchronous execution and its following append/
sync barrier. Network and active expiration share that path. Handlers never await
I/O; unrelated asyncio tasks can run while the file worker waits.

One command, whole EXEC or active-expiry batch yields one effect group. It contains
an optional clear and final typed postimages/deletions for changed keys. Repeated
changes to one key within an indivisible group coalesce, retaining exactly the final
state. Live WATCH/revision notifications remain per observable write. Reads/control
commands do not journal unless they passively expire a key; FLUSHDB records a clear.
Runtime errors inside EXEC preserve successful siblings in the same atomic group.
Queue errors, conflicts, DISCARD and disconnect never persist unexecuted queues.

The encoder freezes bytes before yielding to the worker. Keys/values/collection
members remain binary-safe. Hash fields and set/zset members encode in byte order;
list order is retained and sorted indexes rebuild from exact binary64 scores.
Hash/set enumeration order is not a cross-restart Redis promise. NaN is forbidden.
Expiration is an absolute signed-64-bit Unix millisecond deadline, never a relative
TTL replay. Loading does not expire records midway through history; normal passive/
active expiry resumes at the injected current clock after replay. Expiration itself
is journaled, so a later clock rollback cannot resurrect a committed deletion.
Process-local WATCH registrations/revision numbers are not persisted.

## Version 1 framing

All integers use big-endian encoding; byte payloads are never decoded as text.

| Field | Encoding |
|---|---|
| File magic/version | 12 bytes: `PYREDAOF` + `00 01 0d 0a` |
| Record payload length | uint32; validated before allocating/reading payload |
| Group sequence | uint64, strictly previous + 1 across restarts |
| Header CRC32 | uint32 over the preceding 12 header bytes |
| Payload | clear flag byte, uint32 entry count, unique keyed entries |
| Record digest | SHA-256 over the 16-byte record header plus payload |
| Entry | uint32 key length, key bytes, type byte, expiry flag, int64 deadline |
| Type payload | string blob, or uint32 count followed by typed blobs/scores |

Type tags: 0 deletion, 1 string, 2 hash, 3 list, 4 set, 5 sorted set. Blobs are
uint32 byte length plus bytes. Hashes contain field/value blobs; lists/sets contain
member blobs; sorted sets contain member blobs and IEEE-754 binary64 scores. Every
entry carries the fixed deadline field; a zero flag requires deadline zero and a
deletion cannot carry expiry. Unknown tags/versions, duplicate keys/members/fields,
NaN, trailing payload bytes, bad sequences/checksums and invalid sizes fail closed.
The header CRC prevents length-bit damage from being mistaken for an incomplete
tail. Checksums detect accidental corruption; they do not authenticate hostile edits.

## Durability and failures

| appendfsync | Successful write acknowledgement |
|---|---|
| none | Complete append to the OS; no explicit file fsync, including close |
| everysec (default) | Complete append; worker syncs on a monotonic interval and graceful close |
| always | Complete append and successful file fsync before the reply |

everysec normally risks roughly one interval of writes, but scheduler/storage
delays can extend it. none has no explicit power-loss durability bound. Process kill
does not clear kernel caches and is not a power-failure simulation. fsync guarantees
depend on OS/filesystem/hardware honoring it. New POSIX filenames also sync the
parent directory; Python's stdlib provides no portable Windows directory-fsync
guarantee. Recovered prefixes are synced before serving in always mode. File locks
are released on close/process exit.

Append/sync failures log the error, mark persistence broken, and close the listener
and clients. No later mutation/success is accepted. A complete unacknowledged record
may recover; clients must treat lost replies as uncertain execution. A torn group
never replays partially. Earlier none/everysec acknowledgements retain their weaker
policy. Graceful final-sync failures propagate. Unexpected command/worker bugs also
propagate with original tracebacks; finally cleanup still releases the file lock
and worker. Cancellation, including repeated cancellation, drains owned commits
before file close; no background write is orphaned.
Kernel I/O cannot safely be canceled, so a stalled disk may delay shutdown. This
does not introduce an acknowledgement timeout that falsely claims durable success.

## Recovery and explicit repair

Ordinary startup refuses incomplete tails and all corruption, preserving the file.
After inspecting/backing up the file, an operator may explicitly request:

```powershell
python -m pyredis_engine --appendonly .pyredis-data/engine.aof --aof-repair-tail
```

Only incomplete final records are truncated at the last verified boundary; the
truncation is synced and logged. Complete checksum/schema/header corruption and
unknown file versions are never salvaged automatically, including corruption before
valid later records. A truncated file header (even zero bytes) is not tail-repairable.
Repair can remove acknowledged none/everysec tail effects after a storage crash;
it never implies rollback of earlier valid records. Tests use isolated temporary files.

## Configuration and bounds

| ServerConfig option | Default |
|---|---:|
| aof_path | None (disabled) |
| aof_fsync | everysec |
| aof_repair_tail | False |
| aof_sync_interval | 1.0 seconds |
| aof_max_record_bytes | 64 MiB payload, plus 48 framing bytes |
| aof_max_dataset_bytes | 256 MiB of encoded current state |
| aof_max_keys | 1,000,000 physical keys before pending expiry cleanup |

Existing bulk/collection/frame/client/transaction/reply limits remain. Count and
payload bounds precede untrusted replay allocations. Historical committed dataset
peaks must fit the configured dataset/key caps; lowering limits can require restoring
the earlier values to load an intact log. Startup validates the entire history before
binding and does not silently drop valid records that exceed limits.

Live conservative growth preflight rejects oversized groups before mutation. Plain
SET/MSET replacements use exact postimage sizes, so shrinking data does not copy/
reserve old values. Other near-cap operations may be conservatively rejected. EXEC
capacity rejection clears its queue/watches with EXECABORT and no queued effects.
Active expiry also budgets exact deletion payload bytes, splitting large batches.
At most one bounded journal record is submitted; waiting requests remain bounded
by existing client/input limits. These encoded-state caps are not Python heap or
M12 maxmemory accounting. The log grows until operational disk capacity is reached;
no rewrite, pruning or automatic data deletion is provided without M08 opt-in.

## M08 operation and ownership

Use the existing private directory and opt in explicitly:

```powershell
python -m pyredis_engine --appendonly .pyredis-data/engine.aof --appendfsync always --snapshots
python -m pyredis_engine --appendonly .pyredis-data/engine.aof --appendfsync always --compact-on-start
```

--compact-on-start implies --snapshots and finishes one compaction before binding.
Embedded callers can await `server.persistence.compact()` during active traffic.
The result reports activation, pinned/cutover sequences, deferred cleanup and any
maintenance error. There is no automatic scheduler or new SAVE/BGSAVE RESP command.
Only one compaction may run at a time. Snapshot mode becomes required to reopen
the store, even if its first snapshot has not yet activated. Do not downgrade to a
pre-M08 binary after opting in. Keep the root, lease and generation files together.

The existing serialized commit lock pins a synced sequence S and byte boundary.
A separate worker reconstructs that committed prefix in a private executor using
the same bounded typed effects. It never reads live collections. Commands, whole
EXEC groups and active/passive expiration continue committing to the source journal.
The worker streams physical key postimages in byte order with absolute deadlines;
it does not expire keys while reconstructing history. WATCH registrations/revisions
remain process-local; maintenance itself does not invalidate watches.

At cutover the lock excludes all mutations, syncs through T, and copies precisely
groups S+1..T into a new journal. Snapshot and tail are verified before publication.
Restart loads the snapshot at S then groups S+1 onward, once per group; complete
EXEC effects retain their atomic boundary. No mutable state crosses worker threads.
File I/O runs off the event loop. Cutover pauses command commits for bounded tail
copy and snapshot verification; slow storage can still delay commits/shutdown.

## M08 file formats and activation

All numbers are big-endian. Snapshots start with 12-byte `PYREDSNP` + `00 01 0d 0a`,
16-byte store ID, 16-byte generation ID, uint64 S and uint64 key count. One v1 AOF
frame per key contains exactly one nondeleted typed postimage; its frame sequence
is the 1-based key index. Keys must be strictly increasing. A final SHA-256 covers
the complete header and frames; extra bytes, truncated data, bad checksums, unknown
versions/identities, duplicate keys and out-of-bound sizes fail validation.

Generation journals use 12-byte `PYREDAOF` + `00 02 0d 0a`, store/generation IDs,
uint64 base S and a SHA-256 header digest; record framing/effects remain M07's.
The root manifest starts `PYREDMAN` + `00 01 0d 0a`, store ID and uint32 descriptor
count. Each descriptor is kind byte (legacy=0, snapshot=1), 16-byte generation ID,
uint64 base and uint64 minimum complete sequence. A SHA-256 footer covers the
manifest. The first descriptor selects current files; subsequent descriptors track
retained generations. Counts, uniqueness, ordering and bounds are checked. A
checksummed sibling .lock stores the store ID and holds an exclusive OS lock.
Generated names derive only from fixed-length IDs within the configured directory;
the manifest cannot supply arbitrary paths.

Publish unique, exclusively created candidates: write, fsync, verify, atomic rename,
then sync their directory on POSIX. Before replacing a legacy root, hard-link and
sync a unique old-journal backup. Fully write/sync/verify a manifest candidate, then
atomically replace the configured root with it and sync/verify publication. The
stable lease excludes competing M08 writers throughout, including Windows's required
close of the legacy root just before replacement. Unsupported hard links/atomic
replacement cause explicit maintenance failure; no unsafe copy/delete fallback.

No old generation is removed before successful activation and directory sync. Keep
the immediate predecessor; on POSIX delete only older manifest-owned regular files.
Unlisted/orphan files are ignored and never glob-deleted. Windows uses MoveFileExW
WRITE_THROUGH publication but has no portable directory-fsync guarantee here:
**defer all destructive cleanup**, retaining old files/history. This preserves the
last known recoverable files without claiming portable power-loss durability for
Windows namespace changes. Cleanup errors report retained garbage while the valid
new generation remains writable. Disk space still requires operator monitoring.

## M08 recovery and failures

Only the root selects recovery; never guess the newest orphan or silently roll
back to a stale predecessor. Missing/corrupt/incompatible selected snapshot/journal,
manifest or identity fails startup before TCP binds. Missing root with an existing
lease, or missing lease for a manifest, also fails closed. Unselected partial temp
files do not prevent intact recovery. The manifest's minimum sequence T prevents
--aof-repair-tail from discarding the fully synced cutover prefix; only later
incomplete tails remain eligible for the existing explicit M07 repair policy.

Creation/write/sync/checksum/rename failures before activation leave the original
writer usable. Source corruption or source sync failure is fatal. From attempted
activation onward, any ambiguous failure stops serving; restart resolves the atomic
root selection. A completed unacknowledged operation can still recover under the
M07 uncertainty contract. Cancellation drains owned work; shutdown aborts publication
if still building, then closes files, the lease and both worker threads.

| Additional ServerConfig option | Default |
|---|---:|
| snapshot_enabled | False; requires aof_path |
| compact_on_start | False; requires snapshot_enabled |
| snapshot_max_bytes | 512 MiB per complete snapshot |
| snapshot_max_tail_bytes | 64 MiB copied at cutover |
| snapshot_max_generations | 64 descriptors; allowed range 2..1024 |

Existing key/dataset/record/bulk/collection limits apply during reconstruction and
recovery. A snapshot worker needs an additional bounded encoded dataset plus Python
object overhead; these are not comprehensive heap limits. Exceeding snapshot/tail/
retention limits aborts maintenance without weakening later command limits or
destroying the source. Windows retained history eventually reaches its configured
cap and blocks further compaction while the active journal can continue growing.
Unselected candidates from interrupted attempts are retained for operator inspection;
there is no automatic orphan reaper or universal disk/latency cap.

EXEC reply admission prevents reply overflow from publishing effects: rejected
batches publish no queued effects to Capture, journal, snapshot or replication.
Admitted batches retain the existing group/commit barrier; disk failures or lost
post-commit replies still leave possible unacknowledged effects. See [transactions](transactions.md).
M08 alone introduces neither replication nor Redis RDB compatibility.

## M09 replication integration (provisional)

An optional post-commit observer publishes immutable effects only after the configured
append/fsync barrier. Snapshot export subscribes and pins at one commit boundary,
then holds a maintenance lease excluding compaction while reconstructing the prefix.
Normal writes continue. Exported files are private copies, so subsequent M08 cleanup
cannot invalidate an in-flight transfer.

Replica installation verifies and remaps an incoming snapshot to its local store ID
and a new local sequence epoch. M08 activation can publish this complete replacement
with an empty journal; its predecessor records the old source sequence. This differs
from compaction, which carries the exact S+1..T tail. Both paths sync/verify candidates
and retain the last recoverable generation through manifest activation. Staged live
state transfers synchronously after publication; no partially received state is served.

Subsequent replicated groups validate aggregate capacity before mutation and use the
same serialized append/fsync path before reporting the new applied offset. Local
expiry deletion is disabled on replicas: reads hide expired values, while ordered
primary effects perform physical removal. Primary replication offsets are byte
positions, not local journal sequences. No replication resumption metadata is durable
in M09; a configured replica requires full sync again after restart.

See [replication](replication.md) and [validation](validation-and-limitations.md)
for behavior, platform limits and evidence.

## M10 recovery integration (provisional)

Version-2 backlog/proofs are independent of local journal sequences and survive
M08 compaction only in process memory. A resume token includes the replica's local
journal sequence and store mutation revision; effect application rechecks them
under the commit lock before modifying state or capacity accounting. An unexpected
local mutation forces full reconciliation. Protocol divergence never silently skips
a record. A normal truncated connection keeps only its completely applied prefix.

Resume metadata is not persisted independently of journal commits. Replica restart
therefore requires a full snapshot even if its recovered journal is intact. Primary
restart changes the history ID and also forces full sync. Full replacement continues
to use the existing generation/fsync/manifest retention strategy. No persistence
format, repair policy or previous acceptance criterion is relaxed.

ACKs follow the actual application and configured local append/fsync barrier. With
always, per-effect file fsync has completed subject to OS/storage guarantees. With
none/everysec, an applied ACK is not durable storage proof. ACKs are self-reports,
not authenticated evidence, and primary replies do not wait for them.
