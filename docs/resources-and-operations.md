# Resource management and operations

PyRedis provides a no-eviction datastore budget, bounded connection admission,
operational counters, validated configuration and signal-driven shutdown. These
do not constitute production certification or an exact process-memory guarantee.

## Accounted datastore budget

ServerConfig.maxmemory_bytes defaults to 268435456 (256 MiB). It must be a positive
integer; zero does not disable the limit. CLI: `--maxmemory BYTES`. No eviction or
unlimited mode is implemented. Accounting is deterministic policy units, not
sys.getsizeof, tracemalloc, Python heap use or RSS. Fixed allowances are not measured
object sizes. Only supported datastore values participate in this model.

| Component | Charge |
|---|---:|
| Each physical key | 192 + key byte length |
| String value | Payload byte length |
| Hash | Sum of field/value byte lengths + 72 per field (including scan index) |
| List | Sum of item byte lengths + 32 per item |
| Set | Sum of member byte lengths + 72 per member (including scan index) |
| Sorted set | Sum of member byte lengths + 8 score bytes + 128 per member |

The key allowance covers entry, revision and expiration indexes regardless of TTL.
Empty keys/values/members still incur metadata charges. Expired physical entries
remain charged until passive/active removal; no unbounded admission-time sweep.
Delete, expiry, clear and replacement release charges. No live-key eviction occurs.

Project growth before changing live state. Over-budget mutations return a RESP OOM
error, preserving live contents, TTL, revisions and journal/replication state
(ordinary passive expiration may still delete expired keys). MSET checks its final
unique-key state as a batch, crediting replaced values before any write. EXEC keeps
runtime-error/no-rollback semantics: each command checks its budget; successful
earlier/later commands may commit. Existing persistence/reply limits still apply.

Collections prepare private copies. Allocation failure there returns OOM with the
original object untouched. Unexpected MemoryError elsewhere stops service and
closes clients; persistence does not commit uncertain effects. Recovery retains
previously acknowledged state according to fsync policy. This does not promise
rollback or continued operation after arbitrary allocator failure, nor resilience
to OS termination or physical memory exhaustion.

Restore and replica installation use the same budget. Incompatible recovery limits
refuse startup without deleting valid records; raise the budget or prepare a
compatible snapshot first. Rejecting an over-budget replicated effect stops the
replica before store, dataset accounting, journal sequence or applied offset changes.
Restart with suitable limits rather than silently dropping effects.

The cap applies to committed physical state. Private collection copies, recovery/
snapshot staging, batch preparation and intermediate replacements may retain old
and new data simultaneously. Existing request/record/collection/snapshot caps bound
that work separately. Private executors each enforce the budget. Copy preparation
adds O(collection size) work even to previously constant-time mutations. Hash/set
index changes copy O(N) references and sort added members; recovery rebuilds the
entire index in O(N log N) worst-case work.

Client readers/arguments, transaction/WATCH queues, responses, Pub/Sub queues,
replication backlog, temporary objects, workers, interpreter/allocator overhead,
transport/kernel buffers and filesystem caches are outside this datastore budget.
They retain their separate existing caps; their aggregate can exceed maxmemory_bytes.
Tune limits together. There is no enforced/measured total Python process-memory ceiling.
A process-local LRU is capped at 4,096 formatted score results, outside
datastore accounting. Its benchmark report compares actual RSS against charged
state; measured working set can exceed the configured datastore budget.

## Connection and queue protections

Reserve max_clients slots before scheduling normal handlers. At most
min(max_clients, 16) additional bounded rejection handlers send a max-client error;
pause their input. Further arrivals are aborted without another handler. Release
reservations on cancellation, including before a handler starts.

Existing read/frame/write deadlines and chunked reply drains remain. Pub/Sub caps
queued/in-flight bytes/frames and disconnects slow subscribers. Replication retains
its peer/backlog/retry/ACK caps. Request/bulk/header/depth/input, collection/item/
reply, transaction count/payload and WATCH caps remain independent. No configured
cap is increased. Subscribed idle wait retains M11 partial-frame deadlines.
Core numeric resource fields reject bool/nonintegral/invalid types; deadlines must
be finite and positive. Bind/upstream endpoints remain loopback-only. No auth claim.

## Metrics, logging and stop

server.metrics() returns a fresh fixed-cardinality dictionary: accepted/rejected/
active/owned connections, tasks, requests and top-level command errors, protocol
errors, client read/write timeouts, allocation fail-stops, budget/preparation
rejections, accounted bytes/limit, physical/expiring keys, channels/subscriptions
and currently registered Pub/Sub outbox bytes/frames. Counters are local snapshots,
not RSS, durable ACK proof or per-EXEC-element statistics. No per-key/client labels
or history, new metrics RESP command or public HTTP endpoint. INFO replication is
unchanged. Read/write counters cover ordinary/Pub/Sub client handling, not peer I/O.

CLI adds --write-timeout, --shutdown-timeout (10s default) and --log-level
INFO/WARNING/ERROR (INFO default). Stop logging reports metrics without keys,
values, channels or arguments. In-process callers configure their own logging.

SIGINT/SIGTERM and Windows SIGBREAK where available initiate managed close. POSIX
uses event-loop signal handlers; the Windows main-thread callback schedules close
on the loop. Restore prior handlers on exit. Stop admission, close clients/peers/
subscribers, cancel client tasks after shutdown_timeout and drain persistence before
releasing files/threads. Tests verify restart and discard unexecuted queues. Windows
tests raise handled signals inside owned children; POSIX tests deliver external
signals. Windows console control-event delivery is not certified. shutdown_timeout
bounds client-task waiting, not uninterruptible disk/worker I/O. Existing durability
and Windows generation-retention limitations remain.
