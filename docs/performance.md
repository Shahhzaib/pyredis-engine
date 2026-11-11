# Performance observations

These are bounded local observations, not production capacity, latency SLOs,
an independent audit or proof of leak freedom. Raw samples, environment and
parameters are in [benchmark results](../benchmarks/results/README.md).

## Method and environment

Run `python scripts/benchmark.py --suite smoke --output build/benchmark-smoke.json`.
Suites: matrix, range, overhead, profile, stress, memory, compare. Output paths must
be new; run without -O. No dependencies are installed. Each case uses a fresh owned
server process, separate Python load generator and loopback TCP, pipeline depth one.
Persisted cases use isolated owned workspace temp directories. Responses and complete
final contents/counts are checked; INCR checks exact per-key sums. Replica state and
offset are checked after catch-up. Invalid comparisons fail without publishing results.

Initialization and four operations/client of warmup precede three sequential measured
samples. Each sample targets at least 20 operations/client and 0.15s, capped at
min(2048, floor(60000/(clients*samples))) operations/client. JSON records actual counts
and durations. Throughput is total measured operations / total measured wall time;
latency is end-to-end Python client operation time, including encoding, receive and
response validation, with pooled nearest-rank p50/p95/p99. Samples share warmed state;
they are not independent server replicas or confidence intervals. Profiling and
tracemalloc runs are separate from throughput runs.

Windows 11 build 26300, Python 3.14.7 (64-bit), Intel Core i7-8565U @ 1.80GHz,
8 logical CPUs, 16,940,797,952 physical RAM bytes. Storage was the workspace volume;
disk model/medium was unavailable because the optional CIM query was denied. Filesystem
caches, background load, CPU frequency and affinity were not controlled. Python used
default randomized per-process hash seeds; workloads are reproducible, not bit-for-bit
performance traces. Redis pairs ran in fixed PyRedis-then-Redis order. The matrix
covered 72 cases and 220,855 measured operations in 94.88s including setup/verification.

GET/SET: 64/512 keys, 32/1024/65536-byte values, 1/10/100 clients. INCR uses ASCII
integers. HSET replaces existing fields with the same 32-byte value; RPUSH grows a
32-byte-item list; SADD inserts unique compact identifiers. ZADD alternates scores of
existing members; ZRANGE returns the first ten members WITHSCORES. Collection sizes
start at 64/512; growth is finite and verified. Profile sizes additionally reach 4096.

Safety: at most 100 load clients, 64 MiB accounted budget per normal worker, finite
operation caps and a 600s async suite deadline. An RSS watchdog checks a 256 MiB
threshold at roughly 10ms and control boundaries; it is not an OS-enforced ceiling.
Profiles use fixed counts; cleanup may add bounded process-wait time. Stress uses a
1 MiB accounting budget instead of exhausting the host. All endpoints are loopback.

## Windows command observations

512 initial keys/elements, ten clients; scalar values 32 bytes (INCR numeric).
These selected rows come from the full matrix, including repeated writes/growth
described above; they are not claims about all workloads.

| Command | ops/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|
| GET | 6,815 | 1.369 | 1.991 | 2.400 |
| SET | 5,849 | 1.626 | 2.160 | 2.879 |
| INCR | 4,930 | 1.901 | 2.780 | 3.493 |
| HSET | 4,158 | 2.288 | 3.150 | 3.615 |
| RPUSH | 3,966 | 2.417 | 3.318 | 3.907 |
| SADD | 3,305 | 2.629 | 4.123 | 7.330 |
| ZADD | 3,728 | 2.514 | 3.687 | 4.030 |
| ZRANGE WITHSCORES | 2,768 | 3.408 | 4.979 | 5.687 |

All client counts/payloads and individual samples are in [M13-windows-matrix.json](../benchmarks/results/M13-windows-matrix.json).
The largest captured timer overshoot was 65.601ms (100-client SADD). The 10ms probe
includes OS timer resolution/scheduling, observer/control work and transitions around
the measurement window; it does not isolate executor stalls. Process CPU deltas are
recorded per sample; Windows clock quantization and control overhead preclude precise
CPU-utilization claims. Load-generator CPU can cap these closed-loop results.

## Persistence, snapshots and replication

Matched SET: ten clients, 512 keys, 1 KiB values, three samples. Durability policies
are explicit; none/everysec do not guarantee a per-write durable ACK.

| Mode | ops/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|
| Memory | 4,407 | 2.082 | 3.274 | 3.617 |
| AOF none | 1,613 | 6.079 | 7.296 | 8.166 |
| AOF everysec | 1,630 | 5.906 | 7.098 | 9.489 |
| AOF always | 829 | 11.994 | 13.420 | 14.114 |
| Always + concurrent compaction | 601 | 15.511 | 20.656 | 71.323 |
| Always primary + always replica | 720 | 13.600 | 15.809 | 18.531 |

Concurrent compactions completed in 0.344/0.405/0.398s. Foreground latency excludes
waiting for maintenance after its timed window; JSON retains maintenance completion
metrics. Replication responses wait for local policy, not a quorum. The final replica
reached the exact target/state in an additional 2.108ms. This is steady live propagation
after an empty initial sync, not an initial large-snapshot transfer benchmark. Total
overhead-suite runtime was 8.59s; no durability/safety checks were disabled.

## RSS, accounting and retention

Final RSS probe v2 reuses ctypes bindings. Windows values below are actual process
WorkingSet/PeakWorkingSet, not tracemalloc or a heap estimate. OS peak covers process
lifetime, including initialization; current RSS is after verification. Budget: 64 MiB.

| Workload | Current RSS MiB | Peak RSS MiB | Accounted MiB |
|---|---:|---:|---:|
| GET, 1 client, 64 x 32 B | 30.375 | 30.375 | 0.01416 |
| SET, 1 client, 64 x 32 B | 30.004 | 30.004 | 0.01416 |
| GET, 100 clients, 512 x 64 KiB | 72.133 | 83.531 | 32.09766 |
| SET, 100 clients, 512 x 64 KiB | 74.602 | 84.789 | 32.09766 |

M12's accounting budget is not a process ceiling. Interpreter, indexes/allocator
overhead, temporary copies and network buffers explain excluded space; these runs
do not apportion all of it. The generator/replica have separate processes. No exact
Python heap/RSS ceiling is claimed.

The first probe rebuilt ctypes wrappers per read. A warmed 100-read diagnostic retained
21,164 traced bytes (713,584-byte peak); cached bindings retained zero (26,160-byte peak).
The initial matrix/paired optimization used the same original observer; focused memory
and retention runs were repeated with v2 rather than silently rewriting old evidence.

Six equal create/delete rounds (first warmup) returned accounted keys/bytes to zero.
V2 traced retention grew 8,668 bytes; RSS changed from 30.762 to 30.891 MiB and then
plateaued over the final two samples. Top allocations include Proactor buffers, ctypes
and retained dictionary capacity. This short run does not establish leak freedom or
unbounded growth. Longer attribution/soak remains pending.

The 1 MiB quota admitted 856 keys, charged 1,048,490 bytes, then rejected another SET;
all admitted values were verified. Pressure kept at most eight admitted clients with
17 rejected arrivals. A nonreading subscriber was absent by the ninth publish request,
while a fast subscriber continued receiving. Final tasks/connections/subscriptions
were zero; only the expected counter remained (203 accounted bytes). 3,000 exact
increments completed in 0.875s. Complete revised stress runtime: 5.13s.

## Profile evidence and scoped optimization

Baseline: score formatting consumed 0.3158s of a 0.3629s profiled 200-operation
ZRANGE-10 workload. Added only a 4,096-entry LRU of immutable score bytes, preserving
the formatting algorithm, special values and all resource/durability semantics.
Cache metadata is outside datastore accounting. No new dependency/index/eviction
policy was introduced. Golden, seeded binary64, bounded-cache and Redis checks pass.

| Dedicated matched warm ZRANGE run | Before ops/s | After ops/s | Ratio |
|---|---:|---:|---:|
| 1 client, 512 members | 958 | 1,869 | 1.95x |
| 10 clients, 512 members | 1,292 | 3,748 | 2.90x |
| 100 clients, 512 members | 1,270 | 3,610 | 2.84x |

The profiled 200-operation case fell to 0.0368s; ten full 4096-member scored ranges
fell from 7.214 to 0.480s. Profiles are instrumented timings, not TCP throughput.
DBSIZE still scaled from 0.021s (64 keys) to 0.815s (4096) per 200 profiled calls.
Collection copy preparation and ranked-list changes remain O(N). No unsafe rewrite
was attempted. Cache cold misses, high-cardinality churn, random scores and wider
platform/storage scales need more measurement. The dedicated retest and later matrix
vary substantially; no universal speedup, capacity ceiling or scalability extrapolation.

## Matched official Redis comparison

Same isolated Linux/WSL2 container, Python 3.11.18 generator, Redis 7.4.12, loopback,
512 keys, pipeline one, no persistence, identical warmup/samples/validation. Guest reports
8 logical CPUs and 8,017,568 KiB RAM, no cgroup CPU/memory quota. Fresh server processes
per case; nine pairs (1/10/100 clients) completed in 21.45s. Windows numbers above
must not be compared directly to these Linux numbers. Ten-client selected rows:

| Command | PyRedis ops/s | Redis ops/s | PyRedis p99 ms | Redis p99 ms |
|---|---:|---:|---:|---:|
| GET 32 B | 10,293 | 12,587 | 1.439 | 1.080 |
| SET 32 B | 8,175 | 13,067 | 1.697 | 1.063 |
| INCR | 8,483 | 12,312 | 1.900 | 1.397 |

This Python generator/response checker limits throughput, especially for Redis;
these are matched workload observations, not Redis/PyRedis maximum capacities.
Reference CPU uses coarse /proc ticks; it has no Python monitor. No native Redis
replication interoperability or full command compatibility is implied.

## Remaining acceptance

The later validation results are described in [validation](validation-and-limitations.md).
Long-duration leak/soak, cold/churning cache, large initial
sync, larger datasets, pipelining, dedicated-host/client capacity tests, storage/power-loss
characterization and statistical repeatability remain unmeasured.
RSS probes currently support Windows/Linux only; macOS/other-platform benchmark
support and acceptance remain outstanding. E00 subsequently fixes local EXEC reply
overflow; its savepoint overhead is not measured by these historical benchmarks.
Windows generation retention limits remain.
