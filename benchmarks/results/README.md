# Benchmark data

These JSON files are recorded synthetic workload observations. They are not capacity
or safety guarantees. [Performance methodology](../../docs/performance.md) describes
hardware, parameters, correctness checks, measurement limitations and selected results.

Reproduce a bounded run from the repository root, choosing a new output path:

```bash
python scripts/benchmark.py --suite smoke --output build/smoke.json
```

Other suites include matrix, range, overhead, profile, stress, memory and compare.
The compare suite requires an isolated official Redis 7.4.12 executable. Do not point
it at a shared or production service. Files include historical revision identifiers
as provenance; those identifiers are not part of this fresh public repository history.
