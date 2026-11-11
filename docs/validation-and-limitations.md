# Validation and compatibility limitations

This snapshot is an educational release candidate, not a production certification
or complete Redis implementation. The [command matrix](command-matrix.md) identifies
implemented forms. RESP3, Streams consumer groups and SCAN mutation guarantees are
partial; unsupported commands and options return errors.

A Linux Python 3.13.18 full gate recorded 1,865 passing tests, including 305 strict
comparisons with Redis 7.4.12, zero skips, and passing Ruff/Mypy checks. A Windows
Python 3.14 quick run recorded 1,349 passes and one optional redis-cli skip. These
results describe those environments, not all platforms or every Redis behavior.
Hosted GitHub Actions results remain unverified.

The public snapshot was additionally checked on 2026-11-12: 31 focused protocol,
geographic, demonstration and Redis 7.4.12 tests passed in Linux Python 3.13. A
fresh Windows environment installed the built wheel and its CLI answered PING.
The locally built container ran as UID 10001 and passed startup, PING and shutdown
checks. Isolated standalone and primary-replica Compose smoke tests also passed;
the replica example requires waiting for readiness. These checks do not replace
hosted CI, independent security review or full platform compatibility testing.

GEOSEARCH uses a bounded linear scan. At the exact dateline, its candidate set can
differ from Redis for searches involving +180 and -180 and nearby points, even when
GEOADD scores agree. This is a known nonblocking compatibility limitation for this
snapshot, not a fixed defect; the broader geographic edge matrix is not exhaustive.
See [geographic behavior](probabilistic-and-geographic.md). HyperLogLog uses a
PyRedis-specific binary representation and estimator.

Persistence and replication formats are private to PyRedis; Redis AOF, RDB, PSYNC,
Cluster, Sentinel, failover and scripting are not supported. Windows filesystem
namespace durability and retained generations have documented limits. The memory
budget accounts committed datastore state rather than actual Python process RSS.
Loopback is the only supported bind/upstream address. Default development access is
unauthenticated; use the [security guide](operations-and-security.md) before use,
and remember that password authentication without TLS is plaintext.

Independent provenance and security review, licensing/attribution review and hosted
CI verification remain outstanding. Do not infer production readiness from the tests.
