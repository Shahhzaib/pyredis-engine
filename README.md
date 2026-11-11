# PyRedis Engine

PyRedis is an educational in-memory database server implemented in
Python. It speaks RESP2 and a tested subset of RESP3, with selected Redis-compatible
commands for strings, collections, expiration, transactions, Pub/Sub and Streams.
It includes optional journal/snapshot recovery and replication **between PyRedis
peers**. See the [command matrix](docs/command-matrix.md) for actual supported forms.

This is a **release candidate for review**, not production-certified software or a
complete Redis replacement. Redis Cluster, Sentinel, Redis-native AOF/RDB and native
Redis replication are not supported. SCAN mutation guarantees, consumer groups and
RESP3 remain partial; HyperLogLog uses a PyRedis-specific representation, and
GEOSEARCH uses a bounded linear scan and differs from Redis at the exact dateline.
See [compatibility limitations](docs/validation-and-limitations.md) and
[client compatibility](docs/client-compatibility.md).

By default the server listens only on `127.0.0.1` with an unauthenticated development
user. All listener and upstream addresses are restricted to literal loopback IPs.
Do not proxy it to an untrusted network. AUTH does not encrypt TCP; configure TLS
separately when needed. See [security and operations](docs/operations-and-security.md).

## Install and start

Python 3.11 or newer is required. The server has no third-party runtime dependencies.
From a clone, create an environment and install it:

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install .
pyredis-engine --host 127.0.0.1 --port 6380
```

In another terminal, install redis-py separately (`python -m pip install redis==7.4.1`)
and use its normal API:

```python
import redis

client = redis.Redis(host="127.0.0.1", port=6380, protocol=2)
client.set(b"binary-key", b"\x00\xff")
assert client.get(b"binary-key") == b"\x00\xff"
with client.pipeline(transaction=True) as pipe:
    assert pipe.set("counter", 1).incr("counter").execute() == [True, 2]
client.close()
```

`protocol=3` is available for [selected workflows](docs/protocol.md), not every Redis
response form. Use `python scripts/release_demo.py` after installing redis-py for an
isolated demonstration of transactions, expiration, Streams consumer groups, journal
restart, PyRedis replica synchronization and local metrics. The demo creates and
removes only its own temporary directory.

## Authentication, TLS and persistence

For protected local use, create a private ACL file interactively; the helper prompts
without echo and does not overwrite an existing file:

```bash
mkdir -p .pyredis-data
python -m pyredis_engine.security.credentials .pyredis-data/acl.json
pyredis-engine --acl-file .pyredis-data/acl.json
```

For TLS, additionally pass `--tls-cert-file` and `--tls-key-file` using files
provisioned outside the repository. Configure redis-py with `ssl=True`, a trusted
`ssl_ca_certs` path, `ssl_check_hostname=True`, and credentials obtained securely.
The [security guide](docs/operations-and-security.md) describes CA/client-certificate
settings, ACL syntax and limitations, credential handling and secure peer setup.
For example, with a locally provisioned certificate whose identity matches the host:

```python
import os
import redis

client = redis.Redis(host="localhost", port=6380, ssl=True,
                     ssl_ca_certs="/path/to/trusted-ca.pem", ssl_check_hostname=True,
                     username="admin", password=os.environ["PYREDIS_PASSWORD"])
assert client.ping()
client.close()
```

Persistence is opt-in. For example:

```bash
pyredis-engine --appendonly .pyredis-data/server.aof --appendfsync always --snapshots
```

The custom journal and snapshots are not Redis AOF/RDB. `always`, `everysec` and
`none` have different durability guarantees; Windows namespace durability and old
generation retention have documented limits. Read [persistence](docs/persistence.md)
for backup, recovery, compaction, tail repair and shutdown guidance.

To run a local primary and replica, use separate existing data directories and ports;
for example, run these in separate terminals after creating `.pyredis-data`:

```bash
pyredis-engine --port 6380 --appendonly .pyredis-data/primary.aof --appendfsync always --snapshots --replication-recovery
pyredis-engine --port 6381 --appendonly .pyredis-data/replica.aof --appendfsync always --snapshots --replication-recovery --replicaof 127.0.0.1 6380
```

Replication is a private PyRedis protocol, not Redis PSYNC. Consult
[replication](docs/replication.md) for secure options and trust boundaries.

A minimal Streams consumer-group workflow using a fresh key and group:

```python
import redis

client = redis.Redis(host="127.0.0.1", port=6380)
entry = client.xadd("events", {"message": "hello"})
client.xgroup_create("events", "workers", id="0")
messages = client.xreadgroup("workers", "worker-1", {"events": ">"})
assert messages[0][1][0][0] == entry
assert client.xack("events", "workers", entry) == 1
client.close()
```

See [Streams](docs/streams.md) for supported operations and consumer-group limitations.

## Docker and Compose

The image runs as UID/GID 10001 and uses `/data` for persistent files. The checked-in
examples publish **no host ports**: loopback inside a container is not reachable
through normal Docker port publishing. Use `docker compose exec` to access it from
the same network namespace, or run the native local installation above.

```bash
docker compose up --build -d
# Wait for the server to report healthy in `docker compose ps`.
docker compose exec server python -c "import socket; s=socket.create_connection(('127.0.0.1',6380)); s.sendall(bytes.fromhex('2a310d0a24340d0a50494e470d0a')); print(s.recv(64))"
docker compose down
```

For two PyRedis peers sharing a private network namespace:

```bash
docker compose -f compose.replication.yaml up --build -d
# Wait for the replica to report healthy in `docker compose -f compose.replication.yaml ps`.
docker compose -f compose.replication.yaml exec primary python -c "import socket; s=socket.create_connection(('127.0.0.1',6381)); s.sendall(bytes.fromhex('2a310d0a24340d0a50494e470d0a')); print(s.recv(64))"
docker compose -f compose.replication.yaml down
```

Named volumes survive `down`. The image's health check verifies TCP acceptance, not
authentication, replica freshness or data integrity. The examples are plaintext,
unauthenticated loopback demonstrations, not secure remote deployment. Bind mounts
must be writable by UID/GID 10001. See [container operations](docs/container-operations.md)
before changing volumes, networking or security configuration.

## Development and verification

```bash
python -m pip install -e '.[dev]'
python scripts/check.py --quick
python scripts/check.py --all
```

The full gate includes the unit/integration suite, property/fault/differential tests,
Ruff and Mypy. To require every differential comparison, point
`PYREDIS_REDIS_SERVER` at an isolated official Redis 7.4.12 executable and run
`python scripts/check.py --all --require-redis`. Missing or skipped mandatory Redis
comparisons fail that strict gate. See [testing strategy](docs/testing-strategy.md).
The checked-in CI workflow has not been claimed as passing until it runs on GitHub.

The [architecture](docs/architecture.md) explains ordered execution, persistence and
replication boundaries. The [performance report](docs/performance.md) records workload,
hardware, latency, throughput and RSS measurements; the configured datastore budget
is not a Python heap or RSS ceiling. See [validation and limitations](docs/validation-and-limitations.md)
for the verified scope. See [dependency and license notices](docs/dependencies-and-licenses.md)
and [reference attribution](docs/reference-and-license.md).
