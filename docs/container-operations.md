# Container operations

The local image is built from a digest-pinned official Python base. Build-time
setuptools and wheel versions are pinned with hashes; the runtime install has no
third-party dependencies. The image runs as UID/GID 10001, logs to standard output
and error, uses `/data`, and handles SIGTERM through the server's normal shutdown
path. The Docker health check tests a TCP connection on `PYREDIS_HEALTH_PORT`
(default 6380); set that variable if changing the server port. A successful health
check is not an authentication, durability or replica-readiness check.

The default listener and permitted upstreams are **loopback only**. Docker port
publishing to the container's bridge address will not reach that listener. The
Compose examples do not publish ports: access a server with `docker compose exec`.
The replication example uses `network_mode: service:primary` so both processes share
loopback, with separate named volumes and ports 6380/6381. This is trusted local,
plaintext, unauthenticated development; it is not Redis-native replication or a
recommendation to share the namespace with untrusted workloads. No certificates or
passwords are supplied by the examples.

The named volumes survive `docker compose down`. New Docker-managed volumes inherit
the image's `/data` ownership; for an existing volume or bind mount, ensure the
directory is writable by UID/GID 10001 before starting. Do not make data or ACL
files world-writable. Keep private ACL files, certificates and keys outside the image
and source control; mount them read-only with suitable ownership if using protected
local TLS. Plain AUTH sends credentials unencrypted. See
[security operations](operations-and-security.md) for supported ACL/TLS options and
replication trust boundaries.

Back up only a quiescent or otherwise consistent complete journal/snapshot generation
set, and test restore into a separate private directory. Do not copy a live manifest
and unrelated generation files independently. See [persistence](persistence.md) for
the exact recovery and platform constraints. `docker compose stop` sends SIGTERM;
allow the configured shutdown timeout and do not assume a forced kill is graceful.
Datastore maxmemory accounts committed values, not total container RSS or disk usage.
