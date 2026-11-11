"""CLI entry point; localhost-only until security review."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections.abc import Callable
from types import FrameType

from .config import ServerConfig
from .server import start_server


async def run_server(config: ServerConfig) -> None:
    server = await start_server(config)
    loop = asyncio.get_running_loop()
    handlers: dict[signal.Signals, Callable[[int, FrameType | None], object] | int | None] = {}
    event_handlers: set[signal.Signals] = set()
    try:
        for name in ('SIGINT', 'SIGTERM', 'SIGBREAK'):
            number = getattr(signal, name, None)
            if number is None:
                continue
            signum = signal.Signals(number)
            previous = signal.getsignal(signum)
            try:
                loop.add_signal_handler(signum, server.close)
                event_handlers.add(signum)
            except NotImplementedError:
                # Windows ProactorEventLoop has no add_signal_handler support.
                def stop(number: int, frame: FrameType | None) -> None:
                    loop.call_soon_threadsafe(server.close)
                signal.signal(signum, stop)
            handlers[signum] = previous
        bound = server.sockets[0].getsockname()
        print(f'PyRedis Engine listening on {bound[0]}:{bound[1]}', flush=True)
        async with server:
            await server.stopping.wait()
        logging.getLogger(__name__).info('server stopped; metrics=%s', server.metrics())
        if server.executor.failed:
            raise RuntimeError('server stopped after allocation failure')
    finally:
        try:
            server.close()
            await server.wait_closed()
        finally:
            for signum, previous in handlers.items():
                if signum in event_handlers:
                    loop.remove_signal_handler(signum)
                signal.signal(signum, previous)


def main() -> None:
    parser = argparse.ArgumentParser(description="PyRedis Engine development server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--max-clients", type=int, default=256)
    parser.add_argument("--read-timeout", type=float, default=10.0)
    parser.add_argument('--write-timeout', type=float, default=10.0)
    parser.add_argument('--shutdown-timeout', type=float, default=10.0)
    parser.add_argument('--maxmemory', type=int, default=256 * 1024 * 1024, metavar='BYTES',
                        help='accounted datastore budget in bytes; no eviction (not Python RSS)')
    parser.add_argument('--log-level', choices=('INFO', 'WARNING', 'ERROR'), default='INFO')
    parser.add_argument('--acl-file', metavar='PATH', help='private hash-only ACL bootstrap file')
    parser.add_argument('--tls-cert-file', metavar='PATH')
    parser.add_argument('--tls-key-file', metavar='PATH')
    parser.add_argument('--tls-key-password-file', metavar='PATH')
    parser.add_argument('--tls-ca-file', metavar='PATH')
    parser.add_argument('--tls-client-auth', choices=('none', 'optional', 'required'),
                        default='none')
    parser.add_argument('--tls-handshake-timeout', type=float, default=10.0)
    parser.add_argument('--replication-secure', action='store_true')
    parser.add_argument('--replica-username')
    parser.add_argument('--replica-password-file', metavar='PATH')
    parser.add_argument('--replica-server-name')
    parser.add_argument('--replica-tls-cert-file', metavar='PATH')
    parser.add_argument('--replica-tls-key-file', metavar='PATH')
    parser.add_argument('--replica-tls-key-password-file', metavar='PATH')
    parser.add_argument('--replication-client-cert-sha256')
    parser.add_argument("--appendonly", metavar="PATH",
                        help="enable the custom PyRedis effect journal")
    parser.add_argument("--appendfsync", choices=("none", "everysec", "always"), default="everysec")
    parser.add_argument("--aof-repair-tail", action="store_true",
                        help="explicitly truncate only an incomplete final AOF record")
    parser.add_argument("--snapshots", action="store_true",
                        help="enable snapshot generation recovery")
    parser.add_argument("--compact-on-start", action="store_true",
                        help="create and activate a snapshot before serving")
    parser.add_argument('--replication', action='store_true',
                        help='enable private PyRedis full-sync primary connections')
    parser.add_argument('--replicaof', nargs=2, metavar=('HOST', 'PORT'),
                        help='follow a loopback PyRedis primary (requires snapshots and journal)')
    parser.add_argument('--replication-recovery', action='store_true',
                        help='enable private v2 backlog, ACKs and automatic reconnect')
    args = parser.parse_args()
    try:
        config = ServerConfig(
            host=args.host, port=args.port, max_clients=args.max_clients,
            read_timeout=args.read_timeout,
            write_timeout=args.write_timeout, shutdown_timeout=args.shutdown_timeout,
            maxmemory_bytes=args.maxmemory,
            acl_file=args.acl_file,
            tls_cert_file=args.tls_cert_file, tls_key_file=args.tls_key_file,
            tls_key_password_file=args.tls_key_password_file, tls_ca_file=args.tls_ca_file,
            tls_client_auth=args.tls_client_auth, tls_handshake_timeout=args.tls_handshake_timeout,
            replication_secure=args.replication_secure, replica_username=args.replica_username,
            replica_password_file=args.replica_password_file,
            replica_server_name=args.replica_server_name,
            replica_tls_cert_file=args.replica_tls_cert_file,
            replica_tls_key_file=args.replica_tls_key_file,
            replica_tls_key_password_file=args.replica_tls_key_password_file,
            replication_client_cert_sha256=args.replication_client_cert_sha256,
            aof_path=args.appendonly, aof_fsync=args.appendfsync,
            aof_repair_tail=args.aof_repair_tail,
            snapshot_enabled=args.snapshots or args.compact_on_start,
            compact_on_start=args.compact_on_start,
            replication_enabled=(args.replication or args.replicaof is not None
                                 or args.replication_recovery),
            replication_recovery=args.replication_recovery,
            replica_host=args.replicaof[0] if args.replicaof else None,
            replica_port=int(args.replicaof[1]) if args.replicaof else None,
        )
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(level=args.log_level, format='%(levelname)s %(name)s %(message)s')
    try:
        asyncio.run(run_server(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
