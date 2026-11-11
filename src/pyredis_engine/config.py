from __future__ import annotations

import ipaddress
import math
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 6380
    max_clients: int = 256
    read_timeout: float = 10.0
    max_bulk_bytes: int = 8 * 1024 * 1024
    max_frame_bytes: int = 16 * 1024 * 1024
    max_args: int = 1024
    max_header_bytes: int = 8192
    write_timeout: float = 10.0
    max_depth: int = 64
    max_input_bytes: int = 16 * 1024 * 1024
    max_output_bytes: int = 64 * 1024
    max_reply_bytes: int = 16 * 1024 * 1024
    expiry_interval: float = 0.1
    expiry_batch_size: int = 128
    max_collection_items: int = 100_000
    max_collection_bytes: int = 16 * 1024 * 1024
    max_transaction_commands: int = 1024
    max_transaction_bytes: int = 1024 * 1024
    max_watched_keys: int = 1024
    max_watch_bytes: int = 1024 * 1024
    aof_path: str | None = None
    aof_fsync: str = "everysec"
    aof_repair_tail: bool = False
    aof_sync_interval: float = 1.0
    aof_max_record_bytes: int = 64 * 1024 * 1024
    aof_max_dataset_bytes: int = 256 * 1024 * 1024
    aof_max_keys: int = 1_000_000
    snapshot_enabled: bool = False
    snapshot_max_bytes: int = 512 * 1024 * 1024
    snapshot_max_tail_bytes: int = 64 * 1024 * 1024
    snapshot_max_generations: int = 64
    compact_on_start: bool = False
    replication_enabled: bool = False
    replica_host: str | None = None
    replica_port: int | None = None
    max_replicas: int = 4
    replication_queue_bytes: int = 64 * 1024 * 1024
    replication_queue_frames: int = 4096
    replication_timeout: float = 10.0
    replication_heartbeat_interval: float = 1.0
    replication_recovery: bool = False
    replication_backlog_bytes: int = 64 * 1024 * 1024
    replication_backlog_frames: int = 4096
    replication_retry_attempts: int = 8
    replication_retry_delay: float = 0.1
    replication_retry_max_delay: float = 5.0
    replication_retry_reset_after: float = 30.0
    replication_ack_timeout: float = 30.0
    replication_ack_max_pending: int = 4096
    replication_catchup_timeout: float = 30.0
    max_subscriptions: int = 1024
    max_subscription_bytes: int = 1024 * 1024
    pubsub_queue_bytes: int = 16 * 1024 * 1024
    pubsub_queue_frames: int = 1024
    maxmemory_bytes: int = 256 * 1024 * 1024
    shutdown_timeout: float = 10.0
    acl_file: str | None = None
    tls_cert_file: str | None = None
    tls_key_file: str | None = field(default=None, repr=False)
    tls_key_password_file: str | None = field(default=None, repr=False)
    tls_ca_file: str | None = None
    tls_client_auth: str = 'none'
    tls_handshake_timeout: float = 10.0
    replication_secure: bool = False
    replica_username: str | None = None
    replica_password_file: str | None = field(default=None, repr=False)
    replica_server_name: str | None = None
    replica_tls_cert_file: str | None = None
    replica_tls_key_file: str | None = field(default=None, repr=False)
    replica_tls_key_password_file: str | None = field(default=None, repr=False)
    replication_client_cert_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.acl_file is not None:
            if not isinstance(self.acl_file, str) or not self.acl_file or len(self.acl_file) > 4096:
                raise ValueError('invalid ACL file path')
            if self.replication_enabled and not self.replication_secure:
                raise ValueError('protected replication requires E01B peer authentication')
        paths = (self.tls_cert_file, self.tls_key_file, self.tls_key_password_file,
                 self.tls_ca_file, self.replica_password_file, self.replica_tls_cert_file,
                 self.replica_tls_key_file, self.replica_tls_key_password_file)
        if any(value is not None and (not isinstance(value, str) or not value
                                      or len(value) > 4096 or '\x00' in value) for value in paths):
            raise ValueError('invalid TLS or credential file path')
        if not isinstance(self.replication_secure, bool):
            raise ValueError('replication_secure must be boolean')
        if (self.tls_cert_file is None) != (self.tls_key_file is None):
            raise ValueError('TLS requires both certificate and private key')
        if self.tls_ca_file and not self.tls_cert_file:
            raise ValueError('TLS CA requires a TLS listener')
        if self.tls_key_password_file and not self.tls_key_file:
            raise ValueError('TLS key password requires a private key')
        if self.tls_client_auth not in ('none', 'optional', 'required'):
            raise ValueError('invalid TLS client certificate policy')
        if self.tls_client_auth != 'none' and (not self.tls_cert_file or not self.tls_ca_file):
            raise ValueError('client certificate verification requires TLS and a CA')
        if (not isinstance(self.tls_handshake_timeout, (int, float))
                or isinstance(self.tls_handshake_timeout, bool)
                or not math.isfinite(self.tls_handshake_timeout)
                or self.tls_handshake_timeout <= 0):
            raise ValueError('invalid TLS handshake timeout')
        if self.replication_secure and (not self.replication_enabled or not self.acl_file
                                        or not self.tls_cert_file):
            raise ValueError('secure replication requires replication, ACL and TLS')
        if self.replication_enabled and self.tls_cert_file and not self.replication_secure:
            raise ValueError('TLS replication requires secure replication')
        if (self.replica_tls_cert_file is None) != (self.replica_tls_key_file is None):
            raise ValueError('replica TLS requires both certificate and private key')
        if self.replica_tls_key_password_file and not self.replica_tls_key_file:
            raise ValueError('replica TLS key password requires a private key')
        replica_options = (self.replica_username, self.replica_password_file,
                           self.replica_server_name, self.replica_tls_cert_file,
                           self.replica_tls_key_file, self.replica_tls_key_password_file)
        if any(value is not None for value in replica_options) and (
                not self.replication_secure or self.replica_host is None):
            raise ValueError('replica credentials require a secure upstream')
        if self.replication_secure and self.replica_host is not None:
            if not self.replica_username or not self.replica_password_file \
                    or not self.replica_server_name or not self.tls_ca_file:
                raise ValueError('secure upstream requires username, password file, CA and name')
            try:
                username_bytes = self.replica_username.encode('ascii')
            except (AttributeError, UnicodeError):
                raise ValueError('invalid replica username') from None
            if not 1 <= len(username_bytes) <= 64 or not all(
                    33 <= byte <= 126 for byte in username_bytes):
                raise ValueError('invalid replica username')
            if (not isinstance(self.replica_server_name, str) or not self.replica_server_name
                    or len(self.replica_server_name) > 253 or '\x00' in self.replica_server_name):
                raise ValueError('invalid replica server name')
            try:
                ipaddress.ip_address(self.replica_server_name)
            except ValueError:
                try:
                    encoded_name = self.replica_server_name.encode('idna')
                    labels = encoded_name.split(b'.')
                except UnicodeError:
                    raise ValueError('invalid replica server name') from None
                if len(encoded_name) > 253 or any(not 1 <= len(label) <= 63
                       or label.startswith(b'-') or label.endswith(b'-') or not all(
                           48 <= char <= 57 or 65 <= char <= 90 or 97 <= char <= 122 or char == 45
                           for char in label) for label in labels):
                    raise ValueError('invalid replica server name') from None
        if self.replication_client_cert_sha256 is not None:
            try:
                fingerprint = bytes.fromhex(self.replication_client_cert_sha256)
            except (TypeError, ValueError):
                raise ValueError('invalid replica certificate pin') from None
            if (not self.replication_secure or self.replica_host is not None
                    or self.tls_client_auth == 'none' or len(fingerprint) != 32
                    or len(self.replication_client_cert_sha256) != 64):
                raise ValueError('invalid replica certificate pin configuration')
        # Validate types before arithmetic/range comparisons: bool is not a limit.
        for name in ('port', 'max_clients', 'max_bulk_bytes', 'max_frame_bytes', 'max_args',
                     'max_header_bytes', 'max_depth', 'max_input_bytes', 'max_output_bytes',
                     'max_reply_bytes', 'maxmemory_bytes'):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(name + ' must be an integer')
        if self.maxmemory_bytes < 1:
            raise ValueError('maxmemory_bytes must be positive')
        for name in ('read_timeout', 'write_timeout', 'expiry_interval', 'shutdown_timeout'):
            value = getattr(self, name)
            if (not isinstance(value, (int, float)) or isinstance(value, bool)
                    or not math.isfinite(value) or value <= 0):
                raise ValueError(name + ' must be finite and positive')
        if not isinstance(self.host, str):
            raise ValueError('host must be a loopback IP address')
        try:
            addr = ipaddress.ip_address(self.host)
        except ValueError as exc:
            raise ValueError("host must be a loopback IP address") from exc
        if not addr.is_loopback:
            raise ValueError("non-loopback binding is disabled")
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if self.max_clients < 1 or self.max_args < 1:
            raise ValueError("max_clients and max_args must be positive")
        if self.max_bulk_bytes < 1 or self.max_frame_bytes < self.max_bulk_bytes:
            raise ValueError("invalid frame/bulk limits")
        if self.max_header_bytes < 32 or self.read_timeout <= 0 or self.write_timeout <= 0:
            raise ValueError("invalid header limit or timeout")
        if not math.isfinite(self.read_timeout) or not math.isfinite(self.write_timeout):
            raise ValueError("timeouts must be finite")
        # Bound Python recursion as well as protocol nesting. Input must fit a
        # permitted bulk body and its CRLF, and a complete header.
        if not 1 <= self.max_depth <= 128:
            raise ValueError("max_depth must be between 1 and 128")
        if self.max_input_bytes < max(self.max_bulk_bytes + 2, self.max_header_bytes):
            raise ValueError("input buffer must fit bulk terminator and headers")
        if self.max_output_bytes < 1:
            raise ValueError("output buffer limit must be positive")
        if self.max_reply_bytes < 128:
            raise ValueError("reply limit must be at least 128 bytes for control replies")
        if not math.isfinite(self.expiry_interval) or self.expiry_interval <= 0:
            raise ValueError("expiry_interval must be finite and positive")
        if (not isinstance(self.expiry_batch_size, int) or isinstance(self.expiry_batch_size, bool)
                or self.expiry_batch_size < 1):
            raise ValueError("expiry_batch_size must be positive")
        for limit in (self.max_collection_items, self.max_collection_bytes):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("collection limits must be positive integers")
        for limit in (self.max_transaction_commands, self.max_transaction_bytes,
                      self.max_watched_keys, self.max_watch_bytes):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("transaction/watch limits must be positive integers")
        if self.aof_path is not None and (not isinstance(self.aof_path, str)
                or not self.aof_path.strip() or "\x00" in self.aof_path):
            raise ValueError("aof_path must be a nonempty filesystem path")
        if self.aof_fsync not in ("none", "everysec", "always"):
            raise ValueError("aof_fsync must be none, everysec or always")
        if not isinstance(self.aof_repair_tail, bool):
            raise ValueError("aof_repair_tail must be boolean")
        if self.aof_repair_tail and self.aof_path is None:
            raise ValueError("tail repair requires an explicit aof_path")
        if (not isinstance(self.aof_sync_interval, (int, float))
                or isinstance(self.aof_sync_interval, bool)
                or not math.isfinite(self.aof_sync_interval) or self.aof_sync_interval <= 0):
            raise ValueError("aof_sync_interval must be finite and positive")
        for limit in (self.aof_max_record_bytes, self.aof_max_dataset_bytes, self.aof_max_keys):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("AOF limits must be positive integers")
        if not 1024 <= self.aof_max_record_bytes <= 2**32 - 1:
            raise ValueError("AOF record limit must be between 1024 and 2**32-1")
        if (not isinstance(self.snapshot_enabled, bool)
                or not isinstance(self.compact_on_start, bool)):
            raise ValueError("snapshot flags must be boolean")
        if self.snapshot_enabled and self.aof_path is None:
            raise ValueError("snapshots require an aof_path")
        if self.compact_on_start and not self.snapshot_enabled:
            raise ValueError("compact_on_start requires snapshot_enabled")
        for limit in (self.snapshot_max_bytes, self.snapshot_max_tail_bytes,
                      self.snapshot_max_generations):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("snapshot limits must be positive integers")
        if self.snapshot_max_bytes < 128 or not 2 <= self.snapshot_max_generations <= 1024:
            raise ValueError("invalid snapshot byte/generation limits")
        if not isinstance(self.replication_enabled, bool):
            raise ValueError("replication_enabled must be boolean")
        if self.replication_enabled and not self.snapshot_enabled:
            raise ValueError("replication requires snapshot-enabled persistence")
        if (self.replica_host is None) != (self.replica_port is None):
            raise ValueError("replica host and port must be configured together")
        if self.replica_host is not None:
            if not self.replication_enabled:
                raise ValueError("replica upstream requires replication_enabled")
            try:
                upstream = ipaddress.ip_address(self.replica_host)
            except ValueError as exc:
                raise ValueError("replica host must be a loopback IP address") from exc
            if not upstream.is_loopback:
                raise ValueError("non-loopback replication is disabled")
            if (not isinstance(self.replica_port, int) or isinstance(self.replica_port, bool)
                    or not 1 <= self.replica_port <= 65535):
                raise ValueError("replica port must be between 1 and 65535")
        for limit in (self.max_replicas, self.replication_queue_bytes,
                      self.replication_queue_frames):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("replication limits must be positive integers")
        for timeout in (self.replication_timeout, self.replication_heartbeat_interval):
            if (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
                    or not math.isfinite(timeout) or timeout <= 0):
                raise ValueError("replication timeouts must be finite and positive")
        if self.replication_heartbeat_interval >= self.replication_timeout:
            raise ValueError("replication heartbeat must be shorter than timeout")
        if not isinstance(self.replication_recovery, bool):
            raise ValueError("replication_recovery must be boolean")
        if self.replication_recovery and not self.replication_enabled:
            raise ValueError("replication recovery requires replication_enabled")
        for limit in (self.replication_backlog_bytes, self.replication_backlog_frames,
                      self.replication_retry_attempts, self.replication_ack_max_pending):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("recovery limits must be positive integers")
        if self.replication_retry_attempts > 1024:
            raise ValueError("recovery attempt budget must not exceed 1024")
        for timeout in (self.replication_retry_delay, self.replication_retry_max_delay,
                        self.replication_retry_reset_after, self.replication_ack_timeout,
                        self.replication_catchup_timeout):
            if (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
                    or not math.isfinite(timeout) or timeout <= 0):
                raise ValueError("recovery timeouts must be finite and positive")
        if self.replication_retry_delay > self.replication_retry_max_delay:
            raise ValueError("initial retry delay must not exceed maximum")
        if self.replication_ack_timeout <= self.replication_heartbeat_interval:
            raise ValueError("ACK timeout must exceed heartbeat interval")
        for limit in (self.max_subscriptions, self.max_subscription_bytes,
                      self.pubsub_queue_bytes, self.pubsub_queue_frames):
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError("Pub/Sub limits must be positive integers")
