from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig


@pytest.mark.parametrize(
    "values",
    [
        {"tls_cert_file": "certificate.pem"},
        {"tls_key_file": "key.pem"},
        {"tls_client_auth": "required"},
        {"replication_secure": True},
        {"tls_ca_file": "ca.pem"},
        {"tls_client_auth": "invalid"},
        {"tls_handshake_timeout": 0},
        {"tls_key_password_file": "key-password"},
        {"replica_password_file": "password"},
        {"replication_client_cert_sha256": "00" * 32},
        {
            "tls_cert_file": "cert",
            "tls_key_file": "key",
            "replication_enabled": True,
            "snapshot_enabled": True,
            "aof_path": "data.aof",
        },
    ],
)
def test_incomplete_tls_and_secure_replication_configuration_is_rejected(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        ServerConfig(**values)  # type: ignore[arg-type]


def test_replica_requires_explicit_identity_ca_and_complete_credentials() -> None:
    base: dict[str, object] = {
        "aof_path": "data.aof",
        "snapshot_enabled": True,
        "replication_enabled": True,
        "replication_secure": True,
        "acl_file": "acl.json",
        "tls_cert_file": "cert",
        "tls_key_file": "key",
        "tls_ca_file": "ca",
        "replica_host": "127.0.0.1",
        "replica_port": 6380,
        "replica_username": "peer",
        "replica_password_file": "sensitive-password-path",
        "replica_server_name": "localhost",
    }
    config = ServerConfig(**base)  # type: ignore[arg-type]
    assert "sensitive-password-path" not in repr(config)
    for field in (
        "tls_ca_file",
        "replica_username",
        "replica_password_file",
        "replica_server_name",
    ):
        with pytest.raises(ValueError):
            ServerConfig(**(base | {field: None}))  # type: ignore[arg-type]
    for name in ("bad name", "*.example", "-invalid", "example..com"):
        with pytest.raises(ValueError):
            ServerConfig(**(base | {"replica_server_name": name}))  # type: ignore[arg-type]
