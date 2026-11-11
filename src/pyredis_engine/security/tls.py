"""TLS contexts and private credential files; never log secrets or prompt for keys."""

from __future__ import annotations

import os
import ssl
import stat
from dataclasses import dataclass, field

from ..config import ServerConfig
from ..protocol import Array, Bulk, reply_size


def read_private(filename: str, *, maximum: int = 1024) -> bytes:
    try:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_BINARY", 0)
        )
        with os.fdopen(os.open(filename, flags), "rb") as stream:
            mode = os.fstat(stream.fileno()).st_mode
            if not stat.S_ISREG(mode) or os.name == "posix" and mode & 0o077:
                raise ValueError("invalid private file")
            value = stream.read(maximum + 1)
        if not 1 <= len(value) <= maximum:
            raise ValueError("invalid private file")
        return value
    except (OSError, ValueError):
        raise ValueError("invalid or unsafe private credential file") from None


def check_private_key(filename: str) -> None:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_BINARY", 0)
    )
    fd = os.open(filename, flags)
    try:
        mode = os.fstat(fd).st_mode
        if not stat.S_ISREG(mode) or os.name == "posix" and mode & 0o077:
            raise ValueError("invalid private key file")
    finally:
        os.close(fd)


@dataclass(slots=True, repr=False)
class TLSContexts:
    server: ssl.SSLContext | None = None
    upstream: ssl.SSLContext | None = None
    password: bytes | None = field(default=None, repr=False)

    @classmethod
    def create(cls, config: ServerConfig) -> TLSContexts:
        result = cls()
        try:
            if config.tls_cert_file is not None:
                server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                server.minimum_version = ssl.TLSVersion.TLSv1_2
                server.options |= ssl.OP_NO_COMPRESSION
                password = (
                    read_private(config.tls_key_password_file)
                    if config.tls_key_password_file
                    else b""
                )
                assert config.tls_key_file is not None
                check_private_key(config.tls_key_file)
                server.load_cert_chain(config.tls_cert_file, config.tls_key_file, password=password)
                if config.tls_ca_file is not None:
                    server.load_verify_locations(cafile=config.tls_ca_file)
                server.verify_mode = {
                    "none": ssl.CERT_NONE,
                    "optional": ssl.CERT_OPTIONAL,
                    "required": ssl.CERT_REQUIRED,
                }[config.tls_client_auth]
                result.server = server
            if config.replication_secure and config.replica_host is not None:
                assert config.tls_ca_file and config.replica_password_file
                upstream = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH, cafile=config.tls_ca_file
                )
                upstream.minimum_version = ssl.TLSVersion.TLSv1_2
                upstream.options |= ssl.OP_NO_COMPRESSION
                if config.replica_tls_cert_file is not None:
                    password = (
                        read_private(config.replica_tls_key_password_file)
                        if config.replica_tls_key_password_file
                        else b""
                    )
                    assert config.replica_tls_key_file is not None
                    check_private_key(config.replica_tls_key_file)
                    upstream.load_cert_chain(
                        config.replica_tls_cert_file, config.replica_tls_key_file, password=password
                    )
                result.upstream = upstream
                result.password = read_private(config.replica_password_file)
                assert config.replica_username is not None
                request = Array(
                    (
                        Bulk(b"AUTH"),
                        Bulk(config.replica_username.encode("ascii")),
                        Bulk(result.password),
                    )
                )
                if (
                    config.max_args < 3
                    or len(result.password) > config.max_bulk_bytes
                    or len(config.replica_username) > config.max_bulk_bytes
                    or reply_size(request) > config.max_frame_bytes
                ):
                    raise ValueError("replica credentials exceed protocol limits")
        except (OSError, ssl.SSLError, ValueError):
            raise ValueError("invalid or unsafe TLS/replication configuration") from None
        return result
