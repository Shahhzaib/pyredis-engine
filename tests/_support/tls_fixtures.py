"""Generate throwaway test certificates with an already installed OpenSSL executable."""

from __future__ import annotations

import subprocess
from pathlib import Path


def generate(directory: Path) -> None:
    directory = directory.resolve()
    if any(directory.iterdir()):
        raise ValueError("certificate directory must be empty")

    def run(*args: str) -> None:
        subprocess.run(
            ["openssl", *args],
            cwd=directory,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
        )

    run(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-sha256",
        "-days",
        "2",
        "-subj",
        "/CN=PyRedis Test CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem",
    )
    run(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-sha256",
        "-days",
        "2",
        "-subj",
        "/CN=Other Test CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        "other-ca.key",
        "-out",
        "other-ca.pem",
    )
    for name, extension in [
        ("server", "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"),
        ("client", "extendedKeyUsage=clientAuth\n"),
        ("other-client", "extendedKeyUsage=clientAuth\n"),
    ]:
        run(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-subj",
            f"/CN={name}",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
        )
        (directory / f"{name}.ext").write_text(
            extension + "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "authorityKeyIdentifier=keyid,issuer\n",
            encoding="ascii",
        )
        ca = "other-ca" if name == "other-client" else "ca"
        run(
            "x509",
            "-req",
            "-sha256",
            "-days",
            "2",
            "-in",
            f"{name}.csr",
            "-CA",
            f"{ca}.pem",
            "-CAkey",
            f"{ca}.key",
            "-CAcreateserial",
            "-out",
            f"{name}.pem",
            "-extfile",
            f"{name}.ext",
        )
    run(
        "pkey",
        "-in",
        "server.key",
        "-aes-256-cbc",
        "-passout",
        "pass:synthetic-key-only",
        "-out",
        "server-encrypted.key",
    )
    run(
        "pkey",
        "-in",
        "client.key",
        "-aes-256-cbc",
        "-passout",
        "pass:synthetic-key-only",
        "-out",
        "client-encrypted.key",
    )
    (directory / "key-password").write_bytes(b"synthetic-key-only")
    (directory / "key-password").chmod(0o600)
