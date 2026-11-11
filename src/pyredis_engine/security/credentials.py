"""Salted password verifiers and an interactive, hash-only bootstrap file writer."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

ITERATIONS = 600_000
MAX_PASSWORD_BYTES = 1024


@dataclass(frozen=True, slots=True, repr=False)
class PasswordHash:
    salt: bytes
    digest: bytes

    def __repr__(self) -> str:
        return "<PasswordHash redacted>"

    @classmethod
    def create(cls, password: bytes) -> PasswordHash:
        if not 1 <= len(password) <= MAX_PASSWORD_BYTES:
            raise ValueError("invalid password length")
        salt = secrets.token_bytes(16)
        return cls(salt, hashlib.pbkdf2_hmac("sha256", password, salt, ITERATIONS))

    def verify(self, password: bytes) -> bool:
        candidate = hashlib.pbkdf2_hmac("sha256", password, self.salt, ITERATIONS)
        return hmac.compare_digest(candidate, self.digest)

    def export(self) -> str:
        return f"pbkdf2-sha256${ITERATIONS}${self.salt.hex()}${self.digest.hex()}"

    @classmethod
    def parse(cls, value: str) -> PasswordHash:
        parts = value.split("$")
        if (
            len(parts) != 4
            or parts[:2] != ["pbkdf2-sha256", str(ITERATIONS)]
            or len(parts[2]) != 32
            or len(parts[3]) != 64
        ):
            raise ValueError("invalid password verifier")
        try:
            salt, digest = bytes.fromhex(parts[2]), bytes.fromhex(parts[3])
        except ValueError:
            raise ValueError("invalid password verifier") from None
        if len(salt) != 16 or len(digest) != 32:
            raise ValueError("invalid password verifier")
        return cls(salt, digest)


def main() -> None:
    import argparse
    import getpass
    import json
    import os
    import warnings

    parser = argparse.ArgumentParser(description="Create a private hash-only ACL bootstrap file")
    parser.add_argument("output", help="new file; never overwrite an existing file")
    parser.add_argument("--user", default="admin")
    args = parser.parse_args()
    from .policy import valid_username

    try:
        username = args.user.encode("ascii")
        if not valid_username(username) or username == b"default":
            raise ValueError("invalid administrator name")
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            password = getpass.getpass("Password: ").encode("utf-8")
            if password != getpass.getpass("Confirm password: ").encode("utf-8"):
                raise ValueError("password confirmation did not match")
        verifier = PasswordHash.create(password)
        payload = json.dumps(
            {
                "version": 1,
                "users": [
                    {
                        "name": args.user,
                        "rules": ["on", "+@all", "~*", "&*"],
                        "passwords": [verifier.export()],
                    }
                ],
            }
        ).encode("utf-8")
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
    except (OSError, UnicodeError, ValueError, getpass.GetPassWarning):
        parser.exit(
            1, "Could not create ACL bootstrap file; check input and private destination.\n"
        )
    print("Created private ACL bootstrap file.")


if __name__ == "__main__":
    main()
