"""Synthetic credentials and private bootstrap files; never real deployment secrets."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pyredis_engine.security.credentials import PasswordHash

PASSWORD = b"synthetic-E01A-only\x00\xff"


@lru_cache(maxsize=1)
def verifier() -> PasswordHash:
    return PasswordHash.create(PASSWORD)


def bootstrap(path: Path, application_rules: list[str] | None = None) -> str:
    users = [
        {
            "name": "admin",
            "rules": ["on", "+@all", "~*", "&*"],
            "passwords": [verifier().export()],
        }
    ]
    if application_rules is not None:
        users.append(
            {"name": "app", "rules": application_rules, "passwords": [verifier().export()]}
        )
    path.write_text(json.dumps({"version": 1, "users": users}), encoding="utf-8")
    path.chmod(0o600)
    return str(path)
