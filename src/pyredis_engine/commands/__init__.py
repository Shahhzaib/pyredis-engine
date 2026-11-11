"""The implemented command set; unsupported commands are never registered."""
from ..security.commands import COMMANDS as SECURITY_COMMANDS
from . import (
    bitmaps,
    connection,
    expiration,
    geo,
    hashes,
    hyperloglog,
    keyspace,
    lists,
    pubsub,
    sets,
    smoke,
    sorted_sets,
    stream_groups,
    streams,
    strings,
    transactions,
)
from .registry import Registry


def default_registry() -> Registry:
    return Registry(smoke.COMMANDS + connection.COMMANDS + strings.COMMANDS + bitmaps.COMMANDS
                    + hyperloglog.COMMANDS
                    + geo.COMMANDS
                    + keyspace.COMMANDS
                    + expiration.COMMANDS
                    + hashes.COMMANDS + lists.COMMANDS + sets.COMMANDS + sorted_sets.COMMANDS
                    + streams.COMMANDS
                    + stream_groups.COMMANDS
                    + transactions.COMMANDS + pubsub.COMMANDS + SECURITY_COMMANDS)
