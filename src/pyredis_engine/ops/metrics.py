"""Fixed-cardinality counters; no keys, messages or per-client labels retained."""
from dataclasses import asdict, dataclass


@dataclass(slots=True)
class Metrics:
    connections_accepted: int = 0
    connections_rejected: int = 0
    connections_active: int = 0
    commands: int = 0
    command_errors: int = 0
    protocol_errors: int = 0
    read_timeouts: int = 0
    write_timeouts: int = 0
    allocation_failures: int = 0

    def snapshot(self) -> dict[str, int]:
        return asdict(self)
