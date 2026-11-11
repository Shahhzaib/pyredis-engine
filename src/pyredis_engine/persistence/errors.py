class JournalError(Exception):
    """Persistence could not safely continue; do not acknowledge further writes."""


class CorruptJournal(JournalError):
    """Complete record/header invalid; automatic tail repair is forbidden."""


class TornTail(JournalError):
    """Incomplete final record after a verified prefix; explicit repair required."""


class CapacityError(JournalError):
    """Configured record/state limits cannot represent this effect group."""


class SourceCorruption(CorruptJournal):
    """The currently authoritative snapshot/journal is no longer trustworthy."""


class ReplicaDivergence(JournalError):
    """Local state no longer matches the replica's claimed in-memory boundary."""
