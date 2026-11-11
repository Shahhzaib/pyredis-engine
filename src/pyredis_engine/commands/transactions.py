"""Client command metadata; execution belongs to the serialized executor."""
from .registry import Command

COMMANDS = (
    Command(b"MULTI", 0, 0, None), Command(b"EXEC", 0, 0, None),
    Command(b"DISCARD", 0, 0, None), Command(b"WATCH", 1, None, None),
    Command(b"UNWATCH", 0, 0, None),
)
