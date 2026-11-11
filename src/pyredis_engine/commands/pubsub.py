"""Pub/Sub metadata; handlers need per-connection state in the executor."""
from ..protocol import Error
from .registry import Command


def restricted(name: bytes) -> Error:
    return Error(b"ERR Can't execute '" + name.lower()
        + b"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET"
          b" are allowed in this context")

COMMANDS = (
    Command(b'SUBSCRIBE', 1, None, None, pubsub=True),
    Command(b'UNSUBSCRIBE', 0, None, None, pubsub=True),
    Command(b'PSUBSCRIBE', 1, None, None, pubsub=True),
    Command(b'PUNSUBSCRIBE', 0, None, None, pubsub=True),
    Command(b'PUBLISH', 2, 2, None, pubsub=True),
)
