"""Bounded RESP2 connection identity and handshake commands."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .. import __version__
from ..config import ServerConfig
from ..protocol import Array, Bulk, Error, Integer, Reply, Simple, reply_size
from ..resp3 import encoded_size, for_command
from .numbers import integer
from .registry import Command, CommandError

if TYPE_CHECKING:
    from ..engine.transactions import ClientState


def hello_options(args: list[bytes], current: int) -> tuple[int, tuple[bytes, bytes] | None,
                                                             bytes | None] | Error:
    version = current
    if len(args) > 1:
        try:
            version = integer(args[1])
        except CommandError:
            return Error(b"ERR Protocol version is not an integer or out of range")
        if version not in (2, 3):
            return Error(b"NOPROTO unsupported protocol version")
    auth: tuple[bytes, bytes] | None = None
    client_name: bytes | None = None
    pos = 2
    while pos < len(args):
        option = args[pos].upper()
        if option == b'AUTH' and pos + 2 < len(args) and auth is None:
            auth = (args[pos + 1], args[pos + 2])
            pos += 3
        elif option == b'SETNAME' and pos + 1 < len(args) and client_name is None:
            client_name = args[pos + 1]
            if len(client_name) > 128 or any(byte < 33 or byte > 126 for byte in client_name):
                return Error(b'ERR Client names cannot contain spaces, newlines or '
                             b'special characters.')
            pos += 2
        else:
            return Error(b'ERR syntax error')
    return version, auth, client_name


def dispatch(args: list[bytes], client: ClientState, config: ServerConfig) -> Reply:
    name = args[0].upper()
    if name == b"SELECT":
        try:
            db = integer(args[1])
        except CommandError as error:
            return Error(error.payload)
        return Simple(b"OK") if db == 0 else Error(b"ERR DB index is out of range")
    if name == b"HELLO":
        options = hello_options(args, client.protocol_version)
        if isinstance(options, Error):
            return options
        version, auth, client_name = options
        if auth is not None:
            return Error(b'ERR authentication service unavailable')
        reply = Array(
            (
                Bulk(b"server"),
                Bulk(b"pyredis"),
                Bulk(b"version"),
                Bulk(__version__.encode("ascii")),
                Bulk(b"proto"),
                Integer(version),
                Bulk(b"id"),
                Integer(client.client_id),
                Bulk(b"mode"),
                Bulk(b"standalone"),
                Bulk(b"role"),
                Bulk(b"slave" if config.replica_host else b"master"),
                Bulk(b"modules"),
                Array(()),
            )
        )
        try:
            size = (encoded_size(for_command(args, reply), max_bytes=config.max_reply_bytes,
                                 max_depth=config.max_depth)
                    if version == 3 else reply_size(reply))
        except ValueError:
            return Error(b'ERR reply exceeds configured maximum size')
        if size > config.max_reply_bytes:
            return Error(b'ERR reply exceeds configured maximum size')
        client.protocol_version = version
        if client.subscriber is not None:
            client.subscriber.protocol_version = version
        if client_name is not None:
            client.client_name = client_name or None
        return reply
    sub = args[1].upper()
    if sub == b"GETNAME" and len(args) == 2:
        return Bulk(client.client_name)
    if sub == b"ID" and len(args) == 2:
        return Integer(client.client_id)
    if sub == b"SETNAME" and len(args) == 3:
        value = args[2]
        if len(value) > 128 or any(byte < 33 or byte > 126 for byte in value):
            return Error(b"ERR Client names cannot contain spaces, newlines or special characters.")
        client.client_name = value or None
        return Simple(b"OK")
    if sub == b"SETINFO" and len(args) == 4 and args[2].upper() in (b"LIB-NAME", b"LIB-VER"):
        value = args[3]
        if len(value) > 128 or any(byte < 33 or byte > 126 for byte in value):
            return Error(b"ERR invalid client metadata")
        if args[2].upper() == b"LIB-NAME":
            client.library_name = value
        else:
            client.library_version = value
        return Simple(b"OK")
    return Error(b"ERR unsupported CLIENT subcommand or arguments")


COMMANDS = (
    Command(b"HELLO", 0, None, None),
    Command(b"CLIENT", 1, None, None),
    Command(b"SELECT", 1, 1, None),
)
