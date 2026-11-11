"""Central command metadata, arity validation and expected command errors."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..config import ServerConfig
from ..engine.store import Store, WrongTypeError
from ..ops.memory import OOM, AllocationRejected, MemoryLimitError
from ..protocol import Error, Reply, reply_size, serialize_reply
from ..resp3 import Resp3Value, encode, encoded_size, from_resp2

if TYPE_CHECKING:
    from ..pubsub.broker import Publication
    from ..pubsub.output import OutputFrame


class CommandError(Exception):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.payload = payload


@dataclass(frozen=True, slots=True)
class CommandResult:
    reply: Reply
    close: bool = False
    extra_replies: tuple[Reply, ...] = ()
    publications: tuple[Publication, ...] = ()
    output_queued: bool = False
    output_frame: OutputFrame | None = None
    resp3_reply: Resp3Value | None = None
    resp3_extras: tuple[Resp3Value, ...] = ()


def result_size(result: CommandResult, *, protocol: int = 2,
                config: ServerConfig | None = None) -> int:
    if protocol == 3:
        assert config is not None
        values = (result.resp3_reply or from_resp2(result.reply),
                  *(result.resp3_extras or tuple(map(from_resp2, result.extra_replies))))
        try:
            return sum(encoded_size(value, max_bytes=config.max_reply_bytes,
                                    max_depth=config.max_depth) for value in values)
        except ValueError:
            return config.max_reply_bytes + 1
    return reply_size(result.reply) + sum(map(reply_size, result.extra_replies))


def result_payload(result: CommandResult, *, protocol: int = 2,
                   config: ServerConfig | None = None) -> bytes:
    if protocol == 3:
        assert config is not None
        values = (result.resp3_reply or from_resp2(result.reply),
                  *(result.resp3_extras or tuple(map(from_resp2, result.extra_replies))))
        return b''.join(encode(value, max_bytes=config.max_reply_bytes,
                               max_depth=config.max_depth) for value in values)
    return serialize_reply(result.reply) + b''.join(map(serialize_reply, result.extra_replies))


Handler = Callable[[Store, tuple[bytes, ...], ServerConfig], Reply]


@dataclass(frozen=True, slots=True)
class Command:
    name: bytes
    min_args: int
    max_args: int | None
    handler: Handler | None
    step: int = 1
    writes: bool = False
    closes: bool = False
    queue_variadic: bool = False
    shape_error: bytes | None = None
    pubsub: bool = False

    def accepts(self, count: int) -> bool:
        return (count >= self.min_args
                and (self.max_args is None or count <= self.max_args)
                and (count - self.min_args) % self.step == 0)


class Registry:
    def __init__(self, commands: tuple[Command, ...]) -> None:
        self._commands = {command.name: command for command in commands}
        if len(self._commands) != len(commands):
            raise ValueError("duplicate command registration")

    @property
    def names(self) -> frozenset[bytes]:
        return frozenset(self._commands)

    def resolve(self, args: list[bytes], *, queued: bool = False) -> Command | Error:
        command = self._commands.get(args[0].upper()) if args else None
        if command is None:
            return Error(b"ERR unknown command")
        count = len(args) - 1
        valid = (count >= command.min_args and (command.max_args is None
                 or count <= command.max_args or queued and command.queue_variadic))
        if not queued:
            valid = valid and command.accepts(count)
        if not valid:
            if count >= command.min_args and not queued and command.shape_error is not None:
                return Error(command.shape_error)
            return Error(b"ERR wrong number of arguments for '"
                         + command.name.lower() + b"' command")
        return command

    def dispatch(self, args: list[bytes], store: Store, config: ServerConfig) -> CommandResult:
        command = self.resolve(args)
        if isinstance(command, Error):
            return CommandResult(command)
        if command.handler is None:
            raise RuntimeError("client command must be dispatched through the executor")
        try:
            reply = command.handler(store, tuple(args[1:]), config)
        except CommandError as exc:
            return CommandResult(Error(exc.payload))
        except AllocationRejected:
            store.allocation_rejections += 1
            return CommandResult(Error(OOM))
        except MemoryLimitError:
            return CommandResult(Error(OOM))
        except WrongTypeError:
            return CommandResult(Error(
                b"WRONGTYPE Operation against a key holding the wrong kind of value"))
        return CommandResult(reply, close=command.closes)
