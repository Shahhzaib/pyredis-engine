"""Thread-confined client state, bounded queues and reverse WATCH registrations."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..commands.pubsub import restricted
from ..commands.registry import CommandResult, Registry
from ..config import ServerConfig
from ..protocol import Array, Error, Reply, Simple, reply_size
from ..resp3 import ArrayReply, Resp3Value, encoded_size, for_command
from .exec_stage import ExecStage
from .store import Store

if TYPE_CHECKING:
    from ..pubsub.broker import Broker, Publication, Subscriber
    from .scan import ScanState

OK = Simple(b"OK")
QUEUED = Simple(b"QUEUED")
ABORT = Error(b"EXECABORT Transaction discarded because of previous errors.")
REPLY_ABORT = Error(
    b"EXECABORT Transaction discarded because reply exceeds configured maximum size")


def revoked_command(denied: Error) -> CommandResult:
    reason = (b'no permission to touch the specified keys' if denied.payload.endswith(b'a key')
              else b'no permission to access one of the channels used as arguments'
              if denied.payload.endswith(b'a channel') else
              b'no permission to execute the command or subcommand')
    return CommandResult(Error(
        b'NOPERM ACLs rules changed between the moment the transaction was accumulated and the '
        b'EXEC call. This command is no longer allowed for the following reason: ' + reason))


@dataclass(eq=False)
class ClientState:
    owner: object
    multi: bool = False
    dirty: bool = False
    conflict: bool = False
    closed: bool = False
    queue: list[tuple[bytes, ...]] = field(default_factory=list)
    queued_bytes: int = 0
    watched: set[bytes] = field(default_factory=set)
    watch_bytes: int = 0
    subscriber: Subscriber | None = None
    username: bytes | None = None
    identity: object | None = field(default=None, repr=False)
    security_prepared: object | None = field(default=None, repr=False)
    auth_failures: int = 0
    replication_peer: bool = False
    client_id: int = 0
    protocol_version: int = 2
    client_name: bytes | None = field(default=None, repr=False)
    library_name: bytes | None = field(default=None, repr=False)
    library_version: bytes | None = field(default=None, repr=False)
    scan: ScanState | None = field(default=None, repr=False)


class Transactions:
    def __init__(self, store: Store, registry: Registry, config: ServerConfig, broker: Broker,
                 dispatch: Callable[[list[bytes], ClientState], CommandResult] | None = None,
                 authorize: Callable[[list[bytes], ClientState], Error | None] | None = None,
                 ) -> None:
        self.store, self.registry, self.config = store, registry, config
        self.broker = broker
        self.watchers: dict[bytes, set[ClientState]] = {}
        store.add_change_listener(self.changed)
        self.dispatch = dispatch or (lambda args, state: registry.dispatch(args, store, config))
        self.authorize = authorize or (lambda args, state: None)

    def denied(self, args: list[bytes], state: ClientState, error: Error) -> CommandResult:
        if state.multi:
            if args and args[0].upper() == b'EXEC':
                self.reset(state)
            else:
                state.dirty = True
        return CommandResult(error)

    def changed(self, key: bytes, revision: int) -> None:
        for state in self.watchers.get(key, ()):
            state.conflict = True

    def unwatch(self, state: ClientState) -> None:
        for key in state.watched:
            clients = self.watchers[key]
            clients.remove(state)
            if not clients:
                del self.watchers[key]
        state.watched.clear()
        state.watch_bytes = 0
        state.conflict = False

    def reset(self, state: ClientState) -> None:
        self.unwatch(state)
        state.queue.clear()
        state.queued_bytes = 0
        state.multi = state.dirty = False

    def close(self) -> None:
        for state in {client for clients in self.watchers.values() for client in clients}:
            self.reset(state)
        self.store.remove_change_listener(self.changed)

    def run(self, args: list[bytes], state: ClientState) -> CommandResult:
        if state.owner is not self or state.closed:
            raise RuntimeError("client state is closed or belongs to another executor")
        denied = self.authorize(args, state)
        if denied is not None:
            return self.denied(args, state, denied)
        command = self.registry.resolve(args, queued=state.multi)
        if isinstance(command, Error):
            if state.multi:
                if args and args[0].upper() == b"EXEC":
                    self.reset(state)
                    return CommandResult(Error(b"EXECABORT Transaction discarded because of: "
                                               + command.payload.removeprefix(b"ERR ")))
                state.dirty = True
            return CommandResult(command)
        name = command.name
        if (state.multi and name == b'HELLO' and len(args) > 1
                and (len(args) > 2 or args[1] != str(state.protocol_version).encode('ascii'))):
            state.dirty = True
            return CommandResult(Error(b'ERR protocol switching is not allowed in MULTI'))
        if state.multi and name in (b'AUTH', b'ACL', b'CLIENT', b'SCAN', b'HSCAN',
                                    b'SSCAN', b'ZSCAN'):
            state.dirty = True
            return CommandResult(Error(b'ERR ' + name + b' is not allowed in MULTI'))
        if (state.multi and state.protocol_version == 3
                and name in (b'SUBSCRIBE', b'UNSUBSCRIBE', b'PSUBSCRIBE', b'PUNSUBSCRIBE')):
            state.dirty = True
            return CommandResult(Error(b'ERR subscription commands are not allowed in MULTI'))
        if (state.protocol_version == 2 and state.subscriber is not None and state.subscriber.count
                and name not in (b'SUBSCRIBE', b'UNSUBSCRIBE', b'PSUBSCRIBE', b'PUNSUBSCRIBE',
                                 b'PING', b'QUIT')):
            return CommandResult(restricted(name))
        if name == b"MULTI":
            if state.multi:
                return CommandResult(Error(b"ERR MULTI calls can not be nested"))
            state.multi = True
            return CommandResult(OK)
        if name == b"WATCH":
            return CommandResult(self.watch(args[1:], state))
        if name == b"DISCARD":
            if not state.multi:
                return CommandResult(Error(b"ERR DISCARD without MULTI"))
            self.reset(state)
            return CommandResult(OK)
        if name == b"EXEC":
            return self.execute(state)
        if command.closes:
            self.reset(state)
            return self.dispatch(args, state)
        if state.multi:
            size = sum(len(arg) for arg in args)
            if (len(state.queue) >= self.config.max_transaction_commands
                    or size > self.config.max_transaction_bytes - state.queued_bytes):
                state.dirty = True
                return CommandResult(Error(
                    b"ERR transaction queue exceeds configured maximum size"))
            state.queue.append(tuple(args))
            state.queued_bytes += size
            return CommandResult(QUEUED)
        if name == b"UNWATCH":
            self.unwatch(state)
            return CommandResult(OK)
        return self.dispatch(args, state)

    def watch(self, keys: list[bytes], state: ClientState) -> Reply:
        if state.multi:
            return Error(b"ERR WATCH inside MULTI is not allowed")
        added = set(keys) - state.watched
        size = sum(map(len, added))
        if (len(state.watched) + len(added) > self.config.max_watched_keys
                or size > self.config.max_watch_bytes - state.watch_bytes):
            return Error(b"ERR watched keys exceed configured maximum size")
        for key in added:
            self.store.get(key)  # expire before registration, not after it
            self.watchers.setdefault(key, set()).add(state)
        state.watched.update(added)
        state.watch_bytes += size
        return OK

    def execute(self, state: ClientState) -> CommandResult:
        if not state.multi:
            return CommandResult(Error(b"ERR EXEC without MULTI"))
        if state.dirty:
            self.reset(state)
            return CommandResult(ABORT)
        denials = [self.authorize(list(command), state) for command in state.queue]
        if state.watched and self.authorize([b'WATCH', *state.watched], state) is not None:
            self.reset(state)
            return CommandResult(Error(
                b'EXECABORT Transaction discarded because of ACL permissions'))
        for key in state.watched:
            self.store.get(key)  # includes deadline passage without sweeper activity
        if state.dirty or state.conflict:
            aborted = ABORT if state.dirty else Array(None)
            self.reset(state)
            return CommandResult(aborted)
        commands = state.queue
        state.queue = []
        self.reset(state)  # own queued writes must not dirty this EXEC
        replies: list[Reply] = []
        replies3: list[Resp3Value] = []
        publications: list[Publication] = []
        size = len(str(len(commands))) + 3
        assert state.subscriber is not None
        with ExecStage(self.store, self.broker, state.subscriber) as staged:
            for args, denied in zip(commands, denials, strict=True):
                result = (revoked_command(denied) if denied is not None else
                          CommandResult(OK) if args[0].upper() == b"UNWATCH" else
                          self.dispatch(list(args), state))
                publications.extend(result.publications)
                for reply in (result.reply, *result.extra_replies):
                    if state.protocol_version == 3:
                        converted = for_command(args, reply)
                        try:
                            size += encoded_size(ArrayReply((converted,)),
                                                 max_bytes=self.config.max_reply_bytes,
                                                 max_depth=self.config.max_depth) - 4
                        except ValueError:
                            return CommandResult(REPLY_ABORT)
                        replies3.append(converted)
                    else:
                        size += reply_size(reply)
                    if size > self.config.max_reply_bytes:
                        return CommandResult(REPLY_ABORT)
                    replies.append(reply)
            count = len(commands)
            accepted = CommandResult(Array(tuple(replies[:count])),
                                     extra_replies=tuple(replies[count:]),
                                     publications=tuple(publications),
                                     resp3_reply=(ArrayReply(tuple(replies3[:count]))
                                                  if state.protocol_version == 3 else None),
                                     resp3_extras=tuple(replies3[count:]))
            staged.commit()
            return accepted
