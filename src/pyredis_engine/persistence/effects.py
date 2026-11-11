"""Capture immutable-at-encoding final effects without changing live WATCH events."""

from __future__ import annotations

from collections.abc import Callable
from types import TracebackType

from ..commands.bitmaps import field_plan, offset
from ..commands.hyperloglog import SIZE as HLL_SIZE
from ..commands.registry import CommandError, Registry
from ..config import ServerConfig
from ..engine.collections import StreamValue
from ..engine.store import Store
from ..engine.transactions import ClientState
from ..protocol import Error
from ..security.permissions import command_keys
from .codec import Entry, Group, entry_size
from .errors import CapacityError


class Capture:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.keys: set[bytes] = set()
        self.clear = False

    def changed(self, key: bytes, revision: int) -> None:
        if not self.store.clearing:
            self.keys.add(key)

    def cleared(self) -> None:
        self.keys.clear()
        self.clear = True

    def __enter__(self) -> Capture:
        self.store.add_change_listener(self.changed)
        self.store.add_clear_listener(self.cleared)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.store.remove_change_listener(self.changed)
        self.store.remove_clear_listener(self.cleared)

    def group(self) -> Group:
        return Group(
            self.clear,
            tuple(Entry(key, *self.store.physical_entry(key)) for key in sorted(self.keys)),
        )


class Dataset:
    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self.sizes: dict[bytes, int] = {}
        self.bytes = 0

    def apply(self, group: Group) -> None:
        size = 0 if group.clear else self.bytes
        count = 0 if group.clear else len(self.sizes)
        for entry in group.entries:
            old = 0 if group.clear else self.sizes.get(entry.key, 0)
            new = 0 if entry.value is None else entry_size(entry)
            size += new - old
            count += int(new != 0) - int(old != 0)
        if size > self.config.aof_max_dataset_bytes or count > self.config.aof_max_keys:
            raise CapacityError("AOF dataset exceeds configured limits")
        if group.clear:
            self.sizes.clear()
        for entry in group.entries:
            if entry.value is None:
                self.sizes.pop(entry.key, None)
            else:
                self.sizes[entry.key] = entry_size(entry)
        self.bytes = size

    def preflight(
        self,
        args: list[bytes],
        client: ClientState,
        registry: Registry,
        authorize: Callable[[list[bytes], ClientState], Error | None] | None = None,
        store: Store | None = None,
    ) -> None:
        # Queuing mutates only client state. Estimate the entire eventual EXEC,
        # excluding already-doomed batches so EXECABORT/WATCH precedence survives.
        name = args[0].upper() if args else b""
        # These new connection effects cannot change or expire datastore keys.
        if not client.multi and name in (b"SUBSCRIBE", b"UNSUBSCRIBE", b"PUBLISH"):
            return
        if client.multi and name != b"EXEC":
            return
        commands = (
            [list(command) for command in client.queue]
            if name == b"EXEC"
            and len(args) == 1
            and client.multi
            and not client.dirty
            and not client.conflict
            else [args]
        )
        if authorize is not None:
            commands = [command for command in commands if authorize(command, client) is None]
        estimated: dict[bytes, int] = {}
        stream_additions: dict[bytes, int] = {}
        cleared = False
        reserve = 128 + sum(len(key) + 32 for key in client.watched)
        growth = {
            b"SET",
            b"MSET",
            b"APPEND",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"HSET",
            b"LPUSH",
            b"RPUSH",
            b"SADD",
            b"ZADD",
            b"GEOADD",
            b"XADD",
        }
        for command_args in commands:
            reserve += sum(map(len, command_args)) + 32 * len(command_args)
            command = registry.resolve(command_args, queued=True)
            if isinstance(command, Error) or not command.writes:
                continue
            if command.name == b"XADD" and len(command_args) > 1:
                key = command_args[1]
                stream_additions[key] = stream_additions.get(key, 0) + 1
            if command.name == b"FLUSHDB" and len(command_args) == 1:
                cleared = True
                estimated.clear()
                continue
            if command.name in (b'PFADD', b'PFMERGE') and len(command_args) > 1:
                key = command_args[1]
                old = estimated.get(key, 0 if cleared else self.sizes.get(key, 0))
                changes = [(key, max(old, 18 + len(key) + HLL_SIZE))]
                replacement = True
            elif command.name in (b'SETBIT', b'BITFIELD') and len(command_args) > 1:
                key = command_args[1]
                extent = 0
                try:
                    if command.name == b'SETBIT' and len(command_args) == 4:
                        extent = offset(command_args[2]) // 8 + 1
                    elif command.name == b'BITFIELD':
                        plan = field_plan(tuple(command_args[1:]))
                        extent = max(((start + width + 7) // 8
                                      for op, width, _, start, _, _ in plan if op != b'GET'),
                                     default=0)
                except CommandError:
                    extent = 0
                if extent > self.config.max_bulk_bytes:
                    extent = 0
                old = estimated.get(key, 0 if cleared else self.sizes.get(key, 0))
                changes = [(key, max(old, 18 + len(key) + extent) if extent else old)]
                replacement = True
            elif command.name == b'BITOP' and len(command_args) >= 4:
                key = command_args[2]
                source = max((estimated.get(item, 0 if cleared else self.sizes.get(item, 0))
                              for item in command_args[3:]), default=0)
                changes = [(key, source + len(key) + 32 if source else 0)]
                replacement = True
            elif command.name == b"MSET":
                pairs = zip(command_args[1::2], command_args[2::2], strict=False)
                changes = [(key, 18 + len(key) + len(value)) for key, value in pairs]
                replacement = len(command_args) % 2 == 1 and all(
                    len(value) <= self.config.max_bulk_bytes for value in command_args[2::2]
                )
            elif command.name == b"DEL":
                changes = [(key, 0) for key in command_args[1:]]
                replacement = False
            elif command.name in (b"BLPOP", b"BRPOP"):
                changes = [(key, 0) for key in command_args[1:-1]]
                replacement = False
            elif command.name == b"XGROUP":
                keys = command_keys(command_args)
                cost = sum(map(len, command_args)) + 32 * len(command_args) + 128
                changes = [(key, cost) for key in keys]
                replacement = False
            elif command.name == b"XREADGROUP":
                keys = command_keys(command_args)
                if store is None:
                    raise CapacityError("stream group preflight requires store state")
                consumer = command_args[3] if len(command_args) > 3 else b""
                changes = []
                for key in keys:
                    value, _ = store.physical_entry(key)
                    count = (len(value.entries) if isinstance(value, StreamValue) else 0)
                    count += stream_additions.get(key, 0)
                    cost = (
                        count * (36 + len(consumer))
                        + 128
                        + 2 * len(consumer)
                        + sum(map(len, command_args))
                    )
                    changes.append((key, cost))
                replacement = False
            elif command.name == b"XCLAIM":
                keys = command_keys(command_args)
                consumer = command_args[3] if len(command_args) > 3 else b""
                count = max(0, len(command_args) - 5)
                cost = (
                    128 + len(consumer) + count * (36 + len(consumer)) + sum(map(len, command_args))
                )
                changes = [(key, cost) for key in keys]
                replacement = False
            elif command.name == b"XAUTOCLAIM":
                keys = command_keys(command_args)
                if store is None:
                    raise CapacityError("stream group preflight requires store state")
                consumer = command_args[3] if len(command_args) > 3 else b""
                changes = []
                for key in keys:
                    value, _ = store.physical_entry(key)
                    count = min(len(value.entries), 1024) if isinstance(value, StreamValue) else 0
                    cost = (
                        128
                        + len(consumer)
                        + count * (36 + len(consumer))
                        + sum(map(len, command_args))
                    )
                    changes.append((key, cost))
                replacement = False
            elif (
                command.name == b"SET"
                and len(command_args) == 3
                and len(command_args[2]) <= self.config.max_bulk_bytes
            ):
                changes = [(command_args[1], 18 + len(command_args[1]) + len(command_args[2]))]
                replacement = True
            elif len(command_args) > 1:
                cost = (
                    sum(map(len, command_args)) + 32 * len(command_args)
                    if command.name in growth
                    else 0
                )
                changes = [(command_args[1], cost)]
                replacement = False
            else:
                changes = []
                replacement = False
            for key, cost in changes:
                old = estimated.get(key, 0 if cleared else self.sizes.get(key, 0))
                estimated[key] = (
                    0 if command.name == b"DEL" else cost if replacement else old + cost
                )
        size = 0 if cleared else self.bytes
        count = 0 if cleared else len(self.sizes)
        for key, new in estimated.items():
            old = 0 if cleared else self.sizes.get(key, 0)
            size += new - old
            count += int(new != 0) - int(old != 0)
        record = reserve + sum(value or len(key) + 14 for key, value in estimated.items())
        if (
            record > self.config.aof_max_record_bytes
            or size > self.config.aof_max_dataset_bytes
            or count > self.config.aof_max_keys
        ):
            raise CapacityError("persistence effect group exceeds configured limits")
