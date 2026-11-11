"""Bounded connection-local waiters; mutations are performed by the executor."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..commands.registry import CommandResult
from ..commands.stream_groups import group_read_spec
from ..commands.streams import encode_id, parse_id, read_spec
from ..protocol import Array, Error
from .collections import ListValue, StreamValue

if TYPE_CHECKING:
    from .executor import Executor
    from .transactions import ClientState

MAX_WAITERS = 256
MAX_WAIT_KEYS = 64
MAX_WAIT_BYTES = 65536


@dataclass(eq=False)
class Waiter:
    args: list[bytes]
    client: ClientState
    future: asyncio.Future[CommandResult]
    timeout: float
    alive: Callable[[], bool]
    keys: tuple[bytes, ...]
    available: Callable[[], bool] | None = None
    deadline: float | None = None
    initialized: asyncio.Event = field(default_factory=asyncio.Event)
    claimed: bool = False
    abandoned: bool = False
    started: bool = False


class Waiters:
    def __init__(self, executor: Executor) -> None:
        self.executor = executor
        self.pending: deque[Waiter] = deque()

    @property
    def count(self) -> int:
        return len(self.pending)

    def register(
        self,
        args: list[bytes],
        client: ClientState,
        timeout: float,
        alive: Callable[[], bool],
        keys: tuple[bytes, ...] | None = None,
        available: Callable[[], bool] | None = None,
    ) -> Waiter | Error:
        keys = tuple(args[1:-1]) if keys is None else keys
        if (
            len(self.pending) >= min(MAX_WAITERS, self.executor._config.max_clients)
            or len(keys) > MAX_WAIT_KEYS
            or sum(map(len, keys)) > MAX_WAIT_BYTES
            or args[0].upper() == b"XREADGROUP"
            and sum(map(len, args)) > MAX_WAIT_BYTES
        ):
            return Error(b"ERR blocking waiter limits exceeded")
        loop = asyncio.get_running_loop()
        waiter = Waiter(args.copy(), client, loop.create_future(), timeout, alive, keys, available)
        self.pending.append(waiter)
        return waiter

    def remove(self, waiter: Waiter) -> None:
        try:
            self.pending.remove(waiter)
        except ValueError:
            pass

    def finish(self, waiter: Waiter, result: CommandResult) -> None:
        self.remove(waiter)
        waiter.claimed = False
        if not waiter.future.done():
            waiter.future.set_result(result)

    def cancel(self, waiter: Waiter) -> None:
        waiter.abandoned = True
        if not waiter.claimed:
            self.remove(waiter)
            waiter.future.cancel()

    def close_client(self, client: ClientState) -> None:
        for waiter in tuple(self.pending):
            if waiter.client is client:
                self.cancel(waiter)

    def revalidate(self) -> None:
        for waiter in tuple(self.pending):
            if not waiter.claimed and not waiter.client.closed:
                denied = self.executor.access_error(waiter.args, waiter.client)
                if denied is not None:
                    self.finish(waiter, CommandResult(denied))

    def replica_unavailable(self) -> None:
        for waiter in tuple(self.pending):
            if waiter.available is not None and not waiter.claimed:
                self.finish(waiter, CommandResult(Error(b"MASTERDOWN Replication link is down")))

    def candidates(self) -> tuple[Waiter, ...]:
        return tuple(self.pending)

    def claim(self, waiter: Waiter) -> bool:
        if waiter not in self.pending or waiter.claimed:
            return False
        if waiter.abandoned or waiter.client.closed or not waiter.alive():
            self.cancel(waiter)
            return False
        if waiter.available is not None and not waiter.available():
            self.finish(waiter, CommandResult(Error(b"MASTERDOWN Replication link is down")))
            return False
        if (
            waiter.started
            and waiter.deadline is not None
            and asyncio.get_running_loop().time() >= waiter.deadline
        ):
            self.finish(waiter, CommandResult(Array(None)))
            return False
        if not waiter.started and waiter.args[0].upper() == b"XREAD":
            spec = read_spec(tuple(waiter.args[1:]), self.executor._config)
            for index, (key, raw) in enumerate(zip(spec.keys, spec.ids, strict=True)):
                if raw == b"$":
                    value, expiry = self.executor.store.physical_entry(key)
                    identity = (
                        value.last_id
                        if isinstance(value, StreamValue)
                        and (expiry is None or self.executor.store.now_ms <= expiry)
                        else (0, 0)
                    )
                    waiter.args[spec.ids_offset + index + 1] = encode_id(identity)
        if waiter.started:
            ready = False
            group_spec = (
                group_read_spec(tuple(waiter.args[1:]), self.executor._config)
                if waiter.args[0].upper() == b"XREADGROUP"
                else None
            )
            ready_spec = (
                read_spec(tuple(waiter.args[1:]), self.executor._config)
                if waiter.args[0].upper() == b"XREAD"
                else None
            )
            for index, key in enumerate(waiter.keys):
                value, deadline = self.executor.store.physical_entry(key)
                live = deadline is None or self.executor.store.now_ms <= deadline
                if group_spec is not None:
                    group = (
                        next(
                            (group for group in value.groups if group.name == group_spec.group),
                            None,
                        )
                        if live and isinstance(value, StreamValue)
                        else None
                    )
                    if (
                        group is None
                        or group_spec.ids[index] != b">"
                        or isinstance(value, StreamValue)
                        and value.entries
                        and value.entries[-1].id > group.last_id
                    ):
                        ready = True
                        break
                    continue
                if live and (
                    isinstance(value, ListValue)
                    and value.items
                    and ready_spec is None
                    or isinstance(value, StreamValue)
                    and value.entries
                    and ready_spec is not None
                    and value.entries[-1].id > parse_id(ready_spec.ids[index])
                ):
                    ready = True
                    break
            if not ready:
                return False
        waiter.claimed = True
        return True

    def resolved(self, waiter: Waiter, result: CommandResult) -> None:
        waiter.claimed = False
        if waiter.abandoned or waiter.client.closed:
            self.cancel(waiter)
        elif result.reply == Array(None):
            if not waiter.started:
                waiter.started = True
                if waiter.timeout:
                    waiter.deadline = asyncio.get_running_loop().time() + waiter.timeout
                waiter.initialized.set()
            if waiter.deadline is not None and asyncio.get_running_loop().time() >= waiter.deadline:
                self.finish(waiter, CommandResult(Array(None)))
        else:
            self.finish(waiter, result)

    def drain_sync(self) -> None:
        for waiter in self.candidates():
            if self.claim(waiter):
                result = self.executor.execute(waiter.args, waiter.client)
                self.resolved(waiter, result)
