"""Synchronous EXEC savepoint: observers see only a reply-admitted batch."""
from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..pubsub.broker import Broker, Subscriber
    from .store import Store, StoredValue


class ExecStage:
    def __init__(self, store: Store, broker: Broker, subscriber: Subscriber) -> None:
        self.store, self.broker, self.subscriber = store, broker, subscriber
        self.before: dict[bytes, tuple[StoredValue | None, int | None, int, int]] = {}
        self.events: list[tuple[bytes | None, int, bool]] = []
        self.sequence, self.memory = store.mutation_revision, store.memory_bytes
        self.channels = subscriber.channels.copy()
        self.patterns = subscriber.patterns.copy()
        self.channel_bytes = subscriber.channel_bytes
        self.output_active = subscriber.output.active
        self.admitted = False

    def __enter__(self) -> ExecStage:
        if self.store._exec_stage is not None:
            raise RuntimeError('nested EXEC staging')
        self.store._exec_stage = self
        return self

    def remember(self, key: bytes) -> None:
        if key not in self.before:
            self.before[key] = (*self.store.physical_entry(key),
                                self.store._revisions.get(key, 0),
                                self.store._memory_sizes.get(key, 0))

    def commit(self) -> None:
        # From here, unexpected observer failures must fail-stop as before.
        self.admitted = True
        self.store._exec_stage = None
        try:
            for key, revision, clearing in self.events:
                self.store.clearing = clearing
                if key is None:
                    for listener in tuple(self.store._clear_listeners):
                        listener()
                else:
                    self.store._notify(key, revision)
        finally:
            self.store.clearing = False

    def _discard(self) -> None:
        store = self.store
        for key, (value, deadline, revision, cost) in self.before.items():
            if value is None:
                store._values.pop(key, None)
                store._memory_sizes.pop(key, None)
            else:
                store._values[key] = value
                store._memory_sizes[key] = cost
            if deadline is None:
                store._expiration.discard(key)
            else:
                store._expiration.set(key, deadline)
            if revision:
                store._revisions[key] = revision
            else:
                store._revisions.pop(key, None)
        store._sequence, store.memory_bytes = self.sequence, self.memory
        self.broker.clear_subscriptions(self.subscriber)
        self.subscriber.channels = self.channels
        self.subscriber.patterns = self.patterns
        self.subscriber.channel_bytes = self.channel_bytes
        self.subscriber.output.active = self.output_active
        for channel in self.channels:
            self.broker.channels.setdefault(channel, set()).add(self.subscriber)
        for pattern in self.patterns:
            self.broker.restore_pattern(self.subscriber, pattern)

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 traceback: TracebackType | None) -> None:
        self.store._exec_stage = None
        if not self.admitted:
            self._discard()
