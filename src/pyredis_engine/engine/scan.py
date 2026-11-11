"""Connection-owned bounded iterators; mutations invalidate retained cursors."""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..commands.numbers import integer
from ..commands.registry import CommandError
from ..commands.scores import score_bytes
from ..protocol import INT64_MAX, Array, Bulk, Error, Reply, reply_size
from ..security.permissions import MatchBudget, Pattern
from .collections import HashValue, SetValue, SortedSetValue
from .store import WrongTypeError

if TYPE_CHECKING:
    from ..config import ServerConfig
    from ..security.policy import AccessControl
    from .store import Store
    from .transactions import ClientState

MAX_SCAN_STEPS = 1024
MAX_SCAN_KEY = 4096


@dataclass(slots=True)
class ScanState:
    name: bytes
    key: bytes | None
    items: Iterator[tuple[bytes, object]]
    cursor: int = 0
    last: bytes | None = None
    keyspace_revision: int = 0


class Scanner:
    def __init__(self, store: Store, config: ServerConfig, access: AccessControl) -> None:
        self.store, self.config, self.access = store, config, access
        self.active: set[ClientState] = set()
        self.serial = 0
        store.add_change_listener(self.changed)
        store.add_clear_listener(self.invalidate)

    def changed(self, key: bytes, revision: int) -> None:
        for client in tuple(self.active):
            state = client.scan
            if state is None:
                self.release(client)
            elif state.key is None and state.keyspace_revision != self.store._keyspace_revision:
                self.release(client)
            elif state.key == key:
                value, _ = self.store.physical_entry(key)
                if not ((state.name == b"HSCAN" and isinstance(value, HashValue))
                        or (state.name == b"SSCAN" and isinstance(value, SetValue))):
                    self.release(client)

    def invalidate(self) -> None:
        for client in self.active:
            client.scan = None
        self.active.clear()

    def release(self, client: ClientState) -> None:
        client.scan = None
        self.active.discard(client)

    def close(self) -> None:
        self.invalidate()
        self.store.remove_change_listener(self.changed)
        self.store.remove_clear_listener(self.invalidate)

    def run(self, args: list[bytes], client: ClientState) -> Reply:
        name = args[0].upper()
        key = None if name == b"SCAN" else args[1]
        if key is not None and len(key) > MAX_SCAN_KEY:
            return Error(b"ERR scan key exceeds configured limit")
        cursor_index = 1 if key is None else 2
        try:
            cursor = integer(args[cursor_index])
            if cursor < 0:
                raise CommandError(b"ERR invalid cursor")
        except CommandError:
            return Error(b"ERR invalid cursor")
        count, source, type_filter = 10, None, None
        position = cursor_index + 1
        while position < len(args):
            option = args[position].upper()
            if option not in (b"MATCH", b"COUNT", b"TYPE") or position + 1 >= len(args):
                return Error(b"ERR syntax error")
            token = args[position + 1]
            if option == b"MATCH":
                source = token
            elif option == b"TYPE" and key is None:
                type_filter = token.lower()
            elif option == b"COUNT":
                try:
                    count = integer(token)
                except CommandError:
                    return Error(b"ERR value is not an integer or out of range")
                if count < 1:
                    return Error(b"ERR syntax error")
            else:
                return Error(b"ERR syntax error")
            position += 2
        try:
            match = Pattern.compile(source) if source else None
        except ValueError:
            return Error(b"ERR invalid or oversized scan pattern")
        if cursor == 0:
            self.release(client)
            items: Iterator[tuple[bytes, object]]
            if key is None:
                items = iter(self.store._values.items())
            else:
                try:
                    collection = (
                        self.store.get_hash(key)
                        if name == b"HSCAN"
                        else self.store.get_set(key)
                        if name == b"SSCAN"
                        else self.store.get_sorted_set(key)
                    )
                except WrongTypeError:
                    return Error(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value"
                    )
                if collection is None:
                    return Array((Bulk(b"0"), Array(())))
                if isinstance(collection, HashValue):
                    items = iter(())
                elif isinstance(collection, SetValue):
                    items = iter(())
                else:
                    assert isinstance(collection, SortedSetValue)
                    # The rank list supports constant-time iterator advancement,
                    # unlike a hash table with an arbitrary number of tombstones.
                    items = ((member, score) for score, member in collection.ranked)
            state = ScanState(name, key, items, keyspace_revision=self.store._keyspace_revision)
            client.scan = state
            self.active.add(client)
        else:
            existing = client.scan
            if (
                existing is None
                or existing.cursor != cursor
                or existing.name != name
                or existing.key != key
            ):
                return Error(b"ERR invalid or invalidated cursor")
            state = existing
            if state.key is None and state.keyspace_revision != self.store._keyspace_revision:
                self.release(client)
                return Error(b"ERR invalid or invalidated cursor")
        if key is not None:
            value, deadline = self.store.physical_entry(key)
            if value is None or deadline is not None and self.store.now_ms > deadline:
                self.release(client)
                return Array((Bulk(b"0"), Array(())))
        if state.name in (b"HSCAN", b"SSCAN"):
            assert isinstance(value, (HashValue, SetValue))
            index = value.scan_index
            offset = 0 if state.last is None else bisect_right(index, state.last)
        replies: list[Reply] = []
        size = 64
        budget = MatchBudget()
        user = self.access.current(client)
        assert user is not None
        payload: object
        try:
            for _ in range(min(count, MAX_SCAN_STEPS)):
                if state.name in (b"HSCAN", b"SSCAN"):
                    if offset >= len(index):
                        self.release(client)
                        return Array((Bulk(b"0"), Array(tuple(replies))))
                    member = index[offset]
                    offset += 1
                    state.last = member
                    payload = value.fields[member] if isinstance(value, HashValue) else None
                else:
                    try:
                        member, payload = next(state.items)
                    except StopIteration:
                        self.release(client)
                        return Array((Bulk(b"0"), Array(tuple(replies))))
                    except RuntimeError:
                        self.release(client)
                        return Error(b"ERR invalid or invalidated cursor")
                if key is None:
                    if not any(self._match(pattern, member, budget) for pattern in user.keys):
                        continue
                    value, deadline = self.store.physical_entry(member)
                    if value is None or deadline is not None and self.store.now_ms > deadline:
                        continue
                    if type_filter is not None and value.kind != type_filter:
                        continue
                if source is not None and (
                    member != b"" if match is None else not self._match(match, member, budget)
                ):
                    continue
                parts = [Bulk(member)]
                if name == b"HSCAN":
                    assert isinstance(payload, bytes)
                    parts.append(Bulk(payload))
                elif name == b"ZSCAN":
                    assert isinstance(payload, float)
                    parts.append(Bulk(score_bytes(payload)))
                size += sum(map(reply_size, parts))
                if size > self.config.max_reply_bytes:
                    raise CommandError(b"ERR reply exceeds configured maximum size")
                replies.extend(parts)
        except CommandError as error:
            self.release(client)
            return Error(error.payload)
        self.serial = self.serial % INT64_MAX + 1
        state.cursor = self.serial
        return Array((Bulk(str(state.cursor).encode("ascii")), Array(tuple(replies))))

    @staticmethod
    def _match(pattern: Pattern, value: bytes, budget: MatchBudget) -> bool:
        cost = len(pattern.prefix) if pattern.prefix is not None else len(value)
        if cost > budget.remaining:
            raise CommandError(b"ERR scan pattern matching limit exceeded")
        return pattern.matches(value, budget)
