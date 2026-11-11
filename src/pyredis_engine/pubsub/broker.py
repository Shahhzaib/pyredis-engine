"""Single-executor channel index and post-commit publication recipients."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial

from ..commands.registry import CommandResult
from ..config import ServerConfig
from ..protocol import Array, Bulk, Error, Integer, Reply, reply_size, serialize_reply
from ..security.permissions import MatchBudget, Pattern
from .output import Outbox


@dataclass(eq=False)
class Subscriber:
    output: Outbox
    protocol_version: int = 2
    channels: dict[bytes, None] = field(default_factory=dict)
    patterns: dict[bytes, None] = field(default_factory=dict)
    channel_bytes: int = 0
    closed: bool = False
    on_close: Callable[[], None] = lambda: None

    @property
    def count(self) -> int:
        return len(self.channels) + len(self.patterns)


@dataclass(frozen=True)
class Publication:
    recipients: tuple[Subscriber, ...]
    payload: bytes


def acknowledgement(kind: bytes, channel: bytes | None, count: int) -> Array:
    return Array((Bulk(kind), Bulk(channel), Integer(count)))


class Broker:
    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self.channels: dict[bytes, set[Subscriber]] = {}
        self.patterns: dict[bytes, tuple[Pattern | None, set[Subscriber]]] = {}
        self.clients: set[Subscriber] = set()

    def client(self) -> Subscriber:
        subscriber = Subscriber(Outbox(self.config))
        subscriber.output.on_failure = partial(self.detach, subscriber)
        self.clients.add(subscriber)
        return subscriber

    def _remove(self, subscriber: Subscriber, channel: bytes) -> None:
        if channel not in subscriber.channels:
            return
        del subscriber.channels[channel]
        subscriber.channel_bytes -= len(channel)
        listeners = self.channels[channel]
        listeners.remove(subscriber)
        if not listeners:
            del self.channels[channel]

    def clear_subscriptions(self, subscriber: Subscriber) -> None:
        for channel in tuple(subscriber.channels):
            self._remove(subscriber, channel)
        for pattern in tuple(subscriber.patterns):
            self._remove_pattern(subscriber, pattern)

    def _remove_pattern(self, subscriber: Subscriber, pattern: bytes) -> None:
        if pattern not in subscriber.patterns:
            return
        del subscriber.patterns[pattern]
        subscriber.channel_bytes -= len(pattern)
        listeners = self.patterns[pattern][1]
        listeners.remove(subscriber)
        if not listeners:
            del self.patterns[pattern]

    def restore_pattern(self, subscriber: Subscriber, pattern: bytes) -> None:
        if pattern not in self.patterns:
            self.patterns[pattern] = (Pattern.compile(pattern) if pattern else None, set())
        self.patterns[pattern][1].add(subscriber)

    def detach(self, subscriber: Subscriber) -> None:
        if subscriber.closed:
            return
        subscriber.closed = True
        self.clear_subscriptions(subscriber)
        self.clients.discard(subscriber)
        subscriber.output.fail()
        subscriber.on_close()

    def close(self) -> None:
        for subscriber in tuple(self.clients):
            self.detach(subscriber)

    def _confirm(self, replies: list[Reply]) -> CommandResult:
        if sum(map(reply_size, replies)) > self.config.max_reply_bytes:
            return CommandResult(Error(b'ERR reply exceeds configured maximum size'))
        return CommandResult(replies[0], extra_replies=tuple(replies[1:]))

    def subscribe(self, subscriber: Subscriber, channels: tuple[bytes, ...]) -> CommandResult:
        added = set(channels) - subscriber.channels.keys()
        if (subscriber.count + len(added) > self.config.max_subscriptions
                or sum(map(len, added)) > self.config.max_subscription_bytes
                - subscriber.channel_bytes):
            return CommandResult(Error(b'ERR subscription limits exceeded'))
        preview = set(subscriber.channels)
        replies: list[Reply] = []
        for channel in channels:
            preview.add(channel)
            replies.append(acknowledgement(b'subscribe', channel,
                                           len(preview) + len(subscriber.patterns)))
        result = self._confirm(replies)
        if isinstance(result.reply, Error):
            return result
        for channel in channels:
            if channel not in subscriber.channels:
                subscriber.channels[channel] = None
                subscriber.channel_bytes += len(channel)
                self.channels.setdefault(channel, set()).add(subscriber)
        subscriber.output.active = True
        return result

    def unsubscribe(self, subscriber: Subscriber, channels: tuple[bytes, ...]) -> CommandResult:
        names = channels or tuple(subscriber.channels) or (None,)
        preview = set(subscriber.channels)
        replies: list[Reply] = []
        for channel in names:
            if channel is not None:
                preview.discard(channel)
            replies.append(acknowledgement(b'unsubscribe', channel,
                                           len(preview) + len(subscriber.patterns)))
        result = self._confirm(replies)
        if isinstance(result.reply, Error):
            return result
        for channel in names:
            if channel is not None:
                self._remove(subscriber, channel)
        subscriber.output.active = True
        return result

    def psubscribe(self, subscriber: Subscriber, patterns: tuple[bytes, ...]) -> CommandResult:
        added = set(patterns) - subscriber.patterns.keys()
        new_global = added - self.patterns.keys()
        if (subscriber.count + len(added) > self.config.max_subscriptions
                or sum(map(len, added)) > self.config.max_subscription_bytes
                - subscriber.channel_bytes
                or len(self.patterns) + len(new_global) > self.config.max_subscriptions
                or sum(map(len, new_global)) > 4096):
            return CommandResult(Error(b'ERR subscription limits exceeded'))
        try:
            compiled = {source: Pattern.compile(source) if source else None
                        for source in new_global}
        except ValueError:
            return CommandResult(Error(b'ERR invalid or oversized subscription pattern'))
        preview = set(subscriber.patterns)
        replies: list[Reply] = []
        for pattern in patterns:
            preview.add(pattern)
            replies.append(acknowledgement(b'psubscribe', pattern,
                                           len(preview) + len(subscriber.channels)))
        result = self._confirm(replies)
        if isinstance(result.reply, Error):
            return result
        for pattern in patterns:
            if pattern not in subscriber.patterns:
                subscriber.patterns[pattern] = None
                subscriber.channel_bytes += len(pattern)
                if pattern not in self.patterns:
                    self.patterns[pattern] = (compiled[pattern], set())
                self.patterns[pattern][1].add(subscriber)
        subscriber.output.active = True
        return result

    def punsubscribe(self, subscriber: Subscriber, patterns: tuple[bytes, ...]) -> CommandResult:
        names = patterns or tuple(subscriber.patterns) or (None,)
        preview = set(subscriber.patterns)
        replies: list[Reply] = []
        for pattern in names:
            if pattern is not None:
                preview.discard(pattern)
            replies.append(acknowledgement(b'punsubscribe', pattern,
                                           len(preview) + len(subscriber.channels)))
        result = self._confirm(replies)
        if isinstance(result.reply, Error):
            return result
        for pattern in names:
            if pattern is not None:
                self._remove_pattern(subscriber, pattern)
        subscriber.output.active = True
        return result

    def publish(self, channel: bytes, message: bytes) -> CommandResult:
        reply = Array((Bulk(b'message'), Bulk(channel), Bulk(message)))
        if reply_size(reply) > self.config.max_reply_bytes:
            return CommandResult(Error(b'ERR Pub/Sub message exceeds configured reply size'))
        recipients = tuple(self.channels.get(channel, ()))
        events = [Publication(recipients, serialize_reply(reply))] if recipients else []
        total = reply_size(reply) if recipients else 0
        total_limit = max(self.config.max_reply_bytes, self.config.pubsub_queue_bytes)
        count = len(recipients)
        budget = MatchBudget()
        for source, (pattern, subscribers) in self.patterns.items():
            cost = 0 if pattern is None else len(pattern.prefix) if pattern.prefix is not None \
                else len(channel)
            if cost > budget.remaining:
                return CommandResult(Error(b'ERR Pub/Sub pattern matching limit exceeded'))
            matched = channel == b'' if pattern is None else pattern.matches(channel, budget)
            if matched:
                notice = Array((Bulk(b'pmessage'), Bulk(source), Bulk(channel), Bulk(message)))
                if reply_size(notice) > self.config.max_reply_bytes:
                    return CommandResult(Error(
                        b'ERR Pub/Sub message exceeds configured reply size'))
                total += reply_size(notice)
                if total > total_limit:
                    return CommandResult(Error(b'ERR Pub/Sub publication exceeds configured limit'))
                targets = tuple(subscribers)
                events.append(Publication(targets, serialize_reply(notice)))
                count += len(targets)
        return CommandResult(Integer(count), publications=tuple(events))
