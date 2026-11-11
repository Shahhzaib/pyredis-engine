"""One synchronous, event-loop-thread-owned command path per server."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from threading import get_ident
from typing import TYPE_CHECKING

from ..commands import default_registry
from ..commands.connection import dispatch as connection_dispatch
from ..commands.connection import hello_options
from ..commands.registry import Command, CommandResult, Registry, result_payload, result_size
from ..config import ServerConfig
from ..ops.memory import OOM, AllocationRejected, MemoryLimitError, preparing
from ..ops.metrics import Metrics
from ..persistence.errors import CapacityError
from ..protocol import INT64_MIN, Array, Bulk, Error
from ..pubsub.broker import Broker
from ..resp3 import for_command
from ..security.commands import SecurityCommands
from ..security.policy import AccessControl
from .clock import Clock
from .exec_stage import ExecStage
from .scan import Scanner
from .store import Store
from .transactions import ClientState, Transactions
from .waiters import Waiters

if TYPE_CHECKING:
    from ..persistence.codec import Group


class Executor:
    def __init__(
        self, config: ServerConfig, *, store: Store | None = None, clock: Clock | None = None,
    ) -> None:
        self._config = config
        if store is not None and clock is not None:
            raise ValueError("inject the clock into the supplied store")
        self._store = store if store is not None else Store(
            clock=clock, maxmemory_bytes=config.maxmemory_bytes)
        self._store.maxmemory_bytes = config.maxmemory_bytes
        self._store._check_budget(self._store.memory_bytes)
        self._registry = default_registry()
        self._owner_thread = get_ident()
        self.pubsub = Broker(config)
        self.metrics = Metrics()
        self.failed = False
        self.on_failure: Callable[[], None] = lambda: None
        self.access = AccessControl(self._registry, config.acl_file, config.replication_secure)
        self.scanner = Scanner(self._store, config, self.access)
        self.security = SecurityCommands(
            self, self.access, config.replication_enabled and not config.replication_secure)
        self._clients: set[ClientState] = set()
        self.waiters = Waiters(self)
        self._next_client_id = 0
        self._transactions = Transactions(
            self._store, self._registry, config, self.pubsub, self._dispatch, self.access.authorize)
        self._default_client = self.new_client()

    def new_client(self) -> ClientState:
        self._assert_owner()
        user = self.access.default()
        self._next_client_id += 1
        state = ClientState(self._transactions, subscriber=self.pubsub.client(),
                            username=user.name if user else None,
                            identity=user.identity if user else None,
                            client_id=self._next_client_id)
        self._clients.add(state)
        return state

    def close_client(self, state: ClientState) -> None:
        self._assert_owner()
        if state.owner is not self._transactions:
            raise RuntimeError("client belongs to another executor")
        self._transactions.reset(state)
        self.waiters.close_client(state)
        self.scanner.release(state)
        if state.subscriber is not None:
            self.pubsub.detach(state.subscriber)
        state.closed = True
        state.username, state.identity, state.security_prepared = None, None, None
        self._clients.discard(state)

    def close(self) -> None:
        for state in tuple(self._clients):
            self.close_client(state)
        self._transactions.close()
        self.scanner.close()
        self.pubsub.close()
        self.security.worker.close()

    def refresh_acl(self, affected: set[bytes], *, deleted: bool = False) -> None:
        self._assert_owner()
        for client in tuple(self._clients):
            if (client.username in affected
                    and (deleted or client.replication_peer
                         or not self.security.channels_allowed(client))):
                self.close_client(client)
        self.waiters.revalidate()

    def access_error(self, args: list[bytes], client: ClientState) -> Error | None:
        self._assert_owner()
        if client.owner is not self._transactions or client.closed:
            raise RuntimeError('invalid client state')
        return self.access.authorize(args, client)

    async def security_command(self, args: list[bytes], client: ClientState) -> CommandResult:
        """Prepare credentials off-thread, then recheck/install on the execution thread."""
        self._assert_owner()
        resolved = self._registry.resolve(args)
        if (self.access_error(args, client) is not None or isinstance(resolved, Error)
                or client.multi or client.subscriber is not None and client.subscriber.count):
            return self.execute(args, client)
        prepare = (args[0].upper() == b'AUTH' or len(args) >= 3
                   and args[0].upper() == b'ACL' and args[1].upper() == b'SETUSER')
        try:
            if prepare:
                client.security_prepared = await self.security.prepare(args.copy(), client)
            if client.closed:
                return CommandResult(Error(b'NOAUTH Authentication required.'), close=True)
            return self.execute(args, client)
        except MemoryError:
            self.fail_allocation()
            raise
        finally:
            client.security_prepared = None

    async def hello_command(self, args: list[bytes], client: ClientState) -> CommandResult:
        options = hello_options(args, client.protocol_version)
        if isinstance(options, Error):
            return CommandResult(options)
        version, auth, client_name = options
        if auth is None:
            return self.execute(args, client)
        if client.multi or client.subscriber is not None and client.subscriber.count:
            return CommandResult(Error(b'ERR HELLO AUTH is not allowed in this state'))
        clean = [b'HELLO', str(version).encode('ascii')]
        if client_name is not None:
            clean.extend((b'SETNAME', client_name))
        auth_args = [b'AUTH', *auth]
        try:
            proof = await self.security.prepare(auth_args, client)
            if client.closed:
                return CommandResult(Error(b'NOAUTH Authentication required.'), close=True)
            client.security_prepared = proof
            assert client.subscriber is not None
            staged = replace(client, watched=set(), queue=[],
                             subscriber=replace(client.subscriber))
            result = self.security.dispatch(auth_args, staged)
            if isinstance(result.reply, Error) or result.close:
                # Apply normal failure counting and connection-close policy to the real client.
                return self.execute(auth_args, client)
            result = self.execute(clean, staged, defer_pubsub=True)
            if isinstance(result.reply, Error) or result.close:
                return result
            authenticated = self.security.dispatch(auth_args, client)
            if isinstance(authenticated.reply, Error) or authenticated.close:
                return authenticated
            client.protocol_version = staged.protocol_version
            client.subscriber.protocol_version = staged.protocol_version
            client.client_name = staged.client_name
            return self.complete(result, client)
        except MemoryError:
            self.fail_allocation()
            raise
        finally:
            client.security_prepared = None

    @property
    def watched_key_count(self) -> int:
        return len(self._transactions.watchers)

    @property
    def registry(self) -> Registry:
        return self._registry

    def discard_transaction(self, client: ClientState) -> None:
        self._assert_owner()
        if client.owner is not self._transactions or client.closed:
            raise RuntimeError("invalid client state")
        self._transactions.reset(client)

    def restore_group(self, group: Group) -> None:
        """Validated effects at startup or under the replication commit lock."""
        self._assert_owner()
        try:
            self._store.check_batch({entry.key: entry.value for entry in group.entries},
                                    clear=group.clear)
        except MemoryLimitError as error:
            raise CapacityError('restored dataset exceeds maxmemory accounting budget') from error
        try:
            with preparing():
                indexes = {entry.key: self._store.prepare_scan_index(entry.value)
                           for entry in group.entries}
        except AllocationRejected as error:
            raise CapacityError('unable to prepare restored scan indexes') from error
        with self._store.at_time(INT64_MIN), self._store.memory_batch(
                {entry.key: entry.value for entry in group.entries}, clear=group.clear):
            if group.clear:
                self._store.clear()
            for entry in group.entries:
                if entry.value is None:
                    self._store.delete(entry.key)
                else:
                    self._store.put(entry.key, entry.value, expires_at_ms=entry.expires_at_ms,
                                    _prepared_scan_index=indexes[entry.key])

    def install_state(self, staged: Executor) -> None:
        self._assert_owner()
        staged._assert_owner()
        self._store.install_state(staged.store)

    def execute(self, args: list[bytes], client: ClientState | None = None, *,
                defer_pubsub: bool = False) -> CommandResult:
        self._assert_owner()
        self.metrics.commands += 1
        if self.failed:
            return CommandResult(Error(OOM), close=True)
        try:
            result = self._execute(args, client, defer_pubsub=defer_pubsub)
        except MemoryError:
            self.fail_allocation()
            raise
        self.metrics.command_errors += int(isinstance(result.reply, Error))
        return result

    def fail_allocation(self) -> None:
        if not self.failed:
            self.failed = True
            self.metrics.allocation_failures += 1
            self.on_failure()

    def _execute(self, args: list[bytes], client: ClientState | None = None, *,
                 defer_pubsub: bool = False) -> CommandResult:
        # No await or I/O may enter this path: one event loop applies each whole
        # command before another client can run. Cross-thread use is rejected.
        self._assert_owner()
        target = client or self._default_client
        with self._store.at_time(self._store.clock.now_ms()):
            result = self._transactions.run(args, target)
        if result_size(result, protocol=target.protocol_version,
                       config=self._config) > self._config.max_reply_bytes:
            result = CommandResult(Error(b"ERR reply exceeds configured maximum size"),
                                   publications=result.publications)
        return result if defer_pubsub else self.complete(result, client or self._default_client)

    def _dispatch(self, args: list[bytes], client: ClientState) -> CommandResult:
        resolved = self._registry.resolve(args)
        if (client.protocol_version == 3 and self._store._exec_stage is None
                and isinstance(resolved, Command) and resolved.writes):
            assert client.subscriber is not None
            with ExecStage(self._store, self.pubsub, client.subscriber) as stage:
                result = self._dispatch_converted(args, client)
                if (result_size(result, protocol=3, config=self._config)
                        > self._config.max_reply_bytes):
                    return CommandResult(Error(b'ERR reply exceeds configured maximum size'))
                stage.commit()
                return result
        return self._dispatch_converted(args, client)

    def _dispatch_converted(self, args: list[bytes], client: ClientState) -> CommandResult:
        result = self._dispatch_raw(args, client)
        if client.protocol_version == 3:
            result = replace(result, resp3_reply=for_command(args, result.reply),
                             resp3_extras=tuple(for_command(args, reply)
                                                for reply in result.extra_replies))
        return result

    def _dispatch_raw(self, args: list[bytes], client: ClientState) -> CommandResult:
        subscriber = client.subscriber
        assert subscriber is not None
        name = args[0].upper()
        if name in (b'HELLO', b'CLIENT', b'SELECT'):
            return CommandResult(connection_dispatch(args, client, self._config))
        if name in (b'SCAN', b'HSCAN', b'SSCAN', b'ZSCAN'):
            return CommandResult(self.scanner.run(args, client))
        if name == b'XREADGROUP' and self._store._exec_stage is None:
            with ExecStage(self._store, self.pubsub, subscriber) as stage:
                result = self._registry.dispatch(args, self._store, self._config)
                if not isinstance(result.reply, Error):
                    stage.commit()
                return result
        if name in (b'AUTH', b'ACL'):
            return self.security.dispatch(args, client)
        if name == b'SUBSCRIBE':
            return self.pubsub.subscribe(subscriber, tuple(args[1:]))
        if name == b'UNSUBSCRIBE':
            return self.pubsub.unsubscribe(subscriber, tuple(args[1:]))
        if name == b'PSUBSCRIBE':
            return self.pubsub.psubscribe(subscriber, tuple(args[1:]))
        if name == b'PUNSUBSCRIBE':
            return self.pubsub.punsubscribe(subscriber, tuple(args[1:]))
        if name == b'PUBLISH':
            return self.pubsub.publish(args[1], args[2])
        if name == b'PING' and subscriber.count and client.protocol_version == 2:
            return CommandResult(Array((Bulk(b'pong'), Bulk(args[1] if len(args) == 2 else b''))))
        return self._registry.dispatch(args, self._store, self._config)

    def complete(self, result: CommandResult, client: ClientState) -> CommandResult:
        """Release transient effects after the configured local commit barrier."""
        self._assert_owner()
        subscriber = client.subscriber
        assert subscriber is not None
        frame = None
        if subscriber.output.active and not result.output_queued:
            frame = subscriber.output.enqueue(result_payload(
                result, protocol=client.protocol_version, config=self._config))
        for publication in result.publications:
            for target in publication.recipients:
                payload = publication.payload
                if target.protocol_version == 3:
                    payload = b'>' + payload[1:]
                target.output.enqueue(payload)
        if subscriber.output.active or result.publications:
            return replace(result, publications=(), output_queued=subscriber.output.active,
                           output_frame=frame or result.output_frame)
        return result

    @property
    def store(self) -> Store:
        return self._store

    def expire_batch(self, max_key_bytes: int | None = None) -> int:
        self._assert_owner()
        with self._store.at_time(self._store.clock.now_ms()):
            return self._store.expire_batch(self._config.expiry_batch_size, max_key_bytes)

    def _assert_owner(self) -> None:
        if get_ident() != self._owner_thread:
            raise RuntimeError("executor must run on its owning event-loop thread")
