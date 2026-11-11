"""Bounded RESP2 connections sharing one ordered in-memory executor."""
from __future__ import annotations

import asyncio
import logging
import socket
import ssl
from collections.abc import Awaitable, Callable
from types import TracebackType

from .commands.lists import blocking_timeout
from .commands.registry import CommandError, CommandResult, result_payload
from .commands.stream_groups import group_read_spec
from .commands.streams import read_spec
from .config import ServerConfig
from .engine.clock import Clock
from .engine.executor import Executor
from .engine.waiters import Waiter
from .ops.metrics import Metrics
from .persistence.coordinator import Persistence
from .protocol import Array, Error, ProtocolError, read_command
from .pubsub.output import Outbox
from .replication.node import Replication
from .replication.recovery import RecoveryReplication
from .security.tls import TLSContexts
from .streams import BoundedReader as _BoundedReader
from .streams import close as _close
from .streams import send as _safe_send
from .streams import set_write_limits

logger = logging.getLogger(__name__)


class ClientLimiter:
    def __init__(self, max_clients: int) -> None:
        self.max_clients = max_clients
        self.count = 0

    def acquire(self) -> bool:
        # Called synchronously on a single asyncio event loop.
        if self.count >= self.max_clients:
            return False
        self.count += 1
        return True

    def release(self) -> None:
        self.count -= 1


async def _pubsub_writer(output: Outbox, writer: asyncio.StreamWriter,
                         config: ServerConfig, metrics: Metrics) -> None:
    try:
        while (frame := await output.get()) is not None:
            try:
                await _safe_send(writer, frame.data, config)
            finally:
                output.finish(frame)
    except TimeoutError:
        metrics.write_timeouts += 1
        output.fail()
    except OSError:
        output.fail()


async def serve_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    config: ServerConfig,
    limiter: ClientLimiter,
    executor: Executor,
    persistence: Persistence | None = None,
    replication: Replication | None = None,
    *, admitted: bool = False,
) -> None:
    if config.tls_cert_file is not None and writer.get_extra_info('ssl_object') is None:
        if admitted:
            limiter.release()
        writer.transport.abort()
        return
    set_write_limits(writer)
    if not admitted and not limiter.acquire():
        executor.metrics.connections_rejected += 1
        try:
            message = b"-ERR max number of clients reached\r\n"
            await _safe_send(writer, message, config)
        except (OSError, TimeoutError):
            pass
        finally:
            await _close(writer, config)
        return
    executor.metrics.connections_accepted += 1
    executor.metrics.connections_active += 1
    try:
        client = executor.new_client()
    except MemoryError:
        limiter.release()
        executor.metrics.connections_active -= 1
        executor.fail_allocation()
        await _close(writer, config)
        raise
    subscriber = client.subscriber
    assert subscriber is not None
    subscriber.on_close = writer.close
    subscriber.output.on_overflow = writer.transport.abort
    output_task: asyncio.Task[None] | None = None

    async def respond(result: CommandResult) -> bool:
        nonlocal output_task
        if not subscriber.output.active:
            await _safe_send(writer, result_payload(
                result, protocol=client.protocol_version, config=config), config)
            return True
        frame = (result.output_frame if result.output_queued else
                 subscriber.output.enqueue(result_payload(
                     result, protocol=client.protocol_version, config=config)))
        if output_task is None:
            output_task = asyncio.create_task(_pubsub_writer(
                subscriber.output, writer, config, executor.metrics),
                                               name='pyredis-pubsub-writer')
            def writer_done(task: asyncio.Task[None]) -> None:
                if not task.cancelled() and isinstance(task.exception(), MemoryError):
                    executor.fail_allocation()
                subscriber.output.fail()
            output_task.add_done_callback(writer_done)
        if frame is not None:
            await frame.done.wait()
        return not subscriber.output.failed

    async def await_blocked(waiter: Waiter) -> CommandResult | None:
        assert isinstance(reader, _BoundedReader)
        closed = asyncio.create_task(reader.closed_event.wait())
        initialized = asyncio.create_task(waiter.initialized.wait())
        try:
            while True:
                remaining = None if waiter.deadline is None or waiter.claimed else max(
                    0.0, waiter.deadline - asyncio.get_running_loop().time())
                if remaining is not None:
                    remaining = min(remaining, 3600.0)
                tasks = ((waiter.future, closed) if waiter.started else
                         (waiter.future, closed, initialized))
                done, _ = await asyncio.wait(tasks, timeout=remaining,
                                              return_when=asyncio.FIRST_COMPLETED)
                if closed in done:
                    if reader.failed:
                        executor.metrics.protocol_errors += 1
                    executor.waiters.cancel(waiter)
                    return None
                if waiter.future in done:
                    return waiter.future.result()
                if (waiter.deadline is not None and not waiter.claimed
                        and asyncio.get_running_loop().time() >= waiter.deadline):
                    result = CommandResult(Array(None))
                    executor.waiters.finish(waiter, result)
                    return result
        finally:
            closed.cancel()
            initialized.cancel()
            try:
                await closed
            except asyncio.CancelledError:
                pass
            try:
                await initialized
            except asyncio.CancelledError:
                pass
            if not waiter.future.done():
                executor.waiters.cancel(waiter)

    try:
        while True:
            try:
                if subscriber.output.failed:
                    break
                if subscriber.count and isinstance(reader, _BoundedReader):
                    await reader.wait_input()
                try:
                    async with asyncio.timeout(config.read_timeout):
                        args = await read_command(reader, config)
                except TimeoutError:
                    executor.metrics.read_timeouts += 1
                    break
                if args is None:
                    break
                if subscriber.output.failed:
                    break
                if args[0].upper() == b'HELLO':
                    result = await executor.hello_command(args, client)
                    if not await respond(result) or result.close:
                        break
                    continue
                if executor.access_error(args, client) is not None:
                    if not await respond(executor.execute(args, client)):
                        break
                    continue
                if args[0].upper() in (b'AUTH', b'ACL'):
                    result = await executor.security_command(args, client)
                    if not await respond(result) or result.close:
                        break
                    continue
                if (args[0].upper() == b'XREAD' and not client.multi
                        and (not subscriber.count or client.protocol_version == 3)):
                    try:
                        spec = read_spec(tuple(args[1:]), config, executor.store.now_ms)
                    except CommandError:
                        spec = None
                    if spec is not None and spec.block_ms is not None:
                        if (replication is not None and replication.is_replica
                                and replication.phase != 'up'):
                            if not await respond(await replication.execute(args, client)):
                                break
                            continue
                        assert isinstance(reader, _BoundedReader)
                        def replica_available() -> bool:
                            return (replication is not None and replication.phase == 'up'
                                    and not replication.closing)
                        available = (replica_available if replication is not None
                                     and replication.is_replica else None)
                        waiting = executor.waiters.register(args, client, spec.block_ms / 1000,
                            lambda: not reader.closed_event.is_set(), spec.keys, available)
                        if isinstance(waiting, Error):
                            if not await respond(CommandResult(waiting)):
                                break
                            continue
                        if persistence is None:
                            executor.waiters.drain_sync()
                        else:
                            persistence.schedule_waiters()
                        blocked_result = await await_blocked(waiting)
                        if (blocked_result is None or not await respond(blocked_result)
                                or blocked_result.close):
                            break
                        continue
                if (args[0].upper() == b'XREADGROUP' and not client.multi
                        and (not subscriber.count or client.protocol_version == 3)
                        and (replication is None or not replication.is_replica)):
                    try:
                        group_spec = group_read_spec(tuple(args[1:]), config, executor.store.now_ms)
                    except CommandError:
                        group_spec = None
                    if group_spec is not None and group_spec.block_ms is not None:
                        assert isinstance(reader, _BoundedReader)
                        waiting = executor.waiters.register(
                            args, client, group_spec.block_ms / 1000,
                            lambda: not reader.closed_event.is_set(), group_spec.keys)
                        if isinstance(waiting, Error):
                            if not await respond(CommandResult(waiting)):
                                break
                            continue
                        if persistence is None:
                            executor.waiters.drain_sync()
                        else:
                            persistence.schedule_waiters()
                        blocked_result = await await_blocked(waiting)
                        if (blocked_result is None or not await respond(blocked_result)
                                or blocked_result.close):
                            break
                        continue
                if (args[0].upper() in (b'BLPOP', b'BRPOP') and not client.multi
                        and (not subscriber.count or client.protocol_version == 3)
                        and not isinstance(executor.registry.resolve(args), Error)
                        and (replication is None or not replication.is_replica)):
                    try:
                        timeout = blocking_timeout(args[-1], executor.store.now_ms)
                    except CommandError as error:
                        if not await respond(CommandResult(Error(error.payload))):
                            break
                        continue
                    assert isinstance(reader, _BoundedReader)
                    waiting = executor.waiters.register(
                        args, client, timeout, lambda: not reader.closed_event.is_set())
                    if isinstance(waiting, Error):
                        if not await respond(CommandResult(waiting)):
                            break
                        continue
                    if persistence is None:
                        executor.waiters.drain_sync()
                    else:
                        persistence.schedule_waiters()
                    blocked_result = await await_blocked(waiting)
                    if (blocked_result is None or not await respond(blocked_result)
                            or blocked_result.close):
                        break
                    continue
                if (replication is not None and args[0].upper() == b'PYREDIS.SYNC'
                        and not subscriber.count):
                    if subscriber.output.active:
                        await respond(CommandResult(Error(
                            b'ERR replication requires a dedicated connection')))
                        break
                    await replication.serve(reader, writer, args, client)
                    break
                if replication is not None:
                    result = await replication.execute(args, client)
                else:
                    if persistence is None:
                        revision = executor.store.mutation_revision
                        result = executor.execute(args, client)
                        if executor.store.mutation_revision != revision:
                            executor.waiters.drain_sync()
                    else:
                        result = await persistence.execute(args, client)
                if not await respond(result) or result.close:
                    break
            except ProtocolError:
                executor.metrics.protocol_errors += 1
                try:
                    executor.pubsub.clear_subscriptions(subscriber)
                    await respond(CommandResult(Error(b'ERR Protocol error')))
                except (OSError, TimeoutError):
                    pass
                break
            except TimeoutError:
                executor.metrics.write_timeouts += 1
                break
            except OSError:
                break
            except MemoryError:
                executor.fail_allocation()
                logger.error('allocation failure; stopping server')
                writer.transport.abort()
                break
    finally:
        try:
            executor.close_client(client)
        finally:
            try:
                if output_task is not None:
                    output_task.cancel()
                    try:
                        await output_task
                    except asyncio.CancelledError:
                        pass
            finally:
                limiter.release()
                executor.metrics.connections_active -= 1
                await _close(writer, config)


class ManagedServer:
    """Own the listener, expiration worker and connection tasks as one lifecycle."""

    def __init__(
        self, server: asyncio.Server, executor: Executor, config: ServerConfig,
        clients: set[asyncio.StreamWriter], tasks: set[asyncio.Task[None]],
        expiry_wait: Callable[[float], Awaitable[None]],
        persistence: Persistence | None = None,
        replication: Replication | None = None,
    ) -> None:
        self._server = server
        self.executor = executor
        self._config = config
        self._clients = clients
        self._tasks = tasks
        self._expiry_wait = expiry_wait
        self._closing = False
        self.stopping = asyncio.Event()
        self.executor.on_failure = self.close
        self.persistence = persistence
        self.replication = replication
        if persistence is not None:
            persistence.on_failure = self.close
        self.expiration_task = asyncio.create_task(self._expire_loop(), name="pyredis-expiration")
        self.expiration_task.add_done_callback(self._worker_finished)
        if replication is not None:
            replication.start()
            if replication.task is not None:
                replication.task.add_done_callback(self._worker_finished)

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self._server.sockets

    @property
    def client_task_count(self) -> int:
        return len(self._tasks)

    def metrics(self) -> dict[str, int]:
        result = self.executor.metrics.snapshot()
        store, broker = self.executor.store, self.executor.pubsub
        result.update(memory_accounted_bytes=store.memory_bytes,
                      maxmemory_bytes=self._config.maxmemory_bytes,
                      memory_rejections=store.memory_rejections,
                      allocation_rejections=store.allocation_rejections,
                      keys_physical=store.physical_count, keys_expiring=store.expiration_count,
                      connections_owned=len(self._clients), client_tasks=len(self._tasks),
                      channels=len(broker.channels),
                      subscriptions=sum(client.count for client in broker.clients),
                      pubsub_pending_bytes=sum(client.output.pending_bytes
                                               for client in broker.clients),
                      pubsub_pending_frames=sum(client.output.pending_frames
                                                for client in broker.clients))
        return result

    def is_serving(self) -> bool:
        return self._server.is_serving()

    async def _expire_loop(self) -> None:
        while True:
            await self._expiry_wait(self._config.expiry_interval)
            if self.replication is not None and self.replication.is_replica:
                continue
            if self.persistence is None:
                if self.executor.expire_batch():
                    self.executor.waiters.drain_sync()
            else:
                await self.persistence.expire_batch()

    def _worker_finished(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            if isinstance(error, MemoryError):
                self.executor.fail_allocation()
            self.close()
            task.get_loop().call_exception_handler(
                {"message": "expiration worker failed; closing server", "exception": error})

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.stopping.set()
        self.executor.pubsub.close()
        self._server.close()
        if self.persistence is not None:
            self.persistence.begin_close()
        if self.replication is not None:
            self.replication.begin_close()
        self.expiration_task.cancel()
        for writer in tuple(self._clients):
            writer.close()

    async def wait_closed(self) -> None:
        try:
            try:
                await asyncio.wait_for(self._server.wait_closed(), self._config.write_timeout)
            except TimeoutError:
                for writer in tuple(self._clients):
                    writer.transport.abort()
                await asyncio.wait_for(self._server.wait_closed(), self._config.write_timeout)
            if self._tasks:
                tasks = tuple(self._tasks)
                _, pending = await asyncio.wait(tasks, timeout=self._config.shutdown_timeout)
                for task in pending:
                    task.cancel()
                for task in tasks:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
        finally:
            try:
                try:
                    await self.expiration_task
                except asyncio.CancelledError:
                    pass
            finally:
                try:
                    try:
                        if self.replication is not None:
                            await self.replication.close()
                    finally:
                        if self.persistence is not None:
                            await self.persistence.close()
                finally:
                    self.executor.close()

    async def serve_forever(self) -> None:
        try:
            await self._server.serve_forever()
        finally:
            self.close()
            await self.wait_closed()

    async def __aenter__(self) -> ManagedServer:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
        await self.wait_closed()


async def start_server(
    config: ServerConfig, *, clock: Clock | None = None,
    expiry_wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
    aof_sync_wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> ManagedServer:
    limiter = ClientLimiter(config.max_clients)
    tls = TLSContexts.create(config)
    executor = Executor(config, clock=clock)
    initialized = False
    try:
        persistence = (await Persistence.open(executor, config, aof_sync_wait)
                       if config.aof_path is not None else None)
        replication_type = RecoveryReplication if config.replication_recovery else Replication
        replication = (replication_type(persistence, config)
                       if config.replication_enabled and persistence is not None else None)
        if replication is not None:
            replication.tls = tls
        initialized = True
    finally:
        if not initialized:
            executor.close()
    clients: set[asyncio.StreamWriter] = set()
    tasks: set[asyncio.Task[None]] = set()
    rejections: set[asyncio.Task[None]] = set()

    def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        admitted = limiter.acquire()
        started = False
        if not admitted:
            if tls.server is not None:
                executor.metrics.connections_rejected += 1
                writer.transport.abort()
                return
            # Bound rejection work as well as admitted handlers. Excess arrivals
            # get immediate transport abort rather than another coroutine/buffer.
            if len(rejections) >= min(config.max_clients, 16):
                executor.metrics.connections_rejected += 1
                writer.transport.abort()
                return
            transport = writer.transport
            if isinstance(transport, asyncio.ReadTransport):
                transport.pause_reading()
        clients.add(writer)
        if admitted:
            if tls.server is not None:
                if not isinstance(writer.transport, asyncio.ReadTransport):
                    limiter.release()
                    clients.discard(writer)
                    writer.transport.abort()
                    return
                writer.transport.pause_reading()
            async def run_admitted() -> None:
                nonlocal started
                started = True
                handed_over = False
                try:
                    if tls.server is not None:
                        original_transport = writer.transport
                        try:
                            await writer.start_tls(tls.server,
                                ssl_handshake_timeout=config.tls_handshake_timeout)
                        except (ssl.SSLError, OSError, TimeoutError):
                            return
                        except AttributeError:
                            # Python 3.11/3.14 can replace the writer transport with
                            # None when a closing handshake returns no transport.
                            if getattr(writer, '_transport', None) is not None:
                                raise
                            writer.__dict__['_transport'] = original_transport
                            return
                    handed_over = True
                    await serve_client(reader, writer, config, limiter, executor,
                                       persistence, replication, admitted=True)
                finally:
                    if not handed_over:
                        limiter.release()
            task = asyncio.create_task(run_admitted())
        else:
            async def reject() -> None:
                executor.metrics.connections_rejected += 1
                try:
                    await _safe_send(writer, b'-ERR max number of clients reached\r\n', config)
                except (OSError, TimeoutError):
                    pass
                finally:
                    await _close(writer, config)
            task = asyncio.create_task(reject())
            rejections.add(task)
        tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            if admitted and not started:
                limiter.release()
            writer.close()
            clients.discard(writer)
            tasks.discard(completed)
            rejections.discard(completed)
            if not completed.cancelled() and (error := completed.exception()) is not None:
                if isinstance(error, MemoryError):
                    executor.fail_allocation()
                completed.get_loop().call_exception_handler(
                    {"message": "client task failed", "exception": error})

        task.add_done_callback(finished)

    def protocol_factory() -> asyncio.StreamReaderProtocol:
        reader = _BoundedReader(config)
        return asyncio.StreamReaderProtocol(
            reader, connected,
        )

    success = False
    try:
        server = await asyncio.get_running_loop().create_server(
            protocol_factory,
            host=config.host,
            port=config.port,
        )
        owner = ManagedServer(server, executor, config, clients, tasks, expiry_wait, persistence,
                              replication)
        success = True
        return owner
    finally:
        if not success:
            try:
                if persistence is not None:
                    await persistence.close()
            finally:
                executor.close()
