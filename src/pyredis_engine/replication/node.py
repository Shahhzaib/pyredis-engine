"""One full-sync connection, an ordered effect stream, and explicit link state."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import secrets
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import BinaryIO

from ..commands.pubsub import restricted
from ..commands.registry import CommandResult
from ..config import ServerConfig
from ..engine.transactions import ClientState
from ..persistence.codec import decode_group
from ..persistence.coordinator import Persistence
from ..persistence.errors import JournalError
from ..persistence.journal import _write_all
from ..protocol import (
    Array,
    Bulk,
    Error,
    ProtocolError,
    Simple,
    read_value,
    reply_size,
    serialize_reply,
)
from ..security.tls import TLSContexts
from ..streams import BoundedReader, close, send, set_write_limits
from .wire import MAX_OFFSET, OVERHEAD, REQUEST, ReplicationError, SnapshotInfo, packet, receive

logger = logging.getLogger(__name__)
READONLY = Error(b"READONLY You can't write against a read only replica.")


def _open_snapshot(path: Path, owned: list[BinaryIO]) -> BinaryIO:
    stream = path.open('rb')
    owned.append(stream)
    return stream


@dataclass(eq=False)
class Session:
    writer: asyncio.StreamWriter
    config: ServerConfig
    queue: asyncio.Queue[bytes] = field(default_factory=asyncio.Queue)
    pending_bytes: int = 0
    pending_frames: int = 0
    active: bool = False
    failed: bool = False
    base: int = 0
    authorized: Callable[[], bool] = lambda: True

    def push(self, frame: bytes) -> None:
        if not self.active or self.failed:
            return
        if (len(frame) > self.config.replication_queue_bytes - self.pending_bytes
                or self.pending_frames >= self.config.replication_queue_frames):
            self.failed = True
            self.writer.close()
            logger.warning('replica disconnected: pending effect queue exceeded limit')
            return
        self.pending_bytes += len(frame)
        self.pending_frames += 1
        self.queue.put_nowait(frame)


class Replication:
    def __init__(self, persistence: Persistence, config: ServerConfig) -> None:
        self.persistence, self.config = persistence, config
        self.is_replica = config.replica_host is not None
        self.replication_id = b'' if self.is_replica else secrets.token_hex(20).encode('ascii')
        self.offset = 0
        self.phase = 'connecting' if self.is_replica else 'primary'
        self.last_error = ''
        self.changed = asyncio.Event()
        self.sessions: set[Session] = set()
        self.task: asyncio.Task[None] | None = None
        self.upstream: asyncio.StreamWriter | None = None
        self.closing = False
        self.tls = TLSContexts()
        if self.is_replica:
            persistence.executor.store.expire_locally = False
        else:
            persistence.on_commit = self.publish

    def publish(self, payload: bytes) -> None:
        end = self.offset + OVERHEAD + len(payload)
        if end > MAX_OFFSET:
            raise JournalError('replication offset exhausted')
        self.offset = end
        if any(s.active and not s.failed for s in self.sessions):
            frame = packet(b'E', end, payload)
            for session in tuple(self.sessions):
                session.push(frame)
        self.changed.set()

    def start(self) -> None:
        if self.is_replica:
            self.task = asyncio.create_task(self._follow(), name='pyredis-replica')

    async def wait_ready(self, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            while self.phase not in ('up', 'primary'):
                if self.phase == 'down':
                    raise ReplicationError(self.last_error or 'replication link is down')
                self.changed.clear()
                await self.changed.wait()

    async def wait_offset(self, offset: int, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            while self.offset < offset:
                if self.phase == 'down':
                    raise ReplicationError(self.last_error or 'replication link is down')
                self.changed.clear()
                await self.changed.wait()

    def info(self) -> Bulk:
        fields = [(b'role', b'slave' if self.is_replica else b'master'),
                  (b'master_replid', self.replication_id),
                  (b'master_repl_offset' if not self.is_replica else b'slave_repl_offset',
                   str(self.offset).encode())]
        if self.is_replica:
            fields.extend([(b'master_host', (self.config.replica_host or '').encode('ascii')),
                           (b'master_port', str(self.config.replica_port).encode()),
                           (b'master_link_status', b'up' if self.phase == 'up' else b'down'),
                           (b'master_sync_in_progress',
                            b'1' if self.phase in ('receiving', 'catching_up') else b'0')])
        else:
            fields.append((b'connected_slaves', str(sum(s.active and not s.failed
                                                       for s in self.sessions)).encode()))
        return Bulk(b'# Replication\r\n' + b''.join(k + b':' + v + b'\r\n' for k, v in fields))

    async def execute(self, args: list[bytes], client: ClientState) -> CommandResult:
        if self.persistence.executor.access_error(args, client) is not None:
            return self.persistence.executor.execute(args, client)
        name = args[0].upper()
        if client.subscriber is not None and client.subscriber.count:
            if name == b'INFO':
                return CommandResult(restricted(name))
            return await self.persistence.execute(args, client)
        if name == b'INFO':
            if len(args) != 2 or args[1].lower() != b'replication' or client.multi:
                client.dirty = client.dirty or client.multi
                return CommandResult(Error(b'ERR only INFO replication outside MULTI is supported'))
            reply = self.info()
            return CommandResult(reply if reply_size(reply) <= self.config.max_reply_bytes
                                 else Error(b'ERR reply exceeds configured maximum size'))
        if self.is_replica:
            command = self.persistence.executor.registry.resolve(args, queued=client.multi)
            if not isinstance(command, Error) and command.writes:
                if client.multi:
                    client.dirty = True
                return CommandResult(READONLY)
            if name not in (b'PING', b'QUIT', b'SUBSCRIBE', b'UNSUBSCRIBE', b'PSUBSCRIBE',
                            b'PUNSUBSCRIBE', b'PUBLISH') \
                    and self.phase != 'up':
                return CommandResult(Error(b'MASTERDOWN Replication link is down')
                    if self.phase in ('down', 'retrying') else Error(
                        b'LOADING Replica synchronization in progress'))
        return await self.persistence.execute(args, client)

    @staticmethod
    async def _monitor(reader: asyncio.StreamReader) -> None:
        try:
            await reader.readexactly(1)
        except ProtocolError as error:
            logger.warning('replica input protocol failure: %s', error)
            return
        except (asyncio.IncompleteReadError, OSError):
            return
        logger.warning('replica disconnected: unexpected data after full-sync request')

    def _accepts(self, args: list[bytes]) -> bool:
        return args[1:] == [b'1']

    def _new_session(self, writer: asyncio.StreamWriter, args: list[bytes]) -> Session:
        return Session(writer, self.config)

    async def _monitor_session(self, reader: asyncio.StreamReader, session: Session) -> None:
        await self._monitor(reader)

    async def _try_resume(self, writer: asyncio.StreamWriter, session: Session) -> bool:
        return False

    def _subscribed(self, session: Session) -> None:
        pass

    async def _greeting(self, info: SnapshotInfo, session: Session) -> bytes:
        return info.encode()

    def _before_send(self, session: Session, frame: bytes) -> None:
        pass

    def _idle_frame(self, session: Session, sent: int) -> bytes:
        return packet(b'H', sent)

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                    args: list[bytes], client: ClientState) -> None:
        denied = self.persistence.executor.access.authorize_peer(client)
        if denied is not None:
            await send(writer, serialize_reply(denied), self.config)
            return
        if self.config.replication_secure:
            tls_object = writer.get_extra_info('ssl_object')
            if tls_object is None:
                await send(writer, serialize_reply(Error(b'NOPERM secure transport required')),
                           self.config)
                return
            pin = self.config.replication_client_cert_sha256
            if pin is not None:
                certificate = tls_object.getpeercert(binary_form=True)
                if not certificate or not hmac.compare_digest(
                        hashlib.sha256(certificate).digest(), bytes.fromhex(pin)):
                    await send(writer, serialize_reply(
                        Error(b'NOPERM replica certificate rejected')),
                               self.config)
                    return
        if (self.is_replica or not self._accepts(args) or client.multi or client.watched
                or len(self.sessions) >= self.config.max_replicas):
            await send(writer, serialize_reply(Error(b'ERR full synchronization unavailable')),
                       self.config)
            return
        session = self._new_session(writer, args)
        client.replication_peer = self.config.replication_secure
        session.authorized = lambda: (not client.closed
            and self.persistence.executor.access.authorize_peer(client) is None)
        self.sessions.add(session)
        monitor = asyncio.create_task(self._monitor_session(reader, session))
        exporting: asyncio.Task[tuple[bytes, int]] | None = None
        temporary: tempfile.TemporaryDirectory[str] | None = None
        stream: BinaryIO | None = None
        opened: list[BinaryIO] = []
        try:
            if not await self._try_resume(writer, session):
                temporary = await self._temporary()
                target, uid = Path(temporary.name) / 'snapshot', uuid.uuid4().bytes

                def subscribe(sequence: int) -> None:
                    if session.failed or self.closing:
                        raise ConnectionError('replica left before snapshot boundary')
                    session.base, session.active = self.offset, True
                    self._subscribed(session)

                exporting = asyncio.create_task(self.persistence.export_snapshot(
                    uid, target, subscribe))
                completed, _ = await asyncio.wait((exporting, monitor),
                                                   return_when=asyncio.FIRST_COMPLETED)
                if monitor in completed:
                    await monitor
                    return
                store_id, sequence = await exporting
                if session.failed:
                    return
                size = (await self.persistence._io(target.stat)).st_size
                info = SnapshotInfo(self.replication_id, session.base, store_id, uid,
                                    sequence, size)
                greeting = await self._greeting(info, session)
                if not session.authorized():
                    return
                await send(writer, serialize_reply(Simple(greeting)), self.config)
                stream = await self.persistence._io(partial(_open_snapshot, target, opened))
                while chunk := await self.persistence._io(partial(stream.read, 65536)):
                    if not session.authorized():
                        return
                    await send(writer, chunk, self.config)
                await self.persistence._io(stream.close)
                opened.clear()
                stream = None
                await self.persistence._io(temporary.cleanup)
                temporary = None
            sent = session.base
            while not session.failed and not self.closing:
                if not session.authorized():
                    return
                getter = asyncio.create_task(session.queue.get())
                try:
                    stream_done, _ = await asyncio.wait((getter, monitor),
                        timeout=self.config.replication_heartbeat_interval,
                        return_when=asyncio.FIRST_COMPLETED)
                    if not session.authorized():
                        return
                    if monitor in stream_done:
                        await monitor
                        return
                    if getter in stream_done:
                        frame = getter.result()
                        self._before_send(session, frame)
                        await send(writer, frame, self.config)
                        session.pending_bytes -= len(frame)
                        session.pending_frames -= 1
                        if frame[:1] == b'E':
                            sent += len(frame)
                    else:
                        # Cancel before awaiting I/O: a late queue item must not be
                        # consumed by an abandoned getter during heartbeat send.
                        getter.cancel()
                        try:
                            await getter
                        except asyncio.CancelledError:
                            pass
                        await send(writer, self._idle_frame(session, sent), self.config)
                finally:
                    if not getter.done():
                        getter.cancel()
                    try:
                        await getter
                    except asyncio.CancelledError:
                        pass
        except (OSError, TimeoutError, JournalError, ProtocolError) as error:
            logger.warning('full-sync peer failed: %s', error)
        finally:
            session.active = False
            try:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass
            finally:
                try:
                    if exporting is not None:
                        if not exporting.done():
                            exporting.cancel()
                        try:
                            await exporting
                        except asyncio.CancelledError:
                            pass
                        except (OSError, JournalError, ProtocolError) as error:
                            logger.debug('closed peer snapshot: %s', error)
                finally:
                    try:
                        if opened:
                            await self.persistence._io(opened[0].close)
                    finally:
                        try:
                            if temporary is not None:
                                await self.persistence._io(temporary.cleanup)
                        finally:
                            self.sessions.discard(session)

    async def _temporary(self) -> tempfile.TemporaryDirectory[str]:
        files = self.persistence.generations
        assert files is not None
        owned: list[tempfile.TemporaryDirectory[str]] = []
        handed_over = False

        def allocate() -> tempfile.TemporaryDirectory[str]:
            value = tempfile.TemporaryDirectory(prefix='.pyredis-sync-', dir=files.root.parent)
            owned.append(value)
            return value

        try:
            value = await self.persistence._io(allocate)
            handed_over = True
            return value
        finally:
            if not handed_over and owned:
                await self.persistence._io(owned[0].cleanup)

    async def _receive_snapshot(self, reader: asyncio.StreamReader, info: SnapshotInfo) -> None:
        temporary = await self._temporary()
        try:
            source = Path(temporary.name) / 'received.snapshot'
            descriptors: list[int] = []

            def create_download() -> int:
                fd = os.open(source, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | getattr(os, 'O_BINARY', 0), 0o600)
                descriptors.append(fd)
                return fd

            try:
                fd = await self.persistence._io(create_download)
                remaining = info.size
                async with asyncio.timeout(self.config.replication_timeout):
                    while remaining:
                        chunk = await reader.readexactly(min(65536, self.config.max_input_bytes,
                                                            remaining))
                        await self.persistence._io(partial(_write_all, fd, chunk))
                        remaining -= len(chunk)
            finally:
                if descriptors:
                    await self.persistence._io(partial(os.close, descriptors[0]))
            await self.persistence.install_snapshot(source, info)
        finally:
            await self.persistence._io(temporary.cleanup)

    async def _follow(self) -> None:
        try:
            reader, writer = await self._connect()
            await send(writer, serialize_reply(Array(tuple(Bulk(x) for x in REQUEST))), self.config)
            async with asyncio.timeout(self.config.replication_timeout):
                greeting = await read_value(reader, self.config)
            if not isinstance(greeting, Simple):
                raise ReplicationError('primary refused the PyRedis full-sync handshake')
            info = SnapshotInfo.decode(greeting.payload, self.config)
            self.phase = 'receiving'
            self.changed.set()
            await self._receive_snapshot(reader, info)
            self.replication_id, self.offset, self.phase = info.replication_id, info.offset, 'up'
            self.changed.set()
            while not self.closing:
                kind, end, payload = await receive(reader, self.config, self.offset)
                if kind == b'E':
                    group = decode_group(payload, self.config)
                    if not group.clear and not group.entries:
                        raise ReplicationError('empty replication effect')
                    await self.persistence.apply_effect(group)
                    self.offset = end
                    self.changed.set()
                    if self.persistence.executor.waiters.count:
                        self.persistence.schedule_waiters()
        except (OSError, TimeoutError, JournalError, ProtocolError,
                asyncio.IncompleteReadError) as error:
            self.last_error = str(error)[:256]
            logger.warning('replica link failed: %s', self.last_error)
        finally:
            self.phase = 'down'
            self.changed.set()
            self.persistence.executor.waiters.replica_unavailable()
            if self.upstream is not None:
                await close(self.upstream, self.config)

    async def _connect(self) -> tuple[BoundedReader, asyncio.StreamWriter]:
        loop = asyncio.get_running_loop()
        assert self.config.replica_host is not None and self.config.replica_port is not None
        if self.config.replication_secure and (self.tls.upstream is None
                or self.tls.password is None or self.config.replica_username is None):
            raise ReplicationError('secure upstream configuration unavailable')
        reader = BoundedReader(self.config)
        protocol = asyncio.StreamReaderProtocol(reader)
        async with asyncio.timeout(self.config.replication_timeout):
            transport, _ = await loop.create_connection(
                lambda: protocol, self.config.replica_host, self.config.replica_port,
                ssl=self.tls.upstream,
                server_hostname=self.config.replica_server_name if self.tls.upstream else None,
                ssl_handshake_timeout=(self.config.tls_handshake_timeout
                                       if self.tls.upstream else None))
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        self.upstream = writer
        set_write_limits(writer)
        if self.config.replication_secure:
            assert self.tls.password is not None and self.config.replica_username is not None
            credentials = [b'AUTH', self.config.replica_username.encode('ascii'), self.tls.password]
            await send(writer, serialize_reply(Array(tuple(Bulk(value) for value in credentials))),
                       self.config)
            async with asyncio.timeout(self.config.replication_timeout):
                response = await read_value(reader, self.config)
            if response != Simple(b'OK'):
                raise ReplicationError('upstream authentication failed')
        return reader, writer

    def begin_close(self) -> None:
        self.closing = True
        if self.upstream is not None:
            self.upstream.close()
        if self.task is not None:
            self.task.cancel()
        for session in self.sessions:
            session.writer.close()

    async def close(self) -> None:
        self.begin_close()
        if self.task is not None:
            try:
                await self.task
            except asyncio.CancelledError:
                pass
