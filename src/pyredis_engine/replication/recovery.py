"""Opt-in v2 recovery: bounded live history, ACKs and one reconnect supervisor."""
from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..config import ServerConfig
from ..persistence.codec import decode_group
from ..persistence.coordinator import Persistence
from ..persistence.errors import JournalError, ReplicaDivergence
from ..protocol import Array, Bulk, ProtocolError, Simple, read_value, serialize_reply
from ..streams import close, send
from .backlog import Backlog, advance
from .node import Replication, Session
from .recovery_wire import (
    AckTracker,
    ResumeRequest,
    acknowledgement,
    continuation,
    full_greeting,
    parse_continuation,
    parse_full,
    receive_ack,
)
from .wire import HEADER, ReplicationEOF, ReplicationError, SnapshotInfo, packet, receive

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class RecoverySession(Session):
    request: ResumeRequest | None = None
    tracker: AckTracker | None = None
    base_proof: bytes = b''
    last_ack_time: float | None = None


class RecoveryReplication(Replication):
    def __init__(self, persistence: Persistence, config: ServerConfig) -> None:
        super().__init__(persistence, config)
        self.backlog = (None if self.is_replica else Backlog(config.replication_backlog_bytes,
                        config.replication_backlog_frames, self.replication_id))
        self.proof = self.backlog.proof if self.backlog is not None else b''
        self.resume: ResumeRequest | None = None
        self.guard: tuple[int, int] | None = None
        self.retry_wait: Callable[[float], Awaitable[None]] = asyncio.sleep
        self.attempts_total = self.retry_failures = 0
        self.full_syncs = self.partial_syncs = 0
        self.last_sync = ''
        self.ready_since: float | None = None

    def publish(self, payload: bytes) -> None:
        assert self.backlog is not None
        frame = self.backlog.append(payload)
        self.offset, self.proof = self.backlog.offset, self.backlog.proof
        for session in tuple(self.sessions):
            session.push(frame)
        self.changed.set()

    def _accepts(self, args: list[bytes]) -> bool:
        return super()._accepts(args) or len(args) == 5 and args[1] == b'2'

    def _new_session(self, writer: asyncio.StreamWriter, args: list[bytes]) -> Session:
        if args[1] == b'1':
            return super()._new_session(writer, args)
        return RecoverySession(writer, self.config, request=ResumeRequest.decode(args))

    def _subscribed(self, session: Session) -> None:
        if isinstance(session, RecoverySession):
            session.base_proof = self.proof
            session.tracker = AckTracker(self.replication_id, session.base, self.proof,
                                         self.config.replication_ack_max_pending)

    def _barrier(self, session: RecoverySession) -> int:
        session.push(packet(b'R', self.offset, self.proof))
        if session.failed:
            raise ConnectionError('catch-up boundary exceeds peer queue capacity')
        return self.offset

    async def _try_resume(self, writer: asyncio.StreamWriter, session: Session) -> bool:
        if not isinstance(session, RecoverySession) or session.request is None:
            return False
        request = session.request
        assert self.backlog is not None
        async with self.persistence.lock:
            if self.persistence.failure is not None or self.persistence.closing:
                raise JournalError('persistence is stopping')
            if not session.authorized():
                raise ConnectionError('replica authorization revoked')
            frames = (self.backlog.resume(request.offset, request.proof)
                      if request.history_id == self.replication_id else None)
            if (frames is None or len(frames) + 1 > self.config.replication_queue_frames
                    or sum(map(len, frames)) + 77 > self.config.replication_queue_bytes):
                return False
            session.base, session.active = request.offset, True
            session.base_proof = request.proof
            session.tracker = AckTracker(self.replication_id, request.offset, request.proof,
                                         self.config.replication_ack_max_pending)
            for frame in frames:
                session.push(frame)
            target = self._barrier(session)
        await send(writer, serialize_reply(Simple(continuation(self.replication_id,
            request.offset, request.proof, target))), self.config)
        self.partial_syncs += 1
        return True

    async def _greeting(self, info: SnapshotInfo, session: Session) -> bytes:
        if not isinstance(session, RecoverySession):
            return await super()._greeting(info, session)
        async with self.persistence.lock:
            target = self._barrier(session)
            self.full_syncs += 1
        return full_greeting(info, session.base_proof, target)

    def _before_send(self, session: Session, frame: bytes) -> None:
        if isinstance(session, RecoverySession):
            assert session.tracker is not None
            # Register before yielding: the peer may ACK during the final drain.
            session.tracker.offer(frame)

    def _idle_frame(self, session: Session, sent: int) -> bytes:
        if isinstance(session, RecoverySession):
            assert session.tracker is not None
            return packet(b'H', sent, session.tracker.proof)
        return super()._idle_frame(session, sent)

    async def _monitor_session(self, reader: asyncio.StreamReader, session: Session) -> None:
        if not isinstance(session, RecoverySession):
            await super()._monitor_session(reader, session)
            return
        try:
            while not self.closing:
                offset, payload = await receive_ack(reader, self.config)
                if session.tracker is None:
                    raise ReplicationError('ACK precedes snapshot boundary')
                session.tracker.accept(offset, payload)
                session.last_ack_time = asyncio.get_running_loop().time()
                self.changed.set()
        except (OSError, TimeoutError, ProtocolError) as error:
            logger.warning('replica ACK link ended: %s', error)

    def _boundary(self) -> tuple[int, int]:
        return self.persistence.file.sequence, self.persistence.executor.store.mutation_revision

    def _remember(self) -> None:
        self.guard = self._boundary()
        self.resume = ResumeRequest(self.replication_id, self.offset, self.proof)

    def _request(self) -> list[bytes]:
        if self.resume is not None and self.guard != self._boundary():
            self.resume = None
        token = self.resume
        return [b'PYREDIS.SYNC', b'2', token.history_id if token else b'?',
                str(token.offset).encode() if token else b'-1',
                token.proof.hex().encode() if token else b'?']

    async def _ack(self, writer: asyncio.StreamWriter) -> None:
        await send(writer, acknowledgement(self.replication_id, self.offset, self.proof,
                                           self.config.aof_fsync), self.config)

    async def _process(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                       target: int, mode: str) -> None:
        kind, end, payload = await receive(reader, self.config, self.offset, version=2)
        if self.guard is None or self.guard != self._boundary():
            raise ReplicaDivergence('local state diverged from the applied history boundary')
        if kind == b'E':
            if self.guard is None:
                raise ReplicaDivergence('missing local recovery boundary')
            group = decode_group(payload, self.config)
            if not group.clear and not group.entries:
                raise ReplicationError('empty recovery effect')
            digest = hashlib.sha256(HEADER.pack(b'E', end, len(payload)))
            digest.update(payload)
            next_proof = advance(self.proof, digest.digest())
            await self.persistence.apply_effect(group, boundary=self.guard)
            self.offset, self.proof = end, next_proof
            self._remember()
        elif payload != self.proof:
            raise ReplicationError('replication history proof diverged')
        elif kind == b'R':
            if self.phase != 'catching_up' or end != target:
                raise ReplicationError('invalid or repeated catch-up barrier')
            self.phase, self.last_sync, self.last_error = 'up', mode, ''
            self.ready_since = asyncio.get_running_loop().time()
            if mode == 'full':
                self.full_syncs += 1
            else:
                self.partial_syncs += 1
        self.changed.set()
        if self.phase == 'up' and self.persistence.executor.waiters.count:
            self.persistence.schedule_waiters()
        await self._ack(writer)

    async def _connection(self) -> None:
        reader, writer = await self._connect()
        request = self._request()
        requested = ResumeRequest.decode(request)
        await send(writer, serialize_reply(Array(tuple(Bulk(value) for value in request))),
                   self.config)
        async with asyncio.timeout(self.config.replication_timeout):
            greeting = await read_value(reader, self.config)
        if not isinstance(greeting, Simple):
            raise ReplicationError('primary refused version-2 synchronization')
        if greeting.payload.startswith(b'PYREDIS-CONTINUE '):
            target = parse_continuation(greeting.payload, requested)
            if self.guard != self._boundary():
                raise ReplicaDivergence('replica changed while negotiating resume')
            mode = 'partial'
        else:
            info, proof, target = parse_full(greeting.payload, self.config)
            self.resume = None
            self.phase = 'receiving'
            self.changed.set()
            await self._receive_snapshot(reader, info)
            self.replication_id, self.offset, self.proof = info.replication_id, info.offset, proof
            self._remember()
            mode = 'full'
        self.phase = 'catching_up'
        self.changed.set()
        await self._ack(writer)
        async with asyncio.timeout(self.config.replication_catchup_timeout):
            while self.phase == 'catching_up':
                await self._process(reader, writer, target, mode)
        while not self.closing:
            await self._process(reader, writer, target, mode)

    async def _follow(self) -> None:
        delay = self.config.replication_retry_delay
        try:
            while not self.closing and self.persistence.failure is None:
                self.attempts_total += 1
                self.ready_since = None
                try:
                    await self._connection()
                except (ReplicationEOF, OSError, TimeoutError,
                        asyncio.IncompleteReadError) as error:
                    self.last_error = str(error)[:256]
                except (ProtocolError, JournalError) as error:
                    self.resume = None
                    self.last_error = str(error)[:256]
                finally:
                    self.phase = 'retrying'
                    self.changed.set()
                    self.persistence.executor.waiters.replica_unavailable()
                    if self.upstream is not None:
                        await close(self.upstream, self.config)
                        self.upstream = None
                if self.closing or self.persistence.failure is not None:
                    return
                logger.warning('replica recovery connection ended: %s', self.last_error)
                if (self.ready_since is not None and asyncio.get_running_loop().time()
                        - self.ready_since >= self.config.replication_retry_reset_after):
                    self.retry_failures, delay = 0, self.config.replication_retry_delay
                self.retry_failures += 1
                if self.retry_failures >= self.config.replication_retry_attempts:
                    return
                await self.retry_wait(delay)
                delay = min(delay * 2, self.config.replication_retry_max_delay)
        finally:
            self.phase = 'down'
            self.changed.set()

    async def wait_offset(self, offset: int, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            while self.offset < offset or self.is_replica and self.phase != 'up':
                if self.phase == 'down':
                    raise ReplicationError(self.last_error or 'recovery stopped')
                self.changed.clear()
                await self.changed.wait()

    def info(self) -> Bulk:
        base = super().info().payload
        assert base is not None
        fields = [(b'recovery_enabled', b'1'),
                  (b'recovery_attempts', str(self.attempts_total).encode()),
                  (b'recovery_full_syncs', str(self.full_syncs).encode()),
                  (b'recovery_partial_syncs', str(self.partial_syncs).encode())]
        if self.backlog is not None:
            fields.extend([(b'repl_backlog_first_byte_offset',
                            str(self.backlog.first_offset + 1).encode()),
                           (b'repl_backlog_start_offset', str(self.backlog.first_offset).encode()),
                           (b'repl_backlog_histlen', str(self.backlog.bytes).encode()),
                           (b'repl_backlog_frames', str(len(self.backlog.records)).encode())])
            for index, session in enumerate(self.sessions):
                if isinstance(session, RecoverySession) and session.tracker is not None:
                    tracker = session.tracker
                    prefix = b'replica' + str(index).encode()
                    fields.extend([(prefix + b'_ack_offset',
                                    str(tracker.acknowledged if tracker.acknowledged is not None
                                        else -1).encode()),
                                   (prefix + b'_ack_policy', tracker.policy or b'?')])
        return Bulk(base + b''.join(key + b':' + value + b'\r\n' for key, value in fields))
