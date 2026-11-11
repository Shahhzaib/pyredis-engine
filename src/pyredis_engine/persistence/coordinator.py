"""Serialize mutation then disk barrier; worker threads never access the datastore."""
from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import TypeVar, cast

from ..commands.registry import CommandResult
from ..config import ServerConfig
from ..engine.executor import Executor
from ..engine.transactions import ClientState
from ..ops.memory import MemoryLimitError
from ..protocol import Error
from ..replication.snapshots import export_snapshot, prepare_snapshot
from ..replication.wire import SnapshotInfo
from .codec import Group, encode_group
from .effects import Capture, Dataset
from .errors import CapacityError, JournalError, ReplicaDivergence, SourceCorruption
from .generations import CompactionResult, Generation, GenerationFiles
from .journal import FileJournal

T = TypeVar("T")
FAILED = CommandResult(Error(b"MISCONF persistence unavailable; server is stopping"), close=True)
logger = logging.getLogger(__name__)


class Persistence:
    def __init__(self, executor: Executor, config: ServerConfig,
                 sync_wait: Callable[[float], Awaitable[None]]) -> None:
        self.executor, self.config, self.sync_wait = executor, config, sync_wait
        self.file = FileJournal(config)
        self.dataset = Dataset(config)
        self.worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pyredis-aof")
        self.lock = asyncio.Lock()
        self.pending: set[asyncio.Task[object]] = set()
        self.waiter_task: asyncio.Task[None] | None = None
        self.waiters_dirty = False
        self.sync_task: asyncio.Task[None] | None = None
        self.failure: BaseException | None = None
        self.closing = self.closed = False
        self.on_failure: Callable[[], None] = lambda: None
        self.generations = GenerationFiles(config) if config.snapshot_enabled else None
        self.snapshot_worker: ThreadPoolExecutor | None = None
        self.compacting = False
        self.maintenance = asyncio.Lock()
        self.on_commit: Callable[[bytes], None] = lambda _: None

    async def _io(self, operation: Callable[[], T]) -> T:
        future = asyncio.get_running_loop().run_in_executor(self.worker, operation)
        return await self._finish(future)

    @staticmethod
    async def _finish(future: asyncio.Future[T]) -> T:
        cancelled = False
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                cancelled = True
        result = future.result()
        if cancelled:
            raise asyncio.CancelledError
        return result

    @classmethod
    async def open(cls, executor: Executor, config: ServerConfig,
                   sync_wait: Callable[[float], Awaitable[None]] = asyncio.sleep) -> Persistence:
        owner = cls(executor, config, sync_wait)
        success = False
        try:
            if owner.generations is not None:
                files = owner.generations
                await owner._io(files.open_lease)
                manifest = await owner._io(files.read_manifest)
                owner.file = files.current_journal()
                if manifest is not None:
                    reader = files.snapshot_reader(manifest.current)
                    try:
                        await owner._io(reader.open)
                        while (group := await owner._io(reader.next_group)) is not None:
                            owner.dataset.apply(group)
                            executor.restore_group(group)
                    finally:
                        await owner._io(reader.close)
                await owner._io(lambda: files._stage("journal_replay"))
            await owner._io(owner.file.open)
            while (group := await owner._io(owner.file.next_group)) is not None:
                owner.dataset.apply(group)
                executor.restore_group(group)
                if owner.generations is not None:
                    await owner._io(partial(owner.generations._stage, "journal_replay_group"))
            if config.aof_fsync == "always" and owner.file.dirty:
                await owner._io(owner.file.sync)
            if owner.file.repaired_bytes:
                logger.warning("explicit AOF tail repair removed %d bytes",
                               owner.file.repaired_bytes)
            if config.aof_fsync == "everysec":
                owner.sync_task = asyncio.create_task(owner._sync_loop(), name="pyredis-aof-sync")
                owner.sync_task.add_done_callback(owner._sync_finished)
            if config.compact_on_start:
                result = await owner.compact()
                if not result.activated:
                    raise JournalError("startup compaction failed: " + str(result.error))
            success = True
            return owner
        finally:
            if not success:
                owner.failure = JournalError("AOF startup failed")
                await owner.close()

    def _fail(self, error: BaseException) -> None:
        if self.failure is None:
            self.failure = error
            logger.error("AOF failure; stopping server: %s", error)
            self.on_failure()

    def _sync_finished(self, task: asyncio.Task[None]) -> None:
        # Task boundary: unexpected programming errors remain visible and fail closed.
        if not task.cancelled() and (error := task.exception()) is not None:
            self._fail(error)
            task.get_loop().call_exception_handler(
                {"message": "AOF sync task failed", "exception": error})

    async def _sync_loop(self) -> None:
        while True:
            await self.sync_wait(self.config.aof_sync_interval)
            async with self.lock:
                if self.closing or self.failure is not None:
                    return
                try:
                    if self.file.dirty:
                        await self._io(self.file.sync)
                except (OSError, JournalError) as error:
                    self._fail(error)
                    return

    async def _commit(self, group: Group) -> None:
        if not group.clear and not group.entries:
            return
        payload = encode_group(group, self.config)  # bytes frozen before yielding
        self.dataset.apply(group)
        await self._io(lambda: self.file.append(payload))
        self.on_commit(payload)

    async def _owned(self, operation: Coroutine[None, None, T]) -> T:
        task = asyncio.create_task(operation)
        tracked = cast(asyncio.Task[object], task)
        self.pending.add(tracked)
        task.add_done_callback(self.pending.discard)
        return await self._finish(task)

    async def execute(self, args: list[bytes], client: ClientState) -> CommandResult:
        return await self._owned(self._execute(args, client))

    def schedule_waiters(self) -> None:
        self.waiters_dirty = True
        if self.waiter_task is not None and not self.waiter_task.done():
            return
        task = asyncio.create_task(self._serve_waiters())
        self.waiter_task = task
        self.pending.add(cast(asyncio.Task[object], task))

        def finished(completed: asyncio.Task[None]) -> None:
            self.pending.discard(cast(asyncio.Task[object], completed))
            if self.waiter_task is completed:
                self.waiter_task = None
            if not completed.cancelled() and (error := completed.exception()) is not None:
                self._fail(error)
                completed.get_loop().call_exception_handler(
                    {"message": "blocking waiter worker failed", "exception": error})

        task.add_done_callback(finished)

    async def _serve_waiters(self) -> None:
        while self.waiters_dirty and self.failure is None and not self.closing:
            self.waiters_dirty = False
            async with self.lock:
                if self.failure is not None or self.closing:
                    return
                try:
                    await self._drain_waiters()
                except (OSError, JournalError) as error:
                    self._fail(error)
                    return

    async def _drain_waiters(self) -> None:
        waiters = self.executor.waiters
        for waiter in waiters.candidates():
            if not waiters.claim(waiter):
                continue
            finished = False
            try:
                denied = self.executor.access_error(waiter.args, waiter.client)
                if denied is not None:
                    waiters.resolved(waiter, CommandResult(denied))
                    finished = True
                    continue
                try:
                    self.dataset.preflight(waiter.args, waiter.client, self.executor.registry,
                                           self.executor.access.authorize, self.executor.store)
                except CapacityError as error:
                    waiters.resolved(waiter, CommandResult(Error(
                        b'ERR ' + str(error).encode('ascii'))))
                    finished = True
                    continue
                with Capture(self.executor.store) as captured:
                    result = self.executor.execute(waiter.args, waiter.client, defer_pubsub=True)
                await self._commit(captured.group())
                result = self.executor.complete(result, waiter.client)
                waiters.resolved(waiter, result)
                finished = True
            finally:
                if not finished:
                    waiter.claimed = False
                    waiters.cancel(waiter)

    async def _execute(self, args: list[bytes], client: ClientState) -> CommandResult:
        async with self.lock:
            if self.failure is not None or self.closing:
                return FAILED
            if client.closed:
                return CommandResult(Error(b'NOAUTH Authentication required.'), close=True)
            if self.executor.access_error(args, client) is not None:
                return self.executor.execute(args, client)
            try:
                self.dataset.preflight(args, client, self.executor.registry,
                                       self.executor.access.authorize, self.executor.store)
            except CapacityError as error:
                if args == [b"EXEC"] or len(args) == 1 and args[0].upper() == b"EXEC":
                    if client.multi:
                        self.executor.discard_transaction(client)
                        return CommandResult(Error(b"EXECABORT " + str(error).encode("ascii")))
                return CommandResult(Error(b"ERR " + str(error).encode("ascii")))
            finished = False
            try:
                revision = self.executor.store.mutation_revision
                with Capture(self.executor.store) as captured:
                    result = self.executor.execute(args, client, defer_pubsub=True)
                await self._commit(captured.group())
                result = self.executor.complete(result, client)
                if self.executor.store.mutation_revision != revision:
                    await self._drain_waiters()
                finished = True
                return result
            except (OSError, JournalError) as error:
                self._fail(error)
                finished = True
                return FAILED
            finally:
                if not finished:
                    # Preserve the original exception; task-boundary reporting
                    # remains responsible for its traceback. Never keep serving.
                    self._fail(JournalError("unexpected exception during AOF command commit"))

    async def expire_batch(self) -> int:
        return await self._owned(self._expire_batch())

    async def compact(self) -> CompactionResult:
        if self.generations is None:
            raise JournalError("snapshot mode is not enabled")
        if self.compacting:
            raise JournalError("compaction is already running")
        self.compacting = True
        try:
            return await self._owned(self._compact())
        finally:
            self.compacting = False

    async def _compact(self) -> CompactionResult:
        async with self.maintenance:
            return await self._compact_generation()

    async def _background(self, operation: Callable[[], T]) -> T:
        if self.snapshot_worker is None:
            self.snapshot_worker = ThreadPoolExecutor(max_workers=1,
                                                      thread_name_prefix="pyredis-snapshot")
        future = asyncio.get_running_loop().run_in_executor(self.snapshot_worker, operation)
        return await self._finish(future)

    async def export_snapshot(self, uid: bytes, path: Path,
                              subscribe: Callable[[int], None]) -> tuple[bytes, int]:
        return await self._owned(self._export_snapshot(uid, path, subscribe))

    async def _export_snapshot(self, uid: bytes, path: Path,
                               subscribe: Callable[[int], None]) -> tuple[bytes, int]:
        files = self.generations
        if files is None:
            raise JournalError("snapshot mode required")
        async with self.maintenance:
            async with self.lock:
                if self.closing or self.failure is not None:
                    raise JournalError("persistence is stopping")
                try:
                    pinned = await self._io(lambda: files.pin(self.file))
                except (OSError, JournalError) as error:
                    self._fail(error)
                    raise
                subscribe(pinned.sequence)
            try:
                await self._background(lambda: export_snapshot(files, pinned, uid, path))
            except SourceCorruption as error:
                self._fail(error)
                raise
            return files.store_id, pinned.sequence

    async def apply_effect(self, group: Group, *, boundary: tuple[int, int] | None = None) -> None:
        await self._owned(self._apply_effect(group, boundary))

    async def _apply_effect(self, group: Group, boundary: tuple[int, int] | None = None) -> None:
        async with self.lock:
            if self.closing or self.failure is not None:
                raise JournalError("persistence is stopping")
            if boundary is not None and boundary != (
                    self.file.sequence, self.executor.store.mutation_revision):
                raise ReplicaDivergence('replica state changed outside its upstream history')
            # Validate limits before touching live state, including clear semantics.
            payload = encode_group(group, self.config)
            try:
                self.executor.store.check_batch(
                    {entry.key: entry.value for entry in group.entries}, clear=group.clear)
            except MemoryLimitError as cause:
                error = CapacityError('replicated dataset exceeds maxmemory accounting budget')
                self._fail(error)
                raise error from cause
            self.dataset.apply(group)
            finished = False
            try:
                self.executor.restore_group(group)
                await self._io(lambda: self.file.append(payload))
                finished = True
            except (OSError, JournalError) as error:
                self._fail(error)
                finished = True
                raise
            finally:
                if not finished:
                    self._fail(JournalError("unexpected error applying replicated effects"))

    async def install_snapshot(self, source: Path, info: SnapshotInfo) -> None:
        await self._owned(self._install_snapshot(source, info))

    async def _install_snapshot(self, source: Path, info: SnapshotInfo) -> None:
        files = self.generations
        if files is None:
            raise JournalError("snapshot mode required")
        async with self.maintenance:
            async with self.lock:
                if self.closing or self.failure is not None:
                    raise JournalError("persistence is stopping")
                try:
                    pinned = await self._io(lambda: files.pin(self.file))
                except (OSError, JournalError) as error:
                    self._fail(error)
                    raise
            if pinned.sequence == 2**64 - 1:
                raise JournalError("journal sequence exhausted")
            uid, base = uuid.uuid4().bytes, pinned.sequence + 1
            await self._background(lambda: prepare_snapshot(source, info, files, uid,
                                                             base, self.config))
            staged, dataset = Executor(self.config, clock=self.executor.store.clock), Dataset(
                self.config)
            reader = files.snapshot_reader(Generation(1, uid, base, base))
            try:
                await self._io(reader.open)
                while (group := await self._io(reader.next_group)) is not None:
                    dataset.apply(group)
                    staged.restore_group(group)
                await self._io(reader.close)
                async with self.lock:
                    if (self.closing or self.failure is not None
                            or self.file.sequence != pinned.sequence):
                        raise JournalError("snapshot replacement boundary changed")
                    finished = False
                    try:
                        await self._io(self.file.sync)
                        self.file = await self._io(lambda: files.activate(
                            replace(pinned, sequence=base), uid, self.file,
                            replacement=True))
                        self.executor.install_state(staged)
                        self.dataset = dataset
                        finished = True
                    finally:
                        if not finished:
                            self._fail(JournalError("replica snapshot installation failed"))
                try:
                    await self._io(files.cleanup)
                except (OSError, JournalError) as error:
                    logger.warning("replica snapshot retained old files: %s", error)
            finally:
                await self._io(reader.close)
                staged.close()

    async def _compact_generation(self) -> CompactionResult:
        files = self.generations
        assert files is not None
        finished = False
        try:
            async with self.lock:
                if self.closing or self.failure is not None:
                    finished = True
                    return CompactionResult(False, error="server is stopping")
                try:
                    pinned = await self._io(lambda: files.pin(self.file))
                except (OSError, JournalError) as error:
                    self._fail(error)
                    finished = True
                    return CompactionResult(False, error=str(error))
            uid = uuid.uuid4().bytes
            try:
                await self._background(lambda: files.build(pinned, uid))
            except SourceCorruption as error:
                self._fail(error)
                finished = True
                return CompactionResult(False, error=str(error))
            except (OSError, JournalError) as error:
                logger.warning("snapshot was not activated: %s", error)
                finished = True
                return CompactionResult(False, error=str(error))
            async with self.lock:
                if self.closing or self.failure is not None:
                    finished = True
                    return CompactionResult(False, error="server is stopping")
                try:
                    await self._io(self.file.sync)
                except (OSError, JournalError) as error:
                    self._fail(error)
                    finished = True
                    return CompactionResult(False, error=str(error))
                try:
                    self.file = await self._io(lambda: files.activate(pinned, uid, self.file))
                except (OSError, JournalError) as error:
                    if files.activation_attempted or isinstance(error, SourceCorruption):
                        self._fail(error)
                    logger.warning("snapshot cutover did not complete: %s", error)
                    finished = True
                    return CompactionResult(False, error=str(error))
                cutoff = self.file.sequence
            try:
                cleaned = await self._io(files.cleanup)
            except (OSError, JournalError) as error:
                logger.warning("active snapshot retained old generation files: %s", error)
                finished = True
                return CompactionResult(True, pinned.sequence, cutoff, True, str(error))
            finished = True
            return CompactionResult(True, pinned.sequence, cutoff, not cleaned)
        finally:
            if not finished:
                self._fail(JournalError("unexpected exception during snapshot compaction"))

    async def _expire_batch(self) -> int:
        async with self.lock:
            if self.failure is not None or self.closing:
                return 0
            finished = False
            try:
                with Capture(self.executor.store) as captured:
                    removed = self.executor.expire_batch(self.config.aof_max_record_bytes - 5)
                await self._commit(captured.group())
                if removed:
                    await self._drain_waiters()
                finished = True
                return removed
            except (OSError, JournalError) as error:
                self._fail(error)
                finished = True
                return 0
            finally:
                if not finished:
                    self._fail(JournalError("unexpected exception during AOF expiry commit"))

    def begin_close(self) -> None:
        self.closing = True
        if self.sync_task is not None:
            self.sync_task.cancel()

    async def close(self) -> None:
        if self.closed:
            return
        self.begin_close()
        try:
            if self.sync_task is not None:
                try:
                    await self.sync_task
                except asyncio.CancelledError:
                    pass
            for task in tuple(self.pending):
                await asyncio.shield(task)
            async with self.lock:
                if (self.failure is None and self.config.aof_fsync != "none"
                        and self.file.dirty):
                    try:
                        await self._io(self.file.sync)
                    except (OSError, JournalError) as error:
                        self._fail(error)
                        raise JournalError("AOF final sync failed") from error
        finally:
            try:
                await self._io(self.file.close)
            finally:
                try:
                    if self.generations is not None:
                        await self._io(self.generations.close)
                finally:
                    self.worker.shutdown(wait=True)
                    if self.snapshot_worker is not None:
                        self.snapshot_worker.shutdown(wait=True)
                    self.closed = True
