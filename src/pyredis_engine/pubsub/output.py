"""One bounded FIFO including its in-flight frame; no datastore or network I/O."""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from ..config import ServerConfig

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class OutputFrame:
    data: bytes
    size: int
    done: asyncio.Event = field(default_factory=asyncio.Event)
    finished: bool = False


class Outbox:
    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self.active = self.failed = False
        self.frames: deque[OutputFrame] = deque()
        self.inflight: OutputFrame | None = None
        self.pending_bytes = self.pending_frames = 0
        self.ready = asyncio.Event()
        self.on_failure: Callable[[], None] = lambda: None
        self.on_overflow: Callable[[], None] = lambda: None

    def enqueue(self, data: bytes) -> OutputFrame | None:
        if self.failed:
            return None
        if (len(data) > self.config.pubsub_queue_bytes - self.pending_bytes
                or self.pending_frames >= self.config.pubsub_queue_frames):
            logger.warning('Pub/Sub subscriber disconnected: output queue limit exceeded')
            self.fail()
            self.on_overflow()
            return None
        frame = OutputFrame(data, len(data))
        self.frames.append(frame)
        self.pending_bytes += frame.size
        self.pending_frames += 1
        self.ready.set()
        return frame

    async def get(self) -> OutputFrame | None:
        while not self.frames and not self.failed:
            self.ready.clear()
            await self.ready.wait()
        if self.failed:
            return None
        assert self.inflight is None
        self.inflight = self.frames.popleft()
        return self.inflight

    def finish(self, frame: OutputFrame) -> None:
        if frame.finished:
            return
        self.pending_bytes -= frame.size
        self.pending_frames -= 1
        frame.finished = True
        frame.data = b''
        if self.inflight is frame:
            self.inflight = None
        frame.done.set()

    def fail(self) -> None:
        if self.failed:
            return
        self.failed = True
        while self.frames:
            self.finish(self.frames.popleft())
        self.ready.set()
        self.on_failure()
