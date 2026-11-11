"""Wall time for absolute deadlines; network scheduling remains monotonic."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


@dataclass
class ManualClock:
    milliseconds: int

    def now_ms(self) -> int:
        return self.milliseconds

    def advance(self, milliseconds: int) -> None:
        self.milliseconds += milliseconds
