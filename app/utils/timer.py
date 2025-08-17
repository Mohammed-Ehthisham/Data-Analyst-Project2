"""
Time budget utilities to ensure we always reply within a hard deadline.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class TimeBudget:
    total_seconds: float
    start_monotonic: float

    @property
    def deadline_monotonic(self) -> float:
        return self.start_monotonic + self.total_seconds

    def remaining_seconds(self) -> float:
        now = time.monotonic()
        return max(0.0, self.deadline_monotonic - now)

    def elapsed_seconds(self) -> float:
        return max(0.0, time.monotonic() - self.start_monotonic)

    def time_exhausted(self, buffer_seconds: float = 0.0) -> bool:
        return self.remaining_seconds() <= max(0.0, buffer_seconds)


@contextmanager
def with_time_budget(total_seconds: float = 150.0):
    budget = TimeBudget(total_seconds=total_seconds, start_monotonic=time.monotonic())
    try:
        yield budget
    finally:
        # nothing to cleanup; explicit context manager for ergonomic usage
        pass
