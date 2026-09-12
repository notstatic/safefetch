"""Time source abstraction.

Policies never call the system clock directly. They read the time from a
Clock, so tests can control time instead of sleeping.
"""

import time
from typing import Protocol


class Clock(Protocol):
    """A source of monotonic time, in seconds."""

    def monotonic(self) -> float:
        """Return a value that never decreases between calls.

        Only differences between two readings are meaningful.
        """
        ...


class SystemClock:
    """Real time, backed by time.monotonic()."""

    def monotonic(self) -> float:
        return time.monotonic()


class FakeClock:
    """Time that moves only when the test moves it."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot move time backwards")
        self._now += seconds