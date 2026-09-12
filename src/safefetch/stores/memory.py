"""In-process state storage for limiters.

The store owns two things the policy deliberately does not: where state
lives, and how concurrent access to it is serialised.

One lock guards the whole table. The critical section is pure arithmetic
with no I/O, so it is measured in microseconds. Per-key locks would cut
contention further, but they need a registry that itself needs a lock, and
that cost is not worth paying until a benchmark says otherwise.
"""

import threading
from collections import OrderedDict
from typing import Generic, Protocol, TypeVar

from ..decision import Decision
from ..limiters.base import Limiter

S = TypeVar("S")


class Store(Protocol[S]):
    """Somewhere limiter state can be read and written atomically per key."""

    def check(self, key: str, limiter: Limiter[S], now: float, cost: float = 1.0) -> Decision:
        """Apply the limiter to the state held for this key."""
        ...


class MemoryStore(Generic[S]):
    """Limiter state held in this process only.

    Args:
        max_keys: Evict the least recently used key once the table exceeds
            this size. Leave unset when keys are bounded, such as one per
            host. Set it when keys come from user input, where an unbounded
            table is a memory leak.
    """

    def __init__(self, max_keys: int | None = None) -> None:
        if max_keys is not None and max_keys < 1:
            raise ValueError("max_keys must be at least 1")
        self._max_keys = max_keys
        self._states: OrderedDict[str, S] = OrderedDict()
        self._lock = threading.Lock()

    def check(self, key: str, limiter: Limiter[S], now: float, cost: float = 1.0) -> Decision:
        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = limiter.initial_state(now)

            new_state, decision = limiter.check(state, now, cost)

            self._states[key] = new_state
            self._states.move_to_end(key)
            self._evict_if_needed()

            return decision

    def _evict_if_needed(self) -> None:
        if self._max_keys is None:
            return
        while len(self._states) > self._max_keys:
            self._states.popitem(last=False)

    def reset(self, key: str) -> None:
        """Forget one key. The next request for it starts fresh."""
        with self._lock:
            self._states.pop(key, None)

    def clear(self) -> None:
        """Forget every key."""
        with self._lock:
            self._states.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._states)