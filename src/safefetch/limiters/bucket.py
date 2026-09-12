"""Token bucket rate limiter.

Tokens refill continuously at a fixed rate up to a maximum capacity. A
request costs one or more tokens. When the bucket holds enough, the request
proceeds; otherwise the caller is told how long to wait.

Refill is computed lazily on read from the elapsed time, so the limiter
needs no background task and no timer.

This module performs no I/O, holds no locks, and never sleeps. State is
passed in and returned, which lets the same code serve sync, async and
distributed callers.
"""

from dataclasses import dataclass

from ..decision import Allow, Decision, Wait


@dataclass(frozen=True, slots=True)
class BucketState:
    """A point-in-time snapshot of one bucket."""

    tokens: float
    updated_at: float


@dataclass(frozen=True, slots=True)
class TokenBucket:
    """Configuration for a token bucket.

    Args:
        rate: Tokens added per second.
        capacity: Maximum tokens the bucket can hold, which is also the
            largest burst it will allow.
    """

    rate: float
    capacity: float

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate must be positive")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")

    def initial_state(self, now: float) -> BucketState:
        """Return a full bucket. New callers are not penalised for arriving."""
        return BucketState(tokens=self.capacity, updated_at=now)

    def check(
        self,
        state: BucketState,
        now: float,
        cost: float = 1.0,
    ) -> tuple[BucketState, Decision]:
        """Decide whether a request of this cost may proceed.

        Returns the updated state and the decision. The state is returned
        even when the request is denied, because the refill has already
        been accounted for.

        Raises:
            ValueError: If cost is not positive, or exceeds capacity and so
                could never be satisfied.
        """
        if cost <= 0:
            raise ValueError("cost must be positive")
        if cost > self.capacity:
            raise ValueError(
                f"cost {cost} exceeds capacity {self.capacity} and can never be satisfied"
            )

        # A monotonic clock should never move backwards, but a caller may
        # supply its own clock. Treat any backwards step as no elapsed time
        # rather than removing tokens.
        elapsed = max(0.0, now - state.updated_at)
        tokens = min(self.capacity, state.tokens + elapsed * self.rate)

        if tokens >= cost:
            return BucketState(tokens=tokens - cost, updated_at=now), Allow()

        missing = cost - tokens
        return BucketState(tokens=tokens, updated_at=now), Wait(missing / self.rate)