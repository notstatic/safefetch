"""Retry policy.

Like the limiters, this module decides and returns. It performs no I/O and
never sleeps, so the same policy serves sync and async callers.

Two ideas carry most of the weight here. Full jitter spreads retries across
the whole backoff window instead of having every client return at the same
instant, which is what turns a brief outage into a thundering herd. And a
wall-clock budget sits alongside the attempt count, because ten attempts
with exponential backoff can otherwise stretch into an hour.
"""

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Methods with no side effects on the server, so replaying them is safe.
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})

# 429 means slow down, not stop. The 5xx codes below are transient by
# definition. Every other 4xx is the caller's fault and will fail again.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class Give:
    """Stop retrying. Surface the last response or exception to the caller."""

    reason: str


@dataclass(frozen=True, slots=True)
class RetryAfter:
    """Try again after this many seconds."""

    seconds: float
    attempt: int


RetryDecision = Give | RetryAfter


@dataclass(frozen=True, slots=True)
class Retry:
    """How many times to retry, and how long to back off between tries.

    Args:
        attempts: Total attempts including the first one. 1 disables retrying.
        base: Backoff for the first retry, in seconds. Doubles each time.
        max_backoff: Ceiling for a single backoff, before jitter.
        max_elapsed: Total wall-clock budget across all attempts. None
            removes the budget, which is rarely what you want.
        jitter: Draw each backoff uniformly from [0, computed]. Leave on
            unless you need reproducible timing in a test.
        retry_methods: HTTP methods that may be replayed. Add POST only if
            the endpoint is idempotent or you send an idempotency key.
        retry_status: Response codes worth trying again.
        respect_retry_after: Let a Retry-After header override the computed
            backoff. The server knows its own recovery time.
        max_retry_after: Ignore a Retry-After longer than this and give up
            instead. Some servers answer with hours.
    """

    attempts: int = 3
    base: float = 0.5
    max_backoff: float = 30.0
    max_elapsed: float | None = 60.0
    jitter: bool = True
    retry_methods: frozenset[str] = IDEMPOTENT_METHODS
    retry_status: frozenset[int] = RETRYABLE_STATUS
    respect_retry_after: bool = True
    max_retry_after: float = 120.0
    _random: random.Random = field(default_factory=random.Random, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1")
        if self.base <= 0:
            raise ValueError("base must be positive")
        if self.max_backoff < self.base:
            raise ValueError("max_backoff must be at least base")
        if self.max_elapsed is not None and self.max_elapsed <= 0:
            raise ValueError("max_elapsed must be positive")

    def should_retry(
        self,
        *,
        attempt: int,
        method: str,
        status: int | None,
        retry_after: str | None,
        elapsed: float,
    ) -> RetryDecision:
        """Decide what to do after a failed attempt.

        Args:
            attempt: How many attempts have already been made, starting at 1.
            method: HTTP method of the request.
            status: Response status, or None when the request raised before
                a response arrived, such as a connection error or timeout.
            retry_after: Raw Retry-After header value, if the server sent one.
            elapsed: Seconds spent on this request so far, across all attempts.
        """
        if attempt >= self.attempts:
            return Give("attempts exhausted")

        if method.upper() not in self.retry_methods:
            return Give(f"{method.upper()} is not retryable")

        if status is not None and status not in self.retry_status:
            return Give(f"status {status} is not retryable")

        delay = self._backoff(attempt)

        if self.respect_retry_after and retry_after is not None:
            advertised = parse_retry_after(retry_after)
            if advertised is not None:
                if advertised > self.max_retry_after:
                    return Give(f"Retry-After of {advertised}s exceeds max_retry_after")
                # The server's own estimate wins, jitter included or not.
                delay = advertised

        if self.max_elapsed is not None and elapsed + delay > self.max_elapsed:
            return Give("time budget exhausted")

        return RetryAfter(seconds=delay, attempt=attempt + 1)

    def _backoff(self, attempt: int) -> float:
        growth: float = self.base * float(2 ** (attempt - 1))
        capped: float = min(self.max_backoff, growth)
        if not self.jitter:
            return capped
        # Full jitter. Sleeping a fraction of the window beats every client
        # waking at the same moment, which is the failure this prevents.
        jittered: float = self._random.uniform(0.0, capped)
        return jittered


def parse_retry_after(value: str) -> float | None:
    """Parse a Retry-After header into seconds.

    The header carries either a number of seconds or an HTTP date. Returns
    None when the value is malformed, so the caller falls back to its own
    backoff rather than failing.
    """
    value = value.strip()
    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None

    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delta: float = (when - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delta)