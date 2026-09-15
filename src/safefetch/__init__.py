"""HTTP calls that stay inside rate limits, retry sensibly, and back off when a service is failing."""

from importlib.metadata import version

from .breaker import BreakerDecision, BreakerState, CircuitBreaker, Permit, Reject
from .clock import Clock, FakeClock, SystemClock
from .decision import Allow, Decision, Wait
from .limiters import BucketState, TokenBucket
from .retry import Give, Retry, RetryAfter, RetryDecision
from .stores import MemoryStore, Store

__version__ = version("safefetch")

__all__ = [
    "Allow",
    "BreakerDecision",
    "BreakerState",
    "BucketState",
    "CircuitBreaker",
    "Clock",
    "Decision",
    "FakeClock",
    "Give",
    "MemoryStore",
    "Permit",
    "Reject",
    "Retry",
    "RetryAfter",
    "RetryDecision",
    "Store",
    "SystemClock",
    "TokenBucket",
    "Wait",
    "__version__",
]
