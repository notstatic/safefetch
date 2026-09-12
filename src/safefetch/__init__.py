"""HTTP calls that stay inside rate limits, retry sensibly, and back off when a service is failing."""

from importlib.metadata import version

from .clock import Clock, FakeClock, SystemClock
from .decision import Allow, Decision, Wait
from .limiters import BucketState, TokenBucket
from .retry import Give, Retry, RetryAfter, RetryDecision
from .stores import MemoryStore, Store

__version__ = version("safefetch")

__all__ = [
    "Allow",
    "BucketState",
    "Clock",
    "Decision",
    "FakeClock",
    "Give",
    "MemoryStore",
    "Retry",
    "RetryAfter",
    "RetryDecision",
    "Store",
    "SystemClock",
    "TokenBucket",
    "Wait",
    "__version__",
]