"""The contract every limiter satisfies.

A limiter is a pure policy: it holds configuration, never state. State is
passed in and returned, so the caller decides where it lives and how it
is guarded.
"""

from typing import Protocol, TypeVar

from ..decision import Decision

S = TypeVar("S")


class Limiter(Protocol[S]):
    """A rate limiting policy over some state type S."""

    def initial_state(self, now: float) -> S:
        """Return the state a previously unseen key starts with."""
        ...

    def check(self, state: S, now: float, cost: float = 1.0) -> tuple[S, Decision]:
        """Return the updated state and the decision for this request."""
        ...