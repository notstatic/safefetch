"""The outcome of a limiter check."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Allow:
    """The request may proceed now."""


@dataclass(frozen=True, slots=True)
class Wait:
    """The request must wait this many seconds before proceeding."""

    seconds: float


Decision = Allow | Wait