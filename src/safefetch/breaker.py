"""Circuit breaker policy over caller-owned state and monotonic time.

No I/O, locks, clocks or sleeps live here. A store must apply each check and
record atomically to the latest state for a key. The caller runs the request
between those operations and decides whether its outcome counts as a failure.
"""

import math
from dataclasses import dataclass, replace
from typing import Literal


@dataclass(frozen=True, slots=True)
class Permit:
    """Permission for one request; pass it back when recording the result.

    A generation identifies one visit to a state, so delayed results cannot
    affect a later recovery attempt. Only half-open permits have a probe ID.
    """

    generation: int
    probe_id: int | None = None


@dataclass(frozen=True, slots=True)
class Reject:
    """Do not send a request.

    retry_after is the remaining open cooldown, or None when all half-open
    probes have been issued and their completion time is unknown.
    """

    reason: str
    retry_after: float | None = None


BreakerDecision = Permit | Reject


@dataclass(frozen=True, slots=True)
class BreakerState:
    """An immutable snapshot of one circuit.

    outcomes holds (completion time, success) pairs for the closed window.
    probes_sent counts all probes issued in this half-open round, including
    completed ones. pending_probes holds the IDs still awaiting a result.
    """

    phase: Literal["closed", "open", "half-open"] = "closed"
    generation: int = 0
    outcomes: tuple[tuple[float, bool], ...] = ()
    retry_at: float = 0.0
    probes_sent: int = 0
    pending_probes: frozenset[int] = frozenset()


@dataclass(frozen=True, slots=True)
class CircuitBreaker:
    """Stop calls to a failing service, then admit a bounded recovery round.

    Closed circuits admit requests and count completed outcomes in a sliding
    time window. Open circuits reject requests until the cooldown expires.
    Half-open circuits admit at most half_open_max_calls probes in total;
    every probe must succeed to close, while any failure immediately reopens.

    Args:
        failure_threshold: Failure fraction that trips the circuit, inclusive.
            Must be in (0, 1]. Defaults to 0.5.
        min_calls: Minimum completed calls in the current window before the
            threshold can trip. Defaults to 10.
        window: Rolling window duration in seconds. Outcomes exactly this
            old expire. Defaults to 60 seconds.
        recovery_timeout: Seconds to reject calls after opening. Defaults to 30.
        half_open_max_calls: Total probes admitted per recovery round. Defaults
            to 1. Successful probes do not replenish the allowance.
    """

    failure_threshold: float = 0.5
    min_calls: int = 10
    window: float = 60.0
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1

    def __post_init__(self) -> None:
        if not 0 < self.failure_threshold <= 1:
            raise ValueError("failure_threshold must be in (0, 1]")
        for name in ("min_calls", "half_open_max_calls"):
            count = getattr(self, name)
            if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("window", "recovery_timeout"):
            seconds = getattr(self, name)
            if not math.isfinite(seconds) or seconds <= 0:
                raise ValueError(f"{name} must be finite and positive")

    def initial_state(self, now: float) -> BreakerState:
        """Start closed with no outcomes, regardless of the clock's origin."""
        return BreakerState()

    def check(self, state: BreakerState, now: float) -> tuple[BreakerState, BreakerDecision]:
        """Admit or reject a request, reserving a probe before it is sent.

        now is caller-supplied monotonic time. Persist the returned state even
        when a request is rejected. An expired cooldown moves to half-open
        only when a caller asks to send a request.
        """
        if state.phase == "closed":
            state = replace(state, outcomes=self._recent(state, now))
            return state, Permit(state.generation)

        if state.phase == "open":
            if now < state.retry_at:
                return state, Reject("circuit open", state.retry_at - now)
            state = BreakerState(phase="half-open", generation=state.generation + 1)

        if state.probes_sent >= self.half_open_max_calls:
            return state, Reject("half-open probes exhausted")

        probe_id = state.probes_sent + 1
        state = replace(
            state,
            probes_sent=probe_id,
            pending_probes=state.pending_probes | {probe_id},
        )
        return state, Permit(state.generation, probe_id)

    def record(
        self, state: BreakerState, now: float, *, permit: Permit, success: bool
    ) -> BreakerState:
        """Record one admitted request's outcome using its original permit.

        Record exactly once per request, using the latest state for the same
        key and the completion time. Report cancelled or abandoned probes as
        failures so they do not leave a recovery round waiting indefinitely.
        Results from older generations and repeated probe results are ignored.
        Recovery starts a fresh closed window; probe outcomes are not carried
        into it.
        """
        if permit.generation != state.generation:
            return state

        if state.phase == "closed" and permit.probe_id is None:
            outcomes = self._recent(state, now) + ((now, success),)
            failures = sum(not succeeded for _, succeeded in outcomes)
            if (
                len(outcomes) >= self.min_calls
                and failures / len(outcomes) >= self.failure_threshold
            ):
                return self._open(state, now)
            return replace(state, outcomes=outcomes)

        if state.phase == "half-open" and permit.probe_id in state.pending_probes:
            if not success:
                return self._open(state, now)
            pending = state.pending_probes - {permit.probe_id}
            if state.probes_sent == self.half_open_max_calls and not pending:
                return BreakerState(generation=state.generation + 1)
            return replace(state, pending_probes=pending)

        return state

    def _recent(self, state: BreakerState, now: float) -> tuple[tuple[float, bool], ...]:
        return tuple(outcome for outcome in state.outcomes if outcome[0] > now - self.window)

    def _open(self, state: BreakerState, now: float) -> BreakerState:
        return BreakerState(
            phase="open", generation=state.generation + 1, retry_at=now + self.recovery_timeout
        )
