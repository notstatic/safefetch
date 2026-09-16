from dataclasses import replace
from typing import Literal

import pytest

from safefetch import BreakerState, CircuitBreaker, Permit, Reject
from safefetch.clock import FakeClock


def complete(
    breaker: CircuitBreaker, state: BreakerState, now: float, *, success: bool
) -> BreakerState:
    """Admit and complete one request using the returned state and permit."""
    state, permit = breaker.check(state, now)
    assert isinstance(permit, Permit)
    return breaker.record(state, now, permit=permit, success=success)


CLOSED = BreakerState()
OPEN = BreakerState(phase="open", generation=1, retry_at=10.0)
HALF_ONE = BreakerState(
    phase="half-open", generation=2, probes_sent=1, pending_probes=frozenset({1})
)
HALF_FULL = replace(HALF_ONE, probes_sent=2, pending_probes=frozenset({1, 2}))
HALF_LAST = replace(HALF_FULL, pending_probes=frozenset({1}))


@pytest.mark.parametrize(
    ("state", "event", "now", "expected", "decision"),
    [
        pytest.param(CLOSED, "check", 0.0, CLOSED, Permit(0), id="closed-check-stays-closed"),
        pytest.param(
            CLOSED,
            "success",
            0.0,
            replace(CLOSED, outcomes=((0.0, True),)),
            None,
            id="closed-success-stays-closed",
        ),
        pytest.param(
            CLOSED,
            "failure",
            0.0,
            replace(CLOSED, outcomes=((0.0, False),)),
            None,
            id="closed-failure-below-minimum-stays-closed",
        ),
        pytest.param(
            replace(CLOSED, outcomes=((0.0, True), (0.0, True))),
            "failure",
            1.0,
            replace(CLOSED, outcomes=((0.0, True), (0.0, True), (1.0, False))),
            None,
            id="closed-below-failure-threshold-stays-closed",
        ),
        pytest.param(
            replace(CLOSED, outcomes=((0.0, True),)),
            "failure",
            0.0,
            OPEN,
            None,
            id="closed-failure-at-threshold-opens",
        ),
        pytest.param(
            replace(CLOSED, outcomes=((0.0, False),)),
            "success",
            0.0,
            OPEN,
            None,
            id="closed-success-at-threshold-opens",
        ),
        pytest.param(
            OPEN,
            "check",
            9.0,
            OPEN,
            Reject("circuit open", 1.0),
            id="open-before-cooldown-rejects",
        ),
        pytest.param(
            OPEN,
            "check",
            10.0,
            HALF_ONE,
            Permit(2, 1),
            id="open-at-cooldown-admits-first-probe",
        ),
        pytest.param(
            OPEN,
            "check",
            100.0,
            HALF_ONE,
            Permit(2, 1),
            id="open-after-cooldown-still-requires-probes",
        ),
        pytest.param(OPEN, "success", 20.0, OPEN, None, id="open-result-cannot-close"),
        pytest.param(OPEN, "failure", 20.0, OPEN, None, id="open-result-cannot-extend-cooldown"),
        pytest.param(
            HALF_ONE,
            "check",
            10.0,
            HALF_FULL,
            Permit(2, 2),
            id="half-open-reserves-another-probe",
        ),
        pytest.param(
            HALF_FULL,
            "check",
            10.0,
            HALF_FULL,
            Reject("half-open probes exhausted"),
            id="half-open-full-rejects",
        ),
        pytest.param(
            HALF_ONE,
            "success",
            10.0,
            replace(HALF_ONE, pending_probes=frozenset()),
            None,
            id="half-open-success-waits-for-unissued-probe",
        ),
        pytest.param(
            HALF_FULL,
            "success",
            10.0,
            replace(HALF_FULL, pending_probes=frozenset({2})),
            None,
            id="half-open-success-waits-for-pending-probe",
        ),
        pytest.param(
            HALF_LAST,
            "success",
            10.0,
            BreakerState(generation=3),
            None,
            id="half-open-final-success-closes",
        ),
        pytest.param(
            HALF_FULL,
            "failure",
            12.0,
            BreakerState(phase="open", generation=3, retry_at=22.0),
            None,
            id="half-open-first-failure-immediately-reopens",
        ),
        pytest.param(
            HALF_LAST,
            "failure",
            12.0,
            BreakerState(phase="open", generation=3, retry_at=22.0),
            None,
            id="half-open-last-failure-reopens",
        ),
    ],
)
def test_transition_table(
    state: BreakerState,
    event: Literal["check", "success", "failure"],
    now: float,
    expected: BreakerState,
    decision: Permit | Reject | None,
) -> None:
    breaker = CircuitBreaker(min_calls=2, window=10, recovery_timeout=10, half_open_max_calls=2)
    if event == "check":
        result, actual_decision = breaker.check(state, now)
        assert actual_decision == decision
    else:
        permit = Permit(state.generation, 1 if state.phase == "half-open" else None)
        result = breaker.record(state, now, permit=permit, success=event == "success")
    assert result == expected


@pytest.mark.parametrize("min_calls", [1, 2, 10])
def test_all_failures_cannot_trip_before_minimum_calls(min_calls: int) -> None:
    breaker = CircuitBreaker(min_calls=min_calls)
    state = breaker.initial_state(0.0)
    for _ in range(min_calls - 1):
        state = complete(breaker, state, 0.0, success=False)
        assert state.phase == "closed"
    state = complete(breaker, state, 0.0, success=False)
    assert state.phase == "open"


@pytest.mark.parametrize(
    ("threshold", "successes", "expected"),
    [
        (0.5, (True, True, True, True), "closed"),
        (0.5, (True, True, True, False), "closed"),
        (0.5, (True, False, True, False), "open"),
        (0.5, (True, False, False, False), "open"),
        (1.0, (True, False, False, False), "closed"),
        (1.0, (False, False, False, False), "open"),
    ],
)
def test_failure_fraction_includes_successes_and_threshold_is_inclusive(
    threshold: float, successes: tuple[bool, ...], expected: str
) -> None:
    breaker = CircuitBreaker(failure_threshold=threshold, min_calls=4)
    state = breaker.initial_state(0.0)
    for success in successes:
        state = complete(breaker, state, 0.0, success=success)
    assert state.phase == expected


@pytest.mark.parametrize(
    ("elapsed", "expected"), [(9.999, "open"), (10.0, "closed"), (10.001, "closed")]
)
def test_outcomes_expire_exactly_at_window_boundary(elapsed: float, expected: str) -> None:
    clock = FakeClock(start=100)
    breaker = CircuitBreaker(min_calls=2, window=10)
    state = complete(
        breaker, breaker.initial_state(clock.monotonic()), clock.monotonic(), success=False
    )
    clock.advance(elapsed)
    state = complete(breaker, state, clock.monotonic(), success=True)
    assert state.phase == expected


def test_expired_successes_do_not_dilute_failure_fraction() -> None:
    breaker = CircuitBreaker(failure_threshold=0.75, min_calls=2, window=10)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=True)
    state = complete(breaker, state, 8.0, success=False)
    assert state.phase == "closed"
    state = complete(breaker, state, 10.0, success=False)
    assert state.phase == "open"


def test_minimum_only_counts_outcomes_still_in_the_window() -> None:
    breaker = CircuitBreaker(min_calls=2, window=10)
    state = breaker.initial_state(0.0)
    for now in (0.0, 10.0, 20.0, 30.0):
        state = complete(breaker, state, now, success=False)
        assert state.phase == "closed"
        assert state.outcomes == ((now, False),)


def test_check_prunes_expired_outcomes_without_counting_unfinished_requests() -> None:
    breaker = CircuitBreaker(min_calls=2, window=10)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    for _ in range(20):
        state, permit = breaker.check(state, 9.0)
        assert isinstance(permit, Permit)
        assert state.outcomes == ((0.0, False),)
    state, permit = breaker.check(state, 10.0)
    assert isinstance(permit, Permit)
    assert state.outcomes == ()


def test_window_uses_completion_time_instead_of_admission_time() -> None:
    breaker = CircuitBreaker(min_calls=2, window=10)
    state, permit = breaker.check(breaker.initial_state(0.0), 0.0)
    assert isinstance(permit, Permit)
    state = breaker.record(state, 100.0, permit=permit, success=False)
    state = complete(breaker, state, 101.0, success=False)
    assert state.phase == "open"
    assert state.retry_at == 131.0


def test_rejected_requests_do_not_change_state_or_extend_cooldown() -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    for now in (0.0, 1.0, 9.0, 9.999):
        result, decision = breaker.check(state, now)
        assert result == state
        assert isinstance(decision, Reject)
        assert decision.retry_after == pytest.approx(10.0 - now)


def test_probe_limit_is_reserved_before_results_arrive() -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10, half_open_max_calls=3)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    permits: list[Permit] = []
    for _ in range(3):
        state, permit = breaker.check(state, 10.0)
        assert isinstance(permit, Permit)
        permits.append(permit)
    assert len(set(permits)) == 3
    for _ in range(5):
        result, decision = breaker.check(state, 10.0)
        assert result == state
        assert decision == Reject("half-open probes exhausted")


@pytest.mark.parametrize("order", [(0, 1, 2), (2, 0, 1)])
def test_all_probes_must_succeed_even_when_results_arrive_out_of_order(
    order: tuple[int, ...],
) -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10, half_open_max_calls=3)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    permits: list[Permit] = []
    for _ in range(3):
        state, permit = breaker.check(state, 10.0)
        assert isinstance(permit, Permit)
        permits.append(permit)
    for index in order[:-1]:
        state = breaker.record(state, 11.0, permit=permits[index], success=True)
        assert state.phase == "half-open"
        state, decision = breaker.check(state, 11.0)
        assert isinstance(decision, Reject)
    state = breaker.record(state, 12.0, permit=permits[order[-1]], success=True)
    assert state.phase == "closed"
    assert state.outcomes == ()


def test_sequential_probes_do_not_close_early() -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10, half_open_max_calls=3)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    for _ in range(2):
        state = complete(breaker, state, 10.0, success=True)
        assert state.phase == "half-open"
    state = complete(breaker, state, 10.0, success=True)
    assert state.phase == "closed"


@pytest.mark.parametrize("success", [True, False])
def test_duplicate_probe_results_are_ignored(success: bool) -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10, half_open_max_calls=2)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    state, permit = breaker.check(state, 10.0)
    assert isinstance(permit, Permit)
    state = breaker.record(state, 10.0, permit=permit, success=True)
    assert breaker.record(state, 11.0, permit=permit, success=success) == state
    state = complete(breaker, state, 11.0, success=True)
    assert state.phase == "closed"


def test_failure_starts_a_full_new_cooldown_and_probe_round() -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10, half_open_max_calls=2)
    state = complete(breaker, breaker.initial_state(0.0), 0.0, success=False)
    state, old_probe = breaker.check(state, 10.0)
    assert isinstance(old_probe, Permit)
    state = complete(breaker, state, 12.0, success=False)
    state, decision = breaker.check(state, 20.0)
    assert decision == Reject("circuit open", 2.0)
    state, new_probe = breaker.check(state, 22.0)
    assert isinstance(new_probe, Permit)
    assert new_probe.probe_id == old_probe.probe_id
    assert new_probe.generation != old_probe.generation
    for success in (True, False):
        assert breaker.record(state, 22.0, permit=old_probe, success=success) == state
    state = breaker.record(state, 22.0, permit=new_probe, success=True)
    state = complete(breaker, state, 22.0, success=True)
    assert state.phase == "closed"


@pytest.mark.parametrize("success", [True, False])
def test_late_closed_results_cannot_affect_open_or_recovering_circuit(success: bool) -> None:
    breaker = CircuitBreaker(min_calls=1, recovery_timeout=10)
    state, old_permit = breaker.check(breaker.initial_state(0.0), 0.0)
    assert isinstance(old_permit, Permit)
    state = complete(breaker, state, 0.0, success=False)
    assert breaker.record(state, 1.0, permit=old_permit, success=success) == state
    state, probe = breaker.check(state, 10.0)
    assert isinstance(probe, Permit)
    assert breaker.record(state, 10.0, permit=old_permit, success=success) == state
    state = breaker.record(state, 10.0, permit=probe, success=True)
    assert state.phase == "closed"
    assert breaker.record(state, 10.0, permit=old_permit, success=success) == state
    assert breaker.record(state, 10.0, permit=probe, success=success) == state


def test_recovery_resets_the_window_and_minimum_call_gate() -> None:
    breaker = CircuitBreaker(min_calls=2, recovery_timeout=1)
    state = breaker.initial_state(0.0)
    for _ in range(2):
        state = complete(breaker, state, 0.0, success=False)
    state = complete(breaker, state, 1.0, success=True)
    assert state.phase == "closed"
    state = complete(breaker, state, 1.0, success=False)
    assert state.phase == "closed"
    state = complete(breaker, state, 1.0, success=False)
    assert state.phase == "open"


def test_policy_has_no_hidden_state_and_leaves_input_snapshots_unchanged() -> None:
    breaker = CircuitBreaker(min_calls=1)
    initial = breaker.initial_state(100.0)
    first = complete(breaker, initial, 100.0, success=False)
    assert initial == BreakerState()
    assert first.phase == "open"
    assert complete(breaker, initial, 100.0, success=False) == first
    second = complete(breaker, initial, 100.0, success=True)
    assert second.phase == "closed"
    assert first.phase == "open"


@pytest.mark.parametrize("value", [0, -0.1, 1.1, float("nan"), float("inf"), -float("inf")])
def test_failure_threshold_must_be_a_finite_fraction(value: float) -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker(failure_threshold=value)


@pytest.mark.parametrize("name", ["window", "recovery_timeout"])
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), -float("inf")])
def test_durations_must_be_finite_and_positive(name: str, value: float) -> None:
    with pytest.raises(ValueError, match=f"{name} must be finite and positive"):
        CircuitBreaker(**{name: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["min_calls", "half_open_max_calls"])
@pytest.mark.parametrize("value", [0, -1, 1.5, True, False])
def test_call_counts_must_be_positive_integers(name: str, value: object) -> None:
    with pytest.raises(ValueError, match=f"{name} must be a positive integer"):
        CircuitBreaker(**{name: value})  # type: ignore[arg-type]
