import pytest

from safefetch.clock import FakeClock
from safefetch.decision import Allow, Wait
from safefetch.limiters.bucket import TokenBucket


def test_full_bucket_allows_a_burst_up_to_capacity() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(clock.monotonic())

    for _ in range(10):
        state, decision = bucket.check(state, clock.monotonic())
        assert isinstance(decision, Allow)


def test_bucket_denies_once_empty() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(clock.monotonic())

    for _ in range(10):
        state, _ = bucket.check(state, clock.monotonic())

    state, decision = bucket.check(state, clock.monotonic())
    assert isinstance(decision, Wait)
    assert decision.seconds == pytest.approx(0.2)


def test_waiting_the_advertised_time_releases_the_request() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(clock.monotonic())

    for _ in range(10):
        state, _ = bucket.check(state, clock.monotonic())

    state, decision = bucket.check(state, clock.monotonic())
    assert isinstance(decision, Wait)

    clock.advance(decision.seconds)
    state, decision = bucket.check(state, clock.monotonic())
    assert isinstance(decision, Allow)


def test_refill_is_capped_at_capacity() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(clock.monotonic())

    state, _ = bucket.check(state, clock.monotonic())
    clock.advance(3600)

    for _ in range(10):
        state, decision = bucket.check(state, clock.monotonic())
        assert isinstance(decision, Allow)

    state, decision = bucket.check(state, clock.monotonic())
    assert isinstance(decision, Wait)


def test_cost_larger_than_capacity_is_rejected() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(clock.monotonic())

    with pytest.raises(ValueError, match="can never be satisfied"):
        bucket.check(state, clock.monotonic(), cost=11)


def test_cost_must_be_positive() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(clock.monotonic())

    with pytest.raises(ValueError, match="cost must be positive"):
        bucket.check(state, clock.monotonic(), cost=0)


def test_rate_and_capacity_must_be_positive() -> None:
    with pytest.raises(ValueError, match="rate must be positive"):
        TokenBucket(rate=0, capacity=10)
    with pytest.raises(ValueError, match="capacity must be positive"):
        TokenBucket(rate=5, capacity=0)


def test_clock_moving_backwards_does_not_remove_tokens() -> None:
    bucket = TokenBucket(rate=5, capacity=10)
    state = bucket.initial_state(100.0)

    state, decision = bucket.check(state, 90.0)
    assert isinstance(decision, Allow)
    assert state.tokens == pytest.approx(9.0)