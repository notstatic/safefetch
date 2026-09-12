import random

import pytest

from safefetch.retry import Give, Retry, RetryAfter, parse_retry_after


def fixed_retry(**kwargs: object) -> Retry:
    """A Retry with jitter off, so backoff values are exact."""
    defaults: dict[str, object] = {"jitter": False, "max_elapsed": None}
    defaults.update(kwargs)
    return Retry(**defaults)  # type: ignore[arg-type]


def test_backoff_doubles() -> None:
    retry = fixed_retry(attempts=5, base=1.0)
    delays = []
    for attempt in range(1, 5):
        decision = retry.should_retry(
            attempt=attempt, method="GET", status=500, retry_after=None, elapsed=0.0
        )
        assert isinstance(decision, RetryAfter)
        delays.append(decision.seconds)
    assert delays == [1.0, 2.0, 4.0, 8.0]


def test_backoff_is_capped() -> None:
    retry = fixed_retry(attempts=10, base=1.0, max_backoff=5.0)
    decision = retry.should_retry(
        attempt=8, method="GET", status=500, retry_after=None, elapsed=0.0
    )
    assert isinstance(decision, RetryAfter)
    assert decision.seconds == 5.0


def test_last_attempt_gives_up() -> None:
    retry = fixed_retry(attempts=3)
    decision = retry.should_retry(
        attempt=3, method="GET", status=500, retry_after=None, elapsed=0.0
    )
    assert isinstance(decision, Give)
    assert decision.reason == "attempts exhausted"


def test_non_idempotent_method_is_not_retried() -> None:
    retry = fixed_retry()
    decision = retry.should_retry(
        attempt=1, method="POST", status=500, retry_after=None, elapsed=0.0
    )
    assert isinstance(decision, Give)


def test_post_can_be_opted_in() -> None:
    retry = fixed_retry(retry_methods=frozenset({"GET", "POST"}))
    decision = retry.should_retry(
        attempt=1, method="POST", status=500, retry_after=None, elapsed=0.0
    )
    assert isinstance(decision, RetryAfter)


def test_client_errors_are_not_retried() -> None:
    retry = fixed_retry()
    for status in (400, 401, 403, 404, 422):
        decision = retry.should_retry(
            attempt=1, method="GET", status=status, retry_after=None, elapsed=0.0
        )
        assert isinstance(decision, Give), status


def test_429_is_retried() -> None:
    retry = fixed_retry()
    decision = retry.should_retry(
        attempt=1, method="GET", status=429, retry_after=None, elapsed=0.0
    )
    assert isinstance(decision, RetryAfter)


def test_connection_error_is_retried() -> None:
    retry = fixed_retry()
    decision = retry.should_retry(
        attempt=1, method="GET", status=None, retry_after=None, elapsed=0.0
    )
    assert isinstance(decision, RetryAfter)


def test_retry_after_overrides_backoff() -> None:
    retry = fixed_retry(base=1.0)
    decision = retry.should_retry(
        attempt=1, method="GET", status=429, retry_after="7", elapsed=0.0
    )
    assert isinstance(decision, RetryAfter)
    assert decision.seconds == 7.0


def test_absurd_retry_after_gives_up() -> None:
    retry = fixed_retry(max_retry_after=60.0)
    decision = retry.should_retry(
        attempt=1, method="GET", status=503, retry_after="3600", elapsed=0.0
    )
    assert isinstance(decision, Give)


def test_malformed_retry_after_falls_back_to_backoff() -> None:
    retry = fixed_retry(base=2.0)
    decision = retry.should_retry(
        attempt=1, method="GET", status=503, retry_after="soon", elapsed=0.0
    )
    assert isinstance(decision, RetryAfter)
    assert decision.seconds == 2.0


def test_time_budget_stops_before_exceeding() -> None:
    retry = Retry(attempts=10, base=1.0, jitter=False, max_elapsed=10.0)
    decision = retry.should_retry(
        attempt=1, method="GET", status=500, retry_after=None, elapsed=9.5
    )
    assert isinstance(decision, Give)
    assert decision.reason == "time budget exhausted"


def test_jitter_stays_within_the_window() -> None:
    retry = Retry(attempts=50, base=1.0, max_elapsed=None, _random=random.Random(0))
    for attempt in range(1, 6):
        decision = retry.should_retry(
            attempt=attempt, method="GET", status=500, retry_after=None, elapsed=0.0
        )
        assert isinstance(decision, RetryAfter)
        ceiling = min(30.0, 1.0 * 2 ** (attempt - 1))
        assert 0.0 <= decision.seconds <= ceiling


def test_jitter_produces_different_values() -> None:
    retry = Retry(attempts=50, base=10.0, max_elapsed=None, _random=random.Random(1))
    values = {
        retry.should_retry(
            attempt=3, method="GET", status=500, retry_after=None, elapsed=0.0
        ).seconds  # type: ignore[union-attr]
        for _ in range(20)
    }
    assert len(values) > 1


def test_attempts_must_be_positive() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        Retry(attempts=0)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("0", 0.0), ("5", 5.0), ("5.5", 5.5), ("-3", 0.0), ("", None), ("later", None)],
)
def test_parse_retry_after_seconds(value: str, expected: float | None) -> None:
    assert parse_retry_after(value) == expected


def test_parse_retry_after_http_date() -> None:
    past = parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT")
    assert past == 0.0