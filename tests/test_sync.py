import httpx
import pytest

from safefetch.clock import FakeClock
from safefetch.limiters.bucket import TokenBucket
from safefetch.retry import Retry
from safefetch.stores.memory import MemoryStore
from safefetch.sync import SafeFetch


def build(responses: list[httpx.Response], **kwargs: object) -> tuple[SafeFetch, FakeClock, list[float]]:
    """A client backed by canned responses, a fake clock and a fake sleep."""
    clock = FakeClock()
    slept: list[float] = []
    remaining = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        return remaining.pop(0)

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)

    client = SafeFetch(
        clock=clock,
        sleep=sleep,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        **kwargs,  # type: ignore[arg-type]
    )
    return client, clock, slept


def test_successful_request_does_not_sleep() -> None:
    client, _, slept = build([httpx.Response(200, text="ok")])
    response = client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == []


def test_client_error_is_returned_without_retrying() -> None:
    client, _, slept = build([httpx.Response(404)])
    response = client.get("https://example.com/")
    assert response.status_code == 404
    assert slept == []


def test_server_error_is_retried_then_succeeds() -> None:
    client, _, slept = build(
        [httpx.Response(503), httpx.Response(200)],
        retry=Retry(attempts=3, base=1.0, jitter=False),
    )
    response = client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == [1.0]


def test_retry_after_header_is_honoured() -> None:
    client, _, slept = build(
        [httpx.Response(429, headers={"Retry-After": "4"}), httpx.Response(200)],
        retry=Retry(attempts=3, base=1.0, jitter=False),
    )
    response = client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == [4.0]


def test_exhausted_attempts_return_the_last_response() -> None:
    client, _, slept = build(
        [httpx.Response(500), httpx.Response(500), httpx.Response(500)],
        retry=Retry(attempts=3, base=1.0, jitter=False),
    )
    response = client.get("https://example.com/")
    assert response.status_code == 500
    assert slept == [1.0, 2.0]


def test_transport_error_is_retried_and_finally_raised() -> None:
    clock = FakeClock()
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)

    client = SafeFetch(
        clock=clock,
        sleep=sleep,
        retry=Retry(attempts=2, base=1.0, jitter=False),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(httpx.ConnectError):
        client.get("https://example.com/")
    assert slept == [1.0]


def test_limiter_makes_the_caller_wait() -> None:
    client, _, slept = build(
        [httpx.Response(200) for _ in range(3)],
        limiter=TokenBucket(rate=1, capacity=2),
        store=MemoryStore(),
    )
    for _ in range(3):
        assert client.get("https://example.com/").status_code == 200
    assert slept == [pytest.approx(1.0)]


def test_hosts_are_limited_independently() -> None:
    client, _, slept = build(
        [httpx.Response(200) for _ in range(4)],
        limiter=TokenBucket(rate=1, capacity=2),
        store=MemoryStore(),
    )
    client.get("https://a.example/")
    client.get("https://a.example/")
    client.get("https://b.example/")
    client.get("https://b.example/")
    assert slept == []


def test_wait_above_max_wait_raises() -> None:
    client, _, _ = build(
        [httpx.Response(200) for _ in range(3)],
        limiter=TokenBucket(rate=0.01, capacity=1),
        max_wait=5.0,
    )
    client.get("https://example.com/")
    with pytest.raises(RuntimeError, match="above max_wait"):
        client.get("https://example.com/")


def test_max_wait_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_wait must be positive"):
        SafeFetch(max_wait=0)


def test_context_manager_closes_its_own_client() -> None:
    with SafeFetch() as client:
        assert client is not None