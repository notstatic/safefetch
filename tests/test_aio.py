import asyncio
from collections.abc import Callable

import httpx
import pytest

from safefetch.aio import AsyncSafeFetch
from safefetch.clock import FakeClock
from safefetch.limiters.bucket import BucketState, TokenBucket
from safefetch.retry import Retry
from safefetch.stores.memory import MemoryStore
from safefetch.sync import SafeFetch

pytestmark = pytest.mark.asyncio


def build(
    outcomes: list[httpx.Response | httpx.HTTPError], **kwargs: object
) -> tuple[AsyncSafeFetch, FakeClock, list[float]]:
    """Canned responses and errors, with an async sleep advancing fake time."""
    clock = FakeClock()
    slept: list[float] = []
    remaining = list(outcomes)

    def handler(request: httpx.Request) -> httpx.Response:
        outcome = remaining.pop(0)
        if isinstance(outcome, httpx.HTTPError):
            raise outcome
        return outcome

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)

    client = AsyncSafeFetch(
        clock=clock,
        sleep=sleep,
        transport=httpx.MockTransport(handler),
        **kwargs,  # type: ignore[arg-type]
    )
    return client, clock, slept


async def test_successful_request_does_not_sleep() -> None:
    client, _, slept = build([httpx.Response(200, text="ok")])
    async with client:
        response = await client.get("https://example.com/")
    assert response.status_code == 200
    assert response.text == "ok"
    assert slept == []


async def test_client_error_is_returned_without_retrying() -> None:
    client, _, slept = build([httpx.Response(404)])
    async with client:
        response = await client.get("https://example.com/")
    assert response.status_code == 404
    assert slept == []


async def test_server_error_is_retried_then_succeeds() -> None:
    client, _, slept = build(
        [httpx.Response(503), httpx.Response(200)],
        retry=Retry(attempts=3, base=1.0, jitter=False),
    )
    async with client:
        response = await client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == [1.0]


async def test_retry_after_header_is_honoured() -> None:
    client, _, slept = build(
        [httpx.Response(429, headers={"Retry-After": "4"}), httpx.Response(200)],
        retry=Retry(attempts=3, base=1.0, jitter=False),
    )
    async with client:
        response = await client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == [4.0]


async def test_exhausted_attempts_return_the_last_response() -> None:
    last = httpx.Response(500)
    client, _, slept = build(
        [httpx.Response(500), httpx.Response(500), last],
        retry=Retry(attempts=3, base=1.0, jitter=False),
    )
    async with client:
        response = await client.get("https://example.com/")
    assert response is last
    assert slept == [1.0, 2.0]


async def test_transport_error_is_retried_and_finally_raised() -> None:
    last = httpx.ConnectError("still down")
    client, _, slept = build(
        [httpx.ConnectError("down"), last],
        retry=Retry(attempts=2, base=1.0, jitter=False),
    )
    async with client:
        with pytest.raises(httpx.ConnectError) as raised:
            await client.get("https://example.com/")
    assert raised.value is last
    assert slept == [1.0]


async def test_transport_error_can_recover() -> None:
    client, _, slept = build(
        [httpx.ReadTimeout("slow"), httpx.Response(200)],
        retry=Retry(base=1.0, jitter=False),
    )
    async with client:
        response = await client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == [1.0]


async def test_limiter_makes_the_caller_wait() -> None:
    client, _, slept = build(
        [httpx.Response(200) for _ in range(3)],
        limiter=TokenBucket(rate=1, capacity=2),
        store=MemoryStore(),
    )
    async with client:
        for _ in range(3):
            assert (await client.get("https://example.com/")).status_code == 200
    assert slept == [pytest.approx(1.0)]


async def test_hosts_are_limited_independently() -> None:
    client, _, slept = build(
        [httpx.Response(200) for _ in range(4)],
        limiter=TokenBucket(rate=1, capacity=2),
        store=MemoryStore(),
    )
    async with client:
        await client.get("https://a.example/")
        await client.get("https://a.example/")
        await client.get("https://b.example/")
        await client.get("https://b.example/")
    assert slept == []


async def test_custom_key_can_share_a_limit_across_hosts() -> None:
    client, _, slept = build(
        [httpx.Response(200), httpx.Response(200)],
        limiter=TokenBucket(rate=1, capacity=1),
        key=lambda request: "shared",
    )
    async with client:
        await client.get("https://a.example/")
        await client.get("https://b.example/")
    assert slept == [1.0]


async def test_wait_above_max_wait_raises_before_sending() -> None:
    client, _, slept = build(
        [httpx.Response(200)],
        limiter=TokenBucket(rate=0.01, capacity=1),
        max_wait=5.0,
    )
    async with client:
        await client.get("https://example.com/")
        with pytest.raises(RuntimeError, match="above max_wait"):
            await client.get("https://example.com/")
    assert slept == []


@pytest.mark.parametrize("max_wait", [0, -1])
async def test_max_wait_must_be_positive(max_wait: float) -> None:
    with pytest.raises(ValueError, match="max_wait must be positive"):
        AsyncSafeFetch(max_wait=max_wait)


async def test_wait_equal_to_max_wait_is_allowed() -> None:
    client, _, slept = build(
        [httpx.Response(200), httpx.Response(200)],
        limiter=TokenBucket(rate=1, capacity=1),
        max_wait=1.0,
    )
    async with client:
        await client.get("https://example.com/")
        await client.get("https://example.com/")
    assert slept == [1.0]


async def test_retry_also_waits_for_a_limiter_slot() -> None:
    client, clock, slept = build(
        [httpx.Response(503), httpx.Response(200)],
        limiter=TokenBucket(rate=0.25, capacity=1),
        retry=Retry(base=1.0, jitter=False),
    )
    async with client:
        response = await client.get("https://example.com/")
    assert response.status_code == 200
    assert slept == [1.0, 3.0]
    assert clock.monotonic() == 4.0


async def test_limiter_wait_counts_towards_retry_budget() -> None:
    client, _, slept = build(
        [httpx.Response(200), httpx.Response(503)],
        limiter=TokenBucket(rate=0.25, capacity=1),
        retry=Retry(base=1.0, jitter=False, max_elapsed=4.0),
    )
    async with client:
        await client.get("https://example.com/")
        response = await client.get("https://example.com/")
    assert response.status_code == 503
    assert slept == [4.0]


async def test_post_is_not_retried_by_default() -> None:
    client, _, slept = build([httpx.Response(503)])
    async with client:
        response = await client.post("https://example.com/", json={"item": 1})
    assert response.status_code == 503
    assert response.request.method == "POST"
    assert slept == []


async def test_head_is_retried() -> None:
    client, _, slept = build(
        [httpx.Response(503), httpx.Response(200)],
        retry=Retry(base=1.0, jitter=False),
    )
    async with client:
        response = await client.head("https://example.com/")
    assert response.status_code == 200
    assert response.request.method == "HEAD"
    assert slept == [1.0]


async def test_retry_rebuilds_request_with_client_and_request_options() -> None:
    first, second = httpx.Response(503), httpx.Response(200)
    client, _, slept = build(
        [first, second],
        retry=Retry(base=1.0, jitter=False, retry_methods=frozenset({"POST"})),
        base_url="https://example.com/api/",
        headers={"X-Client": "default"},
    )
    async with client:
        await client.post("items", params={"page": 2}, headers={"X-Call": "value"}, content=b"body")
    assert first.request is not second.request
    for response in (first, second):
        assert response.request.method == "POST"
        assert str(response.request.url) == "https://example.com/api/items?page=2"
        assert response.request.headers["X-Client"] == "default"
        assert response.request.headers["X-Call"] == "value"
        assert response.request.content == b"body"
    assert slept == [1.0]


class TrackedTransport(httpx.MockTransport):
    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        super().__init__(handler)
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True
        await super().aclose()


async def test_context_manager_closes_its_own_client_on_error() -> None:
    transport = TrackedTransport(lambda request: httpx.Response(200))
    with pytest.raises(ValueError, match="caller failed"):
        async with AsyncSafeFetch(transport=transport) as client:
            assert (await client.get("https://example.com/")).status_code == 200
            raise ValueError("caller failed")
    assert transport.closed


async def test_aclose_closes_its_own_client() -> None:
    transport = TrackedTransport(lambda request: httpx.Response(200))
    client = AsyncSafeFetch(transport=transport)
    await client.aclose()
    assert transport.closed
    with pytest.raises(RuntimeError, match="client has been closed"):
        await client.get("https://example.com/")


async def test_injected_client_stays_open() -> None:
    transport = TrackedTransport(lambda request: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as underlying:
        async with AsyncSafeFetch(client=underlying) as client:
            assert (await client.get("https://example.com/")).status_code == 200
        await client.aclose()
        assert not underlying.is_closed
        assert not transport.closed
        assert (await underlying.get("https://example.com/")).status_code == 200
    assert transport.closed


async def test_sync_and_async_can_share_the_same_store() -> None:
    store = MemoryStore[BucketState]()
    limiter = TokenBucket(rate=1, capacity=1)
    client, clock, slept = build([httpx.Response(200)], limiter=limiter, store=store)
    with SafeFetch(
        limiter=limiter,
        store=store,
        clock=clock,
        sleep=clock.advance,
        transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    ) as sync_client:
        sync_client.get("https://example.com/")
        async with client:
            await client.get("https://example.com/")
    assert slept == [1.0]


async def test_concurrent_requests_have_independent_retry_attempts() -> None:
    attempts = {"/a": 0, "/b": 0}
    slept: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        attempts[request.url.path] += 1
        await asyncio.sleep(0)
        success = attempts[request.url.path] >= (2 if request.url.path == "/a" else 3)
        return httpx.Response(200 if success else 503)

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        await asyncio.sleep(0)

    async with AsyncSafeFetch(
        clock=FakeClock(),
        sleep=sleep,
        retry=Retry(attempts=3, base=1.0, jitter=False),
        transport=httpx.MockTransport(handler),
    ) as client:
        responses = await asyncio.gather(
            client.get("https://example.com/a"), client.get("https://example.com/b")
        )
    assert [response.status_code for response in responses] == [200, 200]
    assert attempts == {"/a": 2, "/b": 3}
    assert sorted(slept) == [1.0, 1.0, 2.0]


async def test_concurrent_waiters_recheck_the_shared_limit() -> None:
    clock = FakeClock()
    wakeups: asyncio.Queue[None] = asyncio.Queue()
    waits: asyncio.Queue[float] = asyncio.Queue()
    sent_at: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent_at.append(clock.monotonic())
        return httpx.Response(200)

    async def sleep(seconds: float) -> None:
        await waits.put(seconds)
        await wakeups.get()

    async with AsyncSafeFetch(
        clock=clock,
        sleep=sleep,
        limiter=TokenBucket(rate=1, capacity=1),
        transport=httpx.MockTransport(handler),
    ) as client:
        await client.get("https://example.com/")
        pending = asyncio.gather(
            client.get("https://example.com/"), client.get("https://example.com/")
        )
        try:
            assert await asyncio.wait_for(waits.get(), timeout=1) == 1.0
            assert await asyncio.wait_for(waits.get(), timeout=1) == 1.0
            clock.advance(1.0)
            wakeups.put_nowait(None)
            wakeups.put_nowait(None)
            assert await asyncio.wait_for(waits.get(), timeout=1) == 1.0
            assert sent_at == [0.0, 1.0]
            clock.advance(1.0)
            wakeups.put_nowait(None)
            responses = await asyncio.wait_for(pending, timeout=1)
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    assert [response.status_code for response in responses] == [200, 200]
    assert sent_at == [0.0, 1.0, 2.0]


@pytest.mark.parametrize("stage", ["send", "retry", "limiter"])
async def test_cancellation_propagates_without_another_attempt(stage: str) -> None:
    entered = asyncio.Event()
    calls = 0

    async def pause() -> None:
        entered.set()
        await asyncio.Event().wait()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if stage == "send":
            await pause()
        return httpx.Response(503 if stage == "retry" else 200)

    async def sleep(seconds: float) -> None:
        await pause()

    async with AsyncSafeFetch(
        clock=FakeClock(),
        sleep=sleep,
        limiter=TokenBucket(rate=1, capacity=1),
        transport=httpx.MockTransport(handler),
    ) as client:
        if stage == "limiter":
            await client.get("https://example.com/")
        task = asyncio.create_task(client.get("https://example.com/"))
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert calls == 1


async def test_default_sleep_yields_to_the_event_loop() -> None:
    other_task_ran = False
    calls = 0

    def other_task() -> None:
        nonlocal other_task_ran
        other_task_ran = True

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            asyncio.get_running_loop().call_soon(other_task)
            return httpx.Response(503, headers={"Retry-After": "0"})
        assert other_task_ran
        return httpx.Response(200)

    async with AsyncSafeFetch(transport=httpx.MockTransport(handler)) as client:
        response = await client.get("https://example.com/")
    assert response.status_code == 200
    assert calls == 2
