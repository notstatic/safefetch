"""Asynchronous HTTP client.

This mirrors the synchronous adapter: the same limiter, retry policy and
store decide what to do, while this module awaits HTTP I/O and sleep.
Sleeping is injected so tests can advance a FakeClock without waiting.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from .clock import Clock, SystemClock
from .decision import Wait
from .limiters.base import Limiter
from .limiters.bucket import TokenBucket
from .retry import Give, Retry
from .stores.memory import MemoryStore, Store
from .sync import host_key


class AsyncSafeFetch:
    """An async httpx client that respects rate limits and retries sensibly.

    Args:
        limiter: Rate limiting policy. Defaults to 5 requests per second
            with a burst of 10.
        store: Where limiter state lives. Defaults to this process only.
        retry: Retry policy. Defaults to 3 attempts inside a 60 second budget.
        clock: Time source. Defaults to the system monotonic clock.
        key: Maps a request to a limiter key. Defaults to the host.
        max_wait: Refuse to sleep longer than this for a rate limit, and
            raise instead. Without a ceiling a misconfigured limiter can
            block a caller indefinitely.
        sleep: Async sleep function. Defaults to asyncio.sleep.
        client: An existing httpx.AsyncClient to wrap. One is created if
            omitted, and then closed with this object.
    """

    def __init__(
        self,
        *,
        limiter: Limiter[Any] | None = None,
        store: Store[Any] | None = None,
        retry: Retry | None = None,
        clock: Clock | None = None,
        key: Callable[[httpx.Request], str] = host_key,
        max_wait: float = 60.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        client: httpx.AsyncClient | None = None,
        **client_kwargs: Any,
    ) -> None:
        if max_wait <= 0:
            raise ValueError("max_wait must be positive")

        self._limiter = limiter if limiter is not None else TokenBucket(rate=5, capacity=10)
        self._store: Store[Any] = store if store is not None else MemoryStore()
        self._retry = retry if retry is not None else Retry()
        self._clock = clock if clock is not None else SystemClock()
        self._key = key
        self._max_wait = max_wait
        self._sleep = sleep

        self._owns_client = client is None
        self._client = client if client is not None else httpx.AsyncClient(**client_kwargs)

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Send a request, waiting for the limiter and retrying on failure.

        Raises:
            RuntimeError: If the limiter asks for a wait longer than max_wait.
            httpx.HTTPError: If every attempt fails with a transport error.
        """
        request = self._client.build_request(method, url, **kwargs)
        key = self._key(request)
        started = self._clock.monotonic()
        attempt = 0

        while True:
            attempt += 1
            await self._await_slot(key)

            response: httpx.Response | None = None
            error: httpx.HTTPError | None = None
            status: int | None = None
            retry_after: str | None = None

            try:
                response = await self._client.send(request)
                status = response.status_code
                retry_after = response.headers.get("Retry-After")
            except httpx.HTTPError as exc:
                error = exc

            if response is not None and status not in self._retry.retry_status:
                return response

            decision = self._retry.should_retry(
                attempt=attempt,
                method=method,
                status=status,
                retry_after=retry_after,
                elapsed=self._clock.monotonic() - started,
            )

            if isinstance(decision, Give):
                if error is not None:
                    raise error
                # response is not None here: one of the two is always set.
                assert response is not None
                return response

            await self._sleep(decision.seconds)
            request = self._client.build_request(method, url, **kwargs)

    async def _await_slot(self, key: str) -> None:
        """Wait until the limiter allows a request for this key."""
        while True:
            decision = self._store.check(key, self._limiter, self._clock.monotonic())
            if not isinstance(decision, Wait):
                return
            if decision.seconds > self._max_wait:
                raise RuntimeError(
                    f"rate limit for {key!r} requires {decision.seconds:.1f}s, "
                    f"above max_wait of {self._max_wait}s"
                )
            await self._sleep(decision.seconds)

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("POST", url, **kwargs)

    async def head(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("HEAD", url, **kwargs)

    async def aclose(self) -> None:
        """Close the underlying client, if this object created it."""
        if self._owns_client:
            await self._client.aclose()

    # Self requires Python 3.11, and the support floor here is 3.10.
    async def __aenter__(self) -> "AsyncSafeFetch":  # noqa: PYI034
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
