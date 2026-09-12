"""Synchronous HTTP client.

This is the only module in the package that performs I/O and sleeps. The
limiter, retry policy and store above it all decide and return, so this
adapter stays thin: it asks what to do, then does it.

Sleeping is injected rather than called directly. Tests pass a function
that advances a FakeClock, which keeps the suite fast and deterministic
even though the code under test is the real request loop.
"""

import time
from collections.abc import Callable
from typing import Any, Self

import httpx

from .clock import Clock, SystemClock
from .decision import Wait
from .limiters.base import Limiter
from .limiters.bucket import TokenBucket
from .retry import Give, Retry
from .stores.memory import MemoryStore, Store


def host_key(request: httpx.Request) -> str:
    """Default key: one bucket per host.

    Rate limits are set by whoever runs the server, so the host is the unit
    that matters. Keying per URL would let a crawler hammer one host through
    a thousand different paths.
    """
    return request.url.host


class SafeFetch:
    """An httpx client that respects rate limits and retries sensibly.

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
        client: An existing httpx.Client to wrap. One is created if omitted,
            and then closed with this object.
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
        sleep: Callable[[float], None] = time.sleep,
        client: httpx.Client | None = None,
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
        self._client = client if client is not None else httpx.Client(**client_kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
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
            self._await_slot(key)

            response: httpx.Response | None = None
            error: httpx.HTTPError | None = None
            status: int | None = None
            retry_after: str | None = None

            try:
                response = self._client.send(request)
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

            self._sleep(decision.seconds)
            # A sent request cannot be sent twice, so build a fresh one.
            request = self._client.build_request(method, url, **kwargs)

    def _await_slot(self, key: str) -> None:
        """Block until the limiter allows a request for this key."""
        while True:
            decision = self._store.check(key, self._limiter, self._clock.monotonic())
            if not isinstance(decision, Wait):
                return
            if decision.seconds > self._max_wait:
                raise RuntimeError(
                    f"rate limit for {key!r} requires {decision.seconds:.1f}s, "
                    f"above max_wait of {self._max_wait}s"
                )
            self._sleep(decision.seconds)

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def head(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("HEAD", url, **kwargs)

    def close(self) -> None:
        """Close the underlying client, if this object created it."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()