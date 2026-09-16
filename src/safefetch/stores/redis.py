"""Redis storage scaffold for limiter state.

This implements the Store interface, but deliberately does not yet provide
its atomicity guarantee: GET and SET are separate operations. Concurrent
callers can read the same state, both proceed, and overwrite each other's
updates. An atomic server-side check-and-update will replace this later.

Import this module explicitly; redis is an optional dependency.
"""

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from redis import Redis

from ..decision import Decision
from ..limiters.base import Limiter

S = TypeVar("S")


class RedisStore(Generic[S]):
    """Limiter state in Redis, using a non-atomic read/check/write sequence.

    One store serves one algorithm and state encoding. Calls use Redis keys
    of the form safefetch:{algorithm}:{key}. The caller supplies serialization
    so neither the limiter protocol nor its state must know about Redis.

    Args:
        url: Connection URL used when client is omitted. Connections are
            opened lazily by redis-py on the first operation.
        algorithm: Stable namespace for this algorithm, such as token_bucket.
            Must be nonempty and contain no colons.
        encode: Encode state as bytes or text for Redis.
        decode: Restore state from bytes or text, depending on the client's
            decode_responses setting. Must be the inverse of encode.
        ttl: Idle lifetime in whole seconds, refreshed on every check,
            including denied requests. Expiry discards state, so this should
            cover the limiter's memory horizon (capacity / rate for a bucket).
        client: An existing synchronous redis.Redis client to borrow. A client
            and its connection pool are created and owned when omitted.
        **client_kwargs: Options passed to Redis.from_url when creating a client.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        algorithm: str,
        encode: Callable[[S], bytes | str],
        decode: Callable[[bytes | str], S],
        ttl: int = 60,
        client: Redis | None = None,
        **client_kwargs: Any,
    ) -> None:
        if not algorithm or ":" in algorithm:
            raise ValueError("algorithm must be nonempty and contain no colons")
        if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 1:
            raise ValueError("ttl must be a positive integer in seconds")

        self._algorithm = algorithm
        self._encode = encode
        self._decode = decode
        self._ttl = ttl
        self._owns_client = client is None
        self._client = client if client is not None else Redis.from_url(url, **client_kwargs)

    def check(self, key: str, limiter: Limiter[S], now: float, cost: float = 1.0) -> Decision:
        """Read state, apply the limiter, and write its new state with a TTL.

        A missing or expired key starts from limiter.initial_state(now).
        now is passed through unchanged; callers sharing state must use a
        common time base. This method performs blocking Redis I/O.

        Redis, codec and limiter errors propagate to the caller. A failed
        write does not return an Allow decision as if it had been persisted.
        """
        redis_key = self._redis_key(key)
        payload = self._client.get(redis_key)
        if payload is None:
            state = limiter.initial_state(now)
        else:
            if not isinstance(payload, (bytes, str)):
                raise TypeError("Redis GET must return bytes, str or None")
            state = self._decode(payload)

        new_state, decision = limiter.check(state, now, cost)
        # Intentionally racy until check-and-update moves into an atomic script.
        self._client.set(redis_key, self._encode(new_state), ex=self._ttl)
        return decision

    def reset(self, key: str) -> None:
        """Forget one key in this store's algorithm namespace."""
        self._client.delete(self._redis_key(key))

    def _redis_key(self, key: str) -> str:
        return f"safefetch:{self._algorithm}:{key}"

    def close(self) -> None:
        """Release the client and pool only if this store created them."""
        if self._owns_client:
            self._client.close()

    # Self requires Python 3.11, and the support floor here is 3.10.
    def __enter__(self) -> "RedisStore[S]":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
