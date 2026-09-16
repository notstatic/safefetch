"""Atomic token-bucket storage in standalone Redis.

The Lua script applies the same arithmetic as TokenBucket in one server-side
operation. Time is supplied by the caller, never read from Redis. Import this
module explicitly; redis remains an optional dependency.
"""

import math
from importlib.resources import files
from typing import Any

from redis import Redis

from ..decision import Allow, Decision, Wait
from ..limiters.base import Limiter
from ..limiters.bucket import BucketState, TokenBucket

_TOKEN_BUCKET_SCRIPT = files(__package__).joinpath("token_bucket.lua").read_text(encoding="utf-8")


class RedisStore:
    """Token-bucket state checked and updated atomically in Redis.

    State uses the keys safefetch:{algorithm}:{key} and a fixed JSON schema
    with tokens and updated_at fields. Other limiter algorithms need their
    own Lua implementation; unsupported limiters are rejected before I/O.

    Args:
        url: Connection URL used when client is omitted. Connections open
            lazily on the first Redis operation.
        algorithm: Stable namespace for this bucket configuration. Defaults
            to token_bucket. Must be nonempty and contain no colons.
        ttl: Idle lifetime in whole seconds, refreshed on every check,
            including denials. Use at least capacity / rate to avoid losing
            limiter state before the bucket would naturally refill.
        max_keys: Optional LRU limit shared by all stores in this namespace.
            Like MemoryStore, eviction makes the next call start fresh.
        client: Existing synchronous redis.Redis client to borrow. Otherwise
            this object creates and owns the client and its connection pool.
        **client_kwargs: Options passed to Redis.from_url when creating a client.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        algorithm: str = "token_bucket",
        ttl: int = 60,
        max_keys: int | None = None,
        client: Redis | None = None,
        **client_kwargs: Any,
    ) -> None:
        if not algorithm or ":" in algorithm:
            raise ValueError("algorithm must be nonempty and contain no colons")
        if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 1:
            raise ValueError("ttl must be a positive integer in seconds")
        if max_keys is not None and (
            isinstance(max_keys, bool) or not isinstance(max_keys, int) or max_keys < 1
        ):
            raise ValueError("max_keys must be a positive integer")

        self._algorithm = algorithm
        self._ttl = ttl
        self._max_keys = max_keys
        self._lru_key = f"safefetch_lru:{algorithm}"
        self._owns_client = client is None
        self._client = client if client is not None else Redis.from_url(url, **client_kwargs)
        # Registration is local; redis-py loads the script lazily and reloads
        # it after NOSCRIPT (for example, after a Redis restart).
        self._script = self._client.register_script(_TOKEN_BUCKET_SCRIPT)

    def check(
        self, key: str, limiter: Limiter[BucketState], now: float, cost: float = 1.0
    ) -> Decision:
        """Apply the bucket and refresh its TTL in one atomic script call.

        now is passed through unchanged. Callers sharing state must use a
        common time base. Redis I/O is blocking, and Redis errors propagate.
        Waits are rounded up to integer milliseconds in Lua and converted
        back to seconds here, so Redis cannot truncate a fractional wait.
        """
        if type(limiter) is not TokenBucket:
            raise TypeError("RedisStore only supports TokenBucket")
        if not math.isfinite(now):
            raise ValueError("now must be finite")
        if not math.isfinite(limiter.rate) or not math.isfinite(limiter.capacity):
            raise ValueError("bucket rate and capacity must be finite")
        if not math.isfinite(cost) or cost <= 0:
            raise ValueError("cost must be finite and positive")
        if cost > limiter.capacity:
            raise ValueError(
                f"cost {cost} exceeds capacity {limiter.capacity} and can never be satisfied"
            )

        wait_ms = self._script(
            keys=[self._redis_key(key), self._lru_key],
            args=[now, limiter.rate, limiter.capacity, cost, self._ttl, self._max_keys or 0],
        )
        if not isinstance(wait_ms, int) or wait_ms < 0:
            raise TypeError("Redis script must return nonnegative integer milliseconds")
        return Allow() if wait_ms == 0 else Wait(wait_ms / 1000.0)

    def reset(self, key: str) -> None:
        """Forget one key and its LRU entry in this namespace."""
        redis_key = self._redis_key(key)
        with self._client.pipeline(transaction=True) as pipe:
            pipe.delete(redis_key)
            pipe.zrem(self._lru_key, redis_key)
            pipe.execute()

    def clear(self) -> None:
        """Forget this namespace's keys. Call with concurrent writers stopped."""
        keys = set(self._client.scan_iter(match=self._key_pattern()))
        if keys:
            self._client.delete(*keys)
        self._client.delete(self._lru_key)

    def __len__(self) -> int:
        """Count live keys in this namespace; concurrent writes may change it."""
        return len(set(self._client.scan_iter(match=self._key_pattern())))

    def _redis_key(self, key: str) -> str:
        return f"safefetch:{self._algorithm}:{key}"

    def _key_pattern(self) -> str:
        # Escape Redis glob syntax so clear() cannot cross namespaces.
        prefix = self._redis_key("")
        return "".join("\\" + char if char in "\\*?[]" else char for char in prefix) + "*"

    def close(self) -> None:
        """Release the client and pool only if this store created them."""
        if self._owns_client:
            self._client.close()

    # Self requires Python 3.11, and the support floor here is 3.10.
    def __enter__(self) -> "RedisStore":  # noqa: PYI034
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
