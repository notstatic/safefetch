# safefetch

HTTP calls that stay inside rate limits, retry sensibly, and back off when a service is failing.
Safe for the target, and safe for your process.

**Status: early development.** The public API is not stable yet.

## Planned features

- Rate limiting: token bucket, sliding window, GCRA
- Retries with exponential backoff, full jitter, and `Retry-After` support
- Circuit breaker integration per host
- Conditional caching with `ETag` and `Last-Modified`
- `robots.txt` support including `Crawl-delay`

## What it does not do

- No SSRF protection or URL validation
- No full HTTP caching per RFC 9111
- No proxy rotation or CAPTCHA handling

## Install

```bash
pip install safefetch
```

## Usage

```python
from safefetch import Retry, TokenBucket
from safefetch.sync import SafeFetch

with SafeFetch(
    limiter=TokenBucket(rate=5, capacity=10),
    retry=Retry(attempts=4, max_elapsed=30),
) as client:
    response = client.get("https://example.com/api/items")
```

For asyncio, use the same policies with `AsyncSafeFetch`:

```python
import asyncio

from safefetch import Retry, TokenBucket
from safefetch.aio import AsyncSafeFetch


async def main():
    async with AsyncSafeFetch(
        limiter=TokenBucket(rate=5, capacity=10),
        retry=Retry(attempts=4, max_elapsed=30),
    ) as client:
        response = await client.get("https://example.com/api/items")
        print(response.status_code)


asyncio.run(main())
```

Both adapters share the limiter, retry policy and store. `AsyncSafeFetch`
awaits HTTP calls and sleeps, so waiting leaves the event loop free to run
other tasks. Use `async with` or `await client.aclose()` to close it; an
injected `httpx.AsyncClient` remains the caller's responsibility.

`SafeFetch` and `AsyncSafeFetch` live in `safefetch.sync` and `safefetch.aio`
rather than the package root because they import httpx, which is an optional
dependency. Install it with `pip install safefetch[httpx]`.

## Circuit breaker policy

`CircuitBreaker` is a pure state machine. Pass in state and monotonic time;
it returns new state and a `Permit` or `Reject` decision. HTTP adapters do
not use it automatically yet.

```python
from safefetch import CircuitBreaker, Permit

breaker = CircuitBreaker(
    failure_threshold=0.5,
    min_calls=10,
    window=60,
    recovery_timeout=30,
    half_open_max_calls=2,
)
state = breaker.initial_state(now=0.0)
state, decision = breaker.check(state, now=0.0)
if isinstance(decision, Permit):
    # The caller sends the request and classifies its result.
    state = breaker.record(state, now=0.2, permit=decision, success=True)
```

| State | Admission | Transition |
| --- | --- | --- |
| `closed` | Allow requests | Open when the window has at least `min_calls` completed calls and the failure fraction reaches `failure_threshold`. |
| `open` | Reject with the remaining cooldown | The first check at or after the cooldown starts a half-open round. |
| `half-open` | Reserve at most `half_open_max_calls` probes per round | Any failure reopens immediately; all probes succeeding closes with a fresh window. |

The window contains results completed in `(now - window, now]`. Denied and
unfinished requests do not count. Half-open admission reserves each probe
before sending; a completed probe does not replenish the allowance.

Keep one state per target. Apply each `check` and `record` atomically to the
latest state in your store, and return the original permit exactly once for
each completed request. Old permits cannot affect a later recovery round.
Record cancelled probes as failures to release the circuit from that round.
The policy itself performs no I/O, locking or sleeping.

## Redis store

Install `safefetch[redis]` and import `RedisStore` from `safefetch.stores.redis`.
It implements the synchronous `Store.check` interface for `TokenBucket` using
an atomic Lua script:

```python
from safefetch import SystemClock, TokenBucket
from safefetch.stores.redis import RedisStore

with RedisStore(
    "redis://localhost:6379/0",
    algorithm="token_bucket",
    ttl=60,
) as store:
    decision = store.check(
        "example.com", TokenBucket(rate=5, capacity=10), SystemClock().monotonic()
    )
```

This uses the key `safefetch:token_bucket:example.com`. Each script invocation
reads state, refills tokens, makes the decision and writes state with
`SET ... EX ttl` atomically. Denied checks also refresh the TTL. Expired keys
restart full; choose a TTL at least as long as `capacity / rate`. Stores
sharing a namespace must agree on the limiter configuration, TTL, `max_keys`
and time base. Monotonic clocks on different machines do not provide a common
time base.

The caller's `now` is passed to Lua as an argument. The script never reads
Redis `TIME`, so `FakeClock` works unchanged. Lua returns waits rounded up to
integer milliseconds; Python converts them back to seconds. Token counts
and timestamps retain double precision in the stored JSON.

`reset(key)`, `clear()` and `len(store)` operate within the store's namespace.
An optional `max_keys` applies LRU eviction atomically, with an expiring
`safefetch_lru:{algorithm}` index. This implementation targets standalone Redis,
not Redis Cluster. Stop concurrent writers before calling `clear()`.

The store opens connections lazily. `close()` and `with` close only the client
and pool it creates; an injected `client=redis.Redis(...)` remains the caller's
responsibility. Connection options such as `socket_timeout` pass through to
redis-py. This store performs blocking I/O.

The scaffold's arbitrary `encode`/`decode` callbacks have been replaced by the
script's fixed `BucketState` JSON schema. Other limiter types are rejected;
they need an equivalent Lua implementation before Redis can run them atomically.
Redis errors propagate to the caller.

## Testing

Install the development dependencies with `uv sync --all-extras --dev` and
install `redis-server` (`brew install redis` on macOS or `apt-get install
redis-server` on Debian/Ubuntu). Run `uv run pytest`.

The suite starts and stops its own real Redis process on a temporary Unix
socket, without persistence. It never uses an existing Redis database. All
memory-store tests also run against Redis, including LRU eviction and
concurrent access. A separate test uses two independent Redis connections
and verifies that together they admit exactly the bucket's capacity. Policy
time remains controlled by `FakeClock`; Redis tests are not skipped if the
server executable is missing.

## License

MIT
