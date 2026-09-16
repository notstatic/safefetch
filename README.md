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
- Atomic Redis state updates, so limits hold across processes

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

## Redis store scaffold

Install `safefetch[redis]` and import `RedisStore` from `safefetch.stores.redis`.
It implements the synchronous `Store.check` interface, with explicit codecs
for the limiter's state:

```python
import json
from dataclasses import asdict

from safefetch import BucketState, SystemClock, TokenBucket
from safefetch.stores.redis import RedisStore

with RedisStore[BucketState](
    "redis://localhost:6379/0",
    algorithm="token_bucket",
    encode=lambda state: json.dumps(asdict(state)),
    decode=lambda payload: BucketState(**json.loads(payload)),
    ttl=60,
) as store:
    decision = store.check(
        "example.com", TokenBucket(rate=5, capacity=10), SystemClock().monotonic()
    )
```

This uses the key `safefetch:token_bucket:example.com`. Every check writes the
new state with `SET ... EX ttl`, including checks that return `Wait`, so idle
keys expire automatically. Expired keys restart from the limiter's initial
state; choose a TTL at least as long as its memory horizon (for a token bucket,
`capacity / rate`). Stores sharing an algorithm and key must agree on the
codec, limiter configuration and time base. Monotonic clocks on different
machines do not provide a common time base.

The store opens connections lazily. `close()` and `with` close only the client
and pool it creates; an injected `client=redis.Redis(...)` remains the caller's
responsibility. Connection options such as `socket_timeout` pass through to
redis-py. This store performs blocking I/O.

GET and SET are intentionally separate in this scaffold. Overlapping callers
can both consume the same token and overwrite an update. Atomic updates are
deferred; a strict expected-failure test records the current race. Redis and
serialization errors propagate to the caller.

## License

MIT
