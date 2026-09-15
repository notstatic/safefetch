# safefetch

HTTP calls that stay inside rate limits, retry sensibly, and back off when a service is failing.
Safe for the target, and safe for your process.

**Status: early development.** The public API is not stable yet.

## Planned features

- Rate limiting: token bucket, sliding window, GCRA
- Retries with exponential backoff, full jitter, and `Retry-After` support
- Circuit breaker per host
- Conditional caching with `ETag` and `Last-Modified`
- `robots.txt` support including `Crawl-delay`
- In-memory or Redis state, so limits hold across processes

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

## License

MIT
