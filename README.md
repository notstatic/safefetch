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
- One core, two adapters: sync and async
- In-memory or Redis state, so limits hold across processes

## What it does not do

- No SSRF protection or URL validation
- No full HTTP caching per RFC 9111
- No proxy rotation or CAPTCHA handling

## Install

```bash
pip install safefetch
```

## License

MIT