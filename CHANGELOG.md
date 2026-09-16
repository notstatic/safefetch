# Changelog

All notable changes to this project are documented here. This project
follows semantic versioning. While the version stays below 1.0.0 the
public API may change between minor releases.

## [Unreleased]

### Added
- Optional `RedisStore` scaffold with connection ownership, algorithm-scoped
  keys, injectable state codecs and a refreshed TTL on every write. GET and
  SET are intentionally separate; concurrent checks are not atomic yet.
- Redis store unit tests, including an expected failure for overlapping reads
- Pure `CircuitBreaker` policy with a rolling failure window, minimum call
  count, open cooldown and bounded half-open probes; request permits isolate
  recovery rounds from delayed results
- Table-driven tests for all circuit breaker transitions and boundary cases
- `AsyncSafeFetch` in `safefetch.aio`, using `httpx.AsyncClient` and
  `asyncio.sleep` with the same limiter, retry policy and store as `SafeFetch`
- Async adapter tests using pytest-asyncio, `MockTransport` and `FakeClock`

## [0.1.0]

First usable release.

### Added
- `Clock` protocol with `SystemClock` and `FakeClock`
- `TokenBucket` limiter with lazy refill from elapsed time
- `MemoryStore` with per-key state, one lock and optional LRU eviction
- `Retry` policy with exponential backoff, full jitter, a wall-clock
  budget and `Retry-After` support
- `SafeFetch`, a synchronous httpx client that ties the above together

## [0.0.1]

### Added
- Project skeleton and name reservation
