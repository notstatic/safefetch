# Changelog

All notable changes to this project are documented here. This project
follows semantic versioning. While the version stays below 1.0.0 the
public API may change between minor releases.

## [Unreleased]

### Added
- Atomic Redis token-bucket Lua script with caller-supplied time, waits returned
  as integer milliseconds, refreshed TTL and optional shared LRU eviction
- Complete memory-store test suite exercised against real Redis, plus a
  capacity test using two independent connections and deterministic clock tests
- Pure `CircuitBreaker` policy with a rolling failure window, minimum call
  count, open cooldown and bounded half-open probes; request permits isolate
  recovery rounds from delayed results
- Table-driven tests for all circuit breaker transitions and boundary cases
- `AsyncSafeFetch` in `safefetch.aio`, using `httpx.AsyncClient` and
  `asyncio.sleep` with the same limiter, retry policy and store as `SafeFetch`
- Async adapter tests using pytest-asyncio, `MockTransport` and `FakeClock`

### Changed
- `RedisStore` now supports `TokenBucket` with a fixed JSON state schema;
  generic codec callbacks and the non-atomic GET/SET fallback are removed
- CI and release verification install Redis and run the integration tests

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
