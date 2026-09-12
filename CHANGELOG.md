# Changelog

All notable changes to this project are documented here. This project
follows semantic versioning. While the version stays below 1.0.0 the
public API may change between minor releases.

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