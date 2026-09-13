# safefetch

HTTP calls that stay inside rate limits, retry sensibly, and back off when a
service is failing. Published on PyPI as `safefetch`. Repository:
github.com/notstatic/safefetch.

This file is the working agreement for anyone, human or agent, working in this
repository. Read it before changing code.

---

## The one decision everything else follows from

**Policy modules perform no I/O, hold no locks, and never sleep.** They are
pure functions over state and time:

```python
def check(state: S, now: float, cost: float) -> tuple[S, Decision]: ...
```

They return a `Decision` (`Allow` or `Wait(seconds)`); the caller decides how to
wait. Three things fall out of this, and all three are the reason it is worth
defending:

- Sync and async adapters share one core. Only the sleep differs.
- Tests never sleep. They inject a `FakeClock` and move time by hand.
- Locking belongs to the store, not the algorithm. In-memory uses a
  `threading.Lock`, Redis will use a Lua script for the same reason.

If a change would put I/O, a lock, or a sleep into a limiter or the retry
policy, that change is wrong. Push it down into a store or up into an adapter.

---

## Layout

```
src/safefetch/
  __init__.py        public API, everything in __all__ is a promise
  clock.py           Clock protocol, SystemClock, FakeClock
  decision.py        Allow, Wait, Decision
  retry.py           Retry policy, Give / RetryAfter, parse_retry_after
  sync.py            SafeFetch, the only module that does I/O and sleeps
  limiters/
    base.py          Limiter protocol
    bucket.py        TokenBucket
  stores/
    memory.py        Store protocol, MemoryStore
tests/               mirrors the module names, no __init__.py
```

`SafeFetch` is deliberately **not** exported from the package root. It imports
httpx, which is an optional dependency, so `import safefetch` must not require
it. Users write `from safefetch.sync import SafeFetch`.

---

## Current state

Released: **0.1.0**.

Working: Clock, TokenBucket, MemoryStore with optional LRU eviction, Retry with
exponential backoff, full jitter, wall-clock budget and `Retry-After` support,
and a synchronous httpx client that ties them together.

Not built yet: async adapter, circuit breaker, Redis store, sliding window,
GCRA, conditional caching, robots.txt.

---

## Commands

```bash
uv sync --all-extras --dev      # after pulling, and after adding a subpackage
uv run pytest
uv run ruff check .
uv run mypy src
uv run pytest --cov=safefetch --cov-report=term-missing
```

All three checks must pass before a commit. CI runs the same three on Python
3.10 through 3.13.

---

## Conventions

**Branches and PRs.** `main` is protected: no direct pushes, PR required, CI
must pass. One short-lived branch per feature, branched from a fresh `main`.
Squash merge, then delete the branch. Names are lowercase: `feat/async-client`,
`fix/retry-after-parsing`, `ci/release-workflow`, `chore/bump-0.2.0`.

**Commits.** Conventional Commits, imperative, under 50 characters, no trailing
period: `feat: add circuit breaker`. The body explains *why*, not what; the
diff already shows what.

**PR descriptions.** Four parts: what problem, what you did, which choice you
made and why, and how to verify it. The third part is the one that matters.
Every non-obvious decision in this package is recorded in a PR description
rather than a comment, and that is on purpose.

**Language.** Everything in the repository is English: code, comments,
docstrings, commits, PR descriptions, changelog, docs.

**Docstrings.** Google style, with `Args:` and `Raises:` where they carry
information. Document the reasoning behind a choice, not the mechanics of the
code.

---

## Testing rules

- **Never call `time.sleep` in a test.** Use `FakeClock` and `advance()`. For
  `SafeFetch`, inject a `sleep` function that records the duration and advances
  the fake clock.
- **Never hit the network.** Use `httpx.MockTransport` with a handler that
  returns canned responses.
- Tests assert on behaviour, not internals. `isinstance(decision, Allow)`,
  not the token count, unless the token count is the point.
- Every error path gets a test. Every `raise` in the source has a test that
  triggers it.
- Coverage target is above 90 percent. Check it before a release, not on every
  commit.

---

## Packaging and release

- Version lives **only** in `pyproject.toml`. Code reads it with
  `importlib.metadata.version("safefetch")`.
- Semver. While below 1.0.0 the public API may break between minor versions,
  and that is stated in the changelog.
- `CHANGELOG.md` is updated in the same PR as the change, not afterwards.
- Release is triggered by a tag: `git tag -a v0.2.0 -m "..."` and
  `git push origin v0.2.0`. The workflow re-runs all checks, verifies the tag
  matches the packaged version, then publishes through trusted publishing.
  There is no API token anywhere.
- Version numbers are permanent on PyPI. A wrong one cannot be reused, which is
  why the tag-versus-version check exists.

---

## Traps already hit in this repository

These cost time once. They should not cost it twice.

- **Adding a new subpackage breaks the editable install.** Run `uv sync` after
  creating any new directory under `src/safefetch/`, before running pytest.
- **`typing.Self` needs Python 3.11.** The support floor is 3.10. `[tool.mypy]`
  pins `python_version = "3.10"` so this surfaces locally rather than in CI.
- **mypy strict reports "Returning Any"** when a stdlib call has loose typing,
  for example `random.uniform` or `parsedate_to_datetime`. Fix it by assigning
  to a variable with an explicit type at the point the value is created, not by
  adding `type: ignore`.
- **Keep the project out of iCloud.** `~/Documents` is synced on macOS and
  produces `__init__ 2.py` style duplicates that break the editable install.
  The project lives in `~/dev/safefetch`.
- **Clear `dist/` before building.** `uv publish` uploads everything in there,
  including stale versions.

---

## What to write by hand

This matters because the author has to be able to explain every line in a live
technical interview. Anything generated that the author cannot explain is not
finished.

**By hand:** limiter algorithms, the Redis Lua script when it arrives, the
circuit breaker state machine, workflow files, and anything security-relevant.
An error in a rate limiter is subtle: the code runs, the tests pass, and the
limit leaks under concurrency.

**Generated freely:** test scaffolding, docstrings, packaging config, the httpx
glue, benchmark scripts, documentation prose.

A good use of an assistant here: ask it to find a race condition or edge case
that has been missed, then write the test that settles it either way.

---

## Roadmap

**0.2.0** async adapter over `httpx.AsyncClient` sharing the same core;
circuit breaker per host as a three-state machine with a rolling failure rate,
a minimum call count before it can trip, and half-open probes; Redis store with
an atomic check-and-update in Lua, tested against a real Redis through
testcontainers.

**0.3.0** sliding window and GCRA limiters with a comparison table in the
README covering memory per key and burst behaviour; Hypothesis property test
asserting that no time window ever exceeds the limit; conditional caching with
`ETag` and `Last-Modified` treating 304 as a hit; robots.txt fetching, parsing,
caching and `Crawl-delay` support; benchmark numbers in the README.

The README documents what the package does **not** do. Keep that section
current. Stating a boundary is more useful than implying everything works.

---

## Where this fits

safefetch is the first of five packages. `scrapekit` (declarative scraping
framework with entry-point plugins) depends on it, `pgqueue` (Postgres job
queue using `SELECT ... FOR UPDATE SKIP LOCKED`) follows, then `tenantkit`
(multi-tenant isolation for FastAPI using Postgres row level security), and
finally a deployed crawl service that composes all four.

That is why the public API here is treated as a promise this early. A sloppy
interface at this layer is paid for four times over.
