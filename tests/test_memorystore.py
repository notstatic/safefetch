import threading
from collections.abc import Callable

import pytest

from safefetch.clock import FakeClock
from safefetch.decision import Allow, Wait
from safefetch.limiters.bucket import TokenBucket
from safefetch.stores.memory import MemoryStore
from safefetch.stores.redis import RedisStore

StoreFactory = Callable[..., MemoryStore | RedisStore]


def test_keys_are_independent(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory()
    bucket = TokenBucket(rate=1, capacity=2)

    for _ in range(2):
        assert isinstance(store.check("a", bucket, clock.monotonic()), Allow)

    assert isinstance(store.check("a", bucket, clock.monotonic()), Wait)
    assert isinstance(store.check("b", bucket, clock.monotonic()), Allow)


def test_same_key_shares_state(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory()
    bucket = TokenBucket(rate=1, capacity=3)

    allowed = sum(
        isinstance(store.check("host", bucket, clock.monotonic()), Allow) for _ in range(10)
    )
    assert allowed == 3


def test_unseen_key_starts_full(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory()
    bucket = TokenBucket(rate=1, capacity=5)

    assert isinstance(store.check("fresh", bucket, clock.monotonic()), Allow)
    assert len(store) == 1


def test_limit_holds_under_concurrency(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory()
    bucket = TokenBucket(rate=1, capacity=10)

    allowed = 0
    counter_lock = threading.Lock()

    def worker() -> None:
        nonlocal allowed
        decision = store.check("shared", bucket, clock.monotonic())
        if isinstance(decision, Allow):
            with counter_lock:
                allowed += 1

    threads = [threading.Thread(target=worker) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert allowed == 10


def test_least_recently_used_key_is_evicted(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory(max_keys=2)
    bucket = TokenBucket(rate=1, capacity=1)

    store.check("first", bucket, clock.monotonic())
    store.check("second", bucket, clock.monotonic())
    store.check("third", bucket, clock.monotonic())

    assert len(store) == 2
    # "first" was evicted, so it starts full again
    assert isinstance(store.check("first", bucket, clock.monotonic()), Allow)


def test_reset_forgets_one_key(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory()
    bucket = TokenBucket(rate=1, capacity=1)

    store.check("a", bucket, clock.monotonic())
    assert isinstance(store.check("a", bucket, clock.monotonic()), Wait)

    store.reset("a")
    assert isinstance(store.check("a", bucket, clock.monotonic()), Allow)


def test_clear_forgets_everything(store_factory: StoreFactory) -> None:
    clock = FakeClock()
    store = store_factory()
    bucket = TokenBucket(rate=1, capacity=1)

    store.check("a", bucket, clock.monotonic())
    store.check("b", bucket, clock.monotonic())
    assert len(store) == 2

    store.clear()
    assert len(store) == 0


def test_max_keys_must_be_positive(store_factory: StoreFactory) -> None:
    with pytest.raises(ValueError, match="at least 1|positive integer"):
        store_factory(max_keys=0)
