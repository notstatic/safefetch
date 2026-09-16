import json
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import Mock

import pytest
from redis import Redis
from redis.exceptions import ConnectionError, ResponseError, TimeoutError

from safefetch.clock import FakeClock
from safefetch.decision import Allow, Wait
from safefetch.limiters.bucket import BucketState, TokenBucket
from safefetch.stores import MemoryStore, Store
from safefetch.stores.redis import RedisStore


def test_store_protocol_and_persisted_state(redis_client: Redis) -> None:
    store: Store[BucketState] = RedisStore(client=redis_client)
    assert isinstance(store.check("example.com", TokenBucket(rate=1, capacity=3), 100.0), Allow)
    assert json.loads(redis_client.get("safefetch:token_bucket:example.com")) == {
        "tokens": 2.0,
        "updated_at": 100.0,
    }


@pytest.mark.parametrize("decode_responses", [False, True])
def test_binary_and_text_connections(
    redis_client: Redis, redis_options: dict[str, Any], decode_responses: bool
) -> None:
    with Redis(**redis_options, decode_responses=decode_responses) as client:
        store = RedisStore(client=client)
        bucket = TokenBucket(rate=1, capacity=2)
        assert isinstance(store.check("host", bucket, 0.0), Allow)
        assert isinstance(store.check("host", bucket, 0.0), Allow)
        assert store.check("host", bucket, 0.0) == Wait(1.0)
        assert isinstance(store.check("host", bucket, 1.0), Allow)


@pytest.mark.parametrize("capacity", [1, 10, 32])
def test_two_independent_connections_allow_exactly_capacity(
    redis_client: Redis, redis_options: dict[str, Any], capacity: int
) -> None:
    clock = FakeClock(start=123.456)
    bucket = TokenBucket(rate=1, capacity=capacity)
    barrier = threading.Barrier(2, timeout=5)

    def consume(store: RedisStore) -> int:
        barrier.wait()
        return sum(
            isinstance(store.check("shared", bucket, clock.monotonic()), Allow) for _ in range(100)
        )

    # Each worker owns a distinct, pinned Redis connection, not just a wrapper
    # around one shared client. No local lock can protect both stores.
    with (
        Redis(**redis_options, single_connection_client=True) as first,
        Redis(**redis_options, single_connection_client=True) as second,
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        assert first.connection_pool is not second.connection_pool
        assert first.client_id() != second.client_id()
        results = [
            executor.submit(consume, RedisStore(client=client)) for client in (first, second)
        ]
        assert sum(result.result(timeout=10) for result in results) == capacity
        assert RedisStore(client=first).check("shared", bucket, clock.monotonic()) == Wait(1.0)


@pytest.mark.parametrize(("rate", "expected"), [(8.0, 0.125), (3.0, 0.334), (4000.0, 0.001)])
def test_fractional_waits_round_up_to_milliseconds(
    redis_client: Redis, rate: float, expected: float
) -> None:
    store = RedisStore(client=redis_client)
    bucket = TokenBucket(rate=rate, capacity=1)
    clock = FakeClock()
    assert isinstance(store.check("host", bucket, clock.monotonic()), Allow)
    decision = store.check("host", bucket, clock.monotonic())
    assert decision == Wait(expected)
    clock.advance(expected)
    assert isinstance(store.check("host", bucket, clock.monotonic()), Allow)


def test_fake_clock_drives_refill_when_redis_time_command_is_forbidden(
    redis_client: Redis, redis_options: dict[str, Any]
) -> None:
    redis_client.acl_setuser(
        "no-clock",
        enabled=True,
        passwords=["+test-only"],
        keys=["*"],
        commands=["+@all", "-time"],
    )
    try:
        with Redis(**redis_options, username="no-clock", password="test-only") as client:
            with pytest.raises(ResponseError):
                client.time()
            store = RedisStore(client=client)
            bucket = TokenBucket(rate=4, capacity=1)
            clock = FakeClock(start=987.125)
            assert isinstance(store.check("host", bucket, clock.monotonic()), Allow)
            assert store.check("host", bucket, clock.monotonic()) == Wait(0.25)
            clock.advance(0.125)
            assert store.check("host", bucket, clock.monotonic()) == Wait(0.125)
            clock.advance(0.125)
            assert isinstance(store.check("host", bucket, clock.monotonic()), Allow)
    finally:
        redis_client.acl_deluser("no-clock")


@pytest.mark.parametrize("rate", [0.3, 3.0, 1000.0])
def test_script_matches_python_bucket_over_a_deterministic_trace(
    redis_client: Redis, rate: float
) -> None:
    memory = MemoryStore[BucketState]()
    store = RedisStore(client=redis_client)
    bucket = TokenBucket(rate=rate, capacity=2.5)
    clock = FakeClock(start=100_000_000.12345679)
    rng = random.Random(17)
    for _ in range(100):
        clock.advance(rng.choice([0.0, 0.001, 0.07, 0.25, 1.0]))
        cost = rng.choice([0.125, 0.5, 1.0, 2.5])
        expected = memory.check("host", bucket, clock.monotonic(), cost)
        actual = store.check("host", bucket, clock.monotonic(), cost)
        if isinstance(expected, Allow):
            assert isinstance(actual, Allow)
        else:
            assert isinstance(actual, Wait)
            assert expected.seconds <= actual.seconds + 1e-12
            assert actual.seconds - expected.seconds <= 0.001 + 1e-12


def test_state_preserves_full_float_precision(redis_client: Redis) -> None:
    now = 100_000_000.12345679
    cost = 0.1234567890123456
    RedisStore(client=redis_client).check("host", TokenBucket(rate=1, capacity=1), now, cost)
    state = json.loads(redis_client.get("safefetch:token_bucket:host"))
    assert state["updated_at"] == now
    assert state["tokens"] == 1 - cost


def test_backward_clock_and_refill_cap_match_memory(redis_client: Redis) -> None:
    store = RedisStore(client=redis_client)
    memory = MemoryStore[BucketState]()
    bucket = TokenBucket(rate=1, capacity=2)
    for now in (100.0, 90.0, 90.0, 3600.0, 3600.0, 3600.0):
        assert store.check("host", bucket, now) == memory.check("host", bucket, now)


def test_script_reloads_after_redis_script_cache_is_cleared(redis_client: Redis) -> None:
    store = RedisStore(client=redis_client)
    bucket = TokenBucket(rate=1, capacity=1)
    assert isinstance(store.check("host", bucket, 0.0), Allow)
    redis_client.script_flush()
    assert store.check("host", bucket, 0.0) == Wait(1.0)


def test_ttl_is_refreshed_on_denial_and_expired_state_starts_fresh(redis_client: Redis) -> None:
    store = RedisStore(client=redis_client, ttl=60)
    bucket = TokenBucket(rate=1, capacity=1)
    key = "safefetch:token_bucket:host"
    assert isinstance(store.check("host", bucket, 0.0), Allow)
    assert 0 < redis_client.pttl(key) <= 60_000
    redis_client.pexpire(key, 1000)
    assert store.check("host", bucket, 0.0) == Wait(1.0)
    assert 1000 < redis_client.pttl(key) <= 60_000
    # Expire the real Redis key deterministically, without sleeping or moving
    # the limiter's FakeClock to match the server's expiry clock.
    redis_client.pexpireat(key, 1)
    assert len(store) == 0
    assert isinstance(store.check("host", bucket, 0.0), Allow)


def test_lru_tracks_access_order_with_a_frozen_clock_and_cleans_expired_entries(
    redis_client: Redis,
) -> None:
    store = RedisStore(client=redis_client, max_keys=2)
    bucket = TokenBucket(rate=1, capacity=1)
    store.check("z-first", bucket, 0.0)
    store.check("a-second", bucket, 0.0)
    assert store.check("z-first", bucket, 0.0) == Wait(1.0)
    store.check("b-third", bucket, 0.0)
    assert redis_client.exists("safefetch:token_bucket:a-second") == 0
    assert store.check("z-first", bucket, 0.0) == Wait(1.0)
    redis_client.pexpireat("safefetch:token_bucket:b-third", 1)
    store.check("c-fourth", bucket, 0.0)
    assert redis_client.zcard("safefetch_lru:token_bucket") == 2
    assert 0 < redis_client.ttl("safefetch_lru:token_bucket") <= 60
    assert store.check("z-first", bucket, 0.0) == Wait(1.0)
    store.reset("z-first")
    assert redis_client.zcard("safefetch_lru:token_bucket") == 1


@pytest.mark.parametrize("algorithm", ["other", "x*", "x?", "x[ab]", "x\\y"])
def test_clear_and_len_stay_inside_the_exact_namespace(redis_client: Redis, algorithm: str) -> None:
    first = RedisStore(client=redis_client, algorithm=algorithm, max_keys=2)
    second = RedisStore(client=redis_client, algorithm="x-other", max_keys=2)
    bucket = TokenBucket(rate=1, capacity=1)
    first.check("example.com:8443/ää", bucket, 0.0)
    second.check("example.com:8443/ää", bucket, 0.0)
    assert len(first) == len(second) == 1
    first.clear()
    first.clear()
    assert len(first) == 0
    assert len(second) == 1
    assert second.check("example.com:8443/ää", bucket, 0.0) == Wait(1.0)


@pytest.mark.parametrize("payload", ["not json", "{}", '{"tokens":"bad","updated_at":0}'])
def test_corrupt_state_errors_do_not_overwrite_it(redis_client: Redis, payload: str) -> None:
    key = "safefetch:token_bucket:host"
    redis_client.set(key, payload, ex=60)
    with pytest.raises(ResponseError):
        RedisStore(client=redis_client).check("host", TokenBucket(rate=1, capacity=1), 0.0)
    assert redis_client.get(key) == payload.encode()


def test_owned_client_is_lazy_and_connection_options_are_forwarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Mock(spec=Redis)
    factory = Mock(return_value=client)
    monkeypatch.setattr(Redis, "from_url", factory)
    with RedisStore("redis://localhost:6380/2", socket_timeout=2.0, decode_responses=True):
        factory.assert_called_once_with(
            "redis://localhost:6380/2", socket_timeout=2.0, decode_responses=True
        )
        client.register_script.assert_called_once()
        client.execute_command.assert_not_called()
        client.close.assert_not_called()
    client.close.assert_called_once_with()


def test_context_manager_closes_owned_client_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock(spec=Redis)
    monkeypatch.setattr(Redis, "from_url", Mock(return_value=client))
    with pytest.raises(RuntimeError, match="caller failed"), RedisStore():
        raise RuntimeError("caller failed")
    client.close.assert_called_once_with()


def test_explicit_close_releases_owned_client(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock(spec=Redis)
    monkeypatch.setattr(Redis, "from_url", Mock(return_value=client))
    RedisStore().close()
    client.close.assert_called_once_with()


def test_borrowed_client_is_not_closed_or_replaced(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = Mock()
    monkeypatch.setattr(Redis, "from_url", factory)
    client = Mock(spec=Redis)
    with RedisStore(client=client) as store:
        pass
    store.close()
    client.close.assert_not_called()
    factory.assert_not_called()


@pytest.mark.parametrize("algorithm", ["", "bucket:variant"])
def test_algorithm_must_be_a_nonempty_namespace_without_colons(algorithm: str) -> None:
    with pytest.raises(ValueError, match="algorithm"):
        RedisStore(client=Mock(spec=Redis), algorithm=algorithm)


@pytest.mark.parametrize("ttl", [0, -1, 1.5, True, None])
def test_ttl_must_be_positive_integer_seconds(ttl: Any) -> None:
    with pytest.raises(ValueError, match="ttl must be a positive integer"):
        RedisStore(client=Mock(spec=Redis), ttl=ttl)


@pytest.mark.parametrize("max_keys", [0, -1, 1.5, True])
def test_max_keys_must_be_a_positive_integer(max_keys: Any) -> None:
    with pytest.raises(ValueError, match="max_keys must be a positive integer"):
        RedisStore(client=Mock(spec=Redis), max_keys=max_keys)


@pytest.mark.parametrize("error_type", [ConnectionError, TimeoutError])
def test_redis_errors_propagate(error_type: type[Exception]) -> None:
    client = Mock(spec=Redis)
    error = error_type("unavailable")
    client.register_script.return_value.side_effect = error
    with pytest.raises(error_type) as raised:
        RedisStore(client=client).check("host", TokenBucket(rate=1, capacity=1), 0.0)
    assert raised.value is error


@pytest.mark.parametrize("reply", [-1, 0.5, "1", None])
def test_invalid_script_reply_is_rejected(reply: Any) -> None:
    client = Mock(spec=Redis)
    client.register_script.return_value.return_value = reply
    with pytest.raises(TypeError, match="integer milliseconds"):
        RedisStore(client=client).check("host", TokenBucket(rate=1, capacity=1), 0.0)


@pytest.mark.parametrize("cost", [0, -1, float("nan"), float("inf"), 2])
def test_invalid_cost_never_writes_state(redis_client: Redis, cost: float) -> None:
    store = RedisStore(client=redis_client)
    with pytest.raises(ValueError, match="cost"):
        store.check("host", TokenBucket(rate=1, capacity=1), 0.0, cost)
    assert len(store) == 0


@pytest.mark.parametrize("now", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_time_is_rejected(now: float) -> None:
    client = Mock(spec=Redis)
    with pytest.raises(ValueError, match="now must be finite"):
        RedisStore(client=client).check("host", TokenBucket(rate=1, capacity=1), now)
    client.register_script.return_value.assert_not_called()


@pytest.mark.parametrize(
    "bucket",
    [TokenBucket(rate=float("inf"), capacity=1), TokenBucket(rate=1, capacity=float("nan"))],
)
def test_nonfinite_bucket_is_rejected(bucket: TokenBucket) -> None:
    client = Mock(spec=Redis)
    with pytest.raises(ValueError, match="rate and capacity must be finite"):
        RedisStore(client=client).check("host", bucket, 0.0)
    client.register_script.return_value.assert_not_called()


def test_unsupported_limiter_is_rejected_without_a_nonatomic_fallback() -> None:
    class CustomBucket(TokenBucket):
        pass

    client = Mock(spec=Redis)
    with pytest.raises(TypeError, match="only supports TokenBucket"):
        RedisStore(client=client).check("host", CustomBucket(rate=1, capacity=1), 0.0)
    client.register_script.return_value.assert_not_called()
