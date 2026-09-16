import json
from dataclasses import asdict
from unittest.mock import Mock

import pytest
from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError

from safefetch.clock import FakeClock
from safefetch.decision import Allow, Decision, Wait
from safefetch.limiters.bucket import BucketState, TokenBucket
from safefetch.stores import Store
from safefetch.stores.redis import RedisStore


class RedisBackend:
    """Just GET, SET with EX, and DELETE, using fake time instead of sockets."""

    def __init__(self) -> None:
        self.clock = FakeClock()
        self.values: dict[str, tuple[bytes, float]] = {}

    def get(self, key: str) -> bytes | None:
        entry = self.values.get(key)
        if entry is None:
            return None
        payload, expires_at = entry
        if self.clock.monotonic() >= expires_at:
            del self.values[key]
            return None
        return payload

    def set(self, key: str, payload: bytes | str, *, ex: int) -> bool:
        data = payload.encode() if isinstance(payload, str) else payload
        self.values[key] = (data, self.clock.monotonic() + ex)
        return True

    def delete(self, key: str) -> int:
        return int(self.values.pop(key, None) is not None)

    def client(self, *, decode_responses: bool = False) -> Mock:
        client = Mock(spec=Redis)

        def get(key: str) -> bytes | str | None:
            payload = self.get(key)
            if payload is not None and decode_responses:
                return payload.decode()
            return payload

        client.get.side_effect = get
        client.set.side_effect = self.set
        client.delete.side_effect = self.delete
        return client


@pytest.fixture
def backend() -> RedisBackend:
    return RedisBackend()


def encode(state: BucketState) -> str:
    return json.dumps(asdict(state))


def decode(payload: bytes | str) -> BucketState:
    return BucketState(**json.loads(payload))


def build(
    client: Redis, *, algorithm: str = "token_bucket", ttl: int = 60
) -> RedisStore[BucketState]:
    return RedisStore(algorithm=algorithm, ttl=ttl, encode=encode, decode=decode, client=client)


def test_store_protocol_and_new_key_initial_state(backend: RedisBackend) -> None:
    client = backend.client()
    store: Store[BucketState] = build(client)
    decision = store.check("example.com", TokenBucket(rate=1, capacity=3), now=100.0)
    assert isinstance(decision, Allow)
    client.get.assert_called_once_with("safefetch:token_bucket:example.com")
    client.set.assert_called_once_with(
        "safefetch:token_bucket:example.com", encode(BucketState(2.0, 100.0)), ex=60
    )


@pytest.mark.parametrize("decode_responses", [False, True])
def test_state_round_trip_for_bytes_and_text_clients(
    backend: RedisBackend, decode_responses: bool
) -> None:
    store = build(backend.client(decode_responses=decode_responses))
    bucket = TokenBucket(rate=1, capacity=2)
    assert isinstance(store.check("host", bucket, 0.0), Allow)
    assert isinstance(store.check("host", bucket, 0.0), Allow)
    assert store.check("host", bucket, 0.0) == Wait(1.0)
    assert isinstance(store.check("host", bucket, 1.0), Allow)


def test_independent_store_instances_share_state(backend: RedisBackend) -> None:
    first = build(backend.client())
    second = build(backend.client())
    bucket = TokenBucket(rate=1, capacity=1)
    assert isinstance(first.check("shared", bucket, 0.0), Allow)
    assert second.check("shared", bucket, 0.0) == Wait(1.0)


def test_keys_are_independent_and_preserved_verbatim(backend: RedisBackend) -> None:
    store = build(backend.client())
    bucket = TokenBucket(rate=1, capacity=1)
    for key in ("example.com", "example.com:8443", "ää.example/{path}"):
        assert isinstance(store.check(key, bucket, 0.0), Allow)
        assert store.check(key, bucket, 0.0) == Wait(1.0)
        assert f"safefetch:token_bucket:{key}" in backend.values


def test_algorithm_namespaces_are_independent(backend: RedisBackend) -> None:
    first = build(backend.client(), algorithm="bucket_v1")
    second = build(backend.client(), algorithm="bucket_v2")
    bucket = TokenBucket(rate=1, capacity=1)
    assert isinstance(first.check("same", bucket, 0.0), Allow)
    assert isinstance(second.check("same", bucket, 0.0), Allow)
    assert first.check("same", bucket, 0.0) == Wait(1.0)
    assert second.check("same", bucket, 0.0) == Wait(1.0)
    assert set(backend.values) == {"safefetch:bucket_v1:same", "safefetch:bucket_v2:same"}


def test_cost_is_forwarded_to_the_limiter(backend: RedisBackend) -> None:
    store = build(backend.client())
    bucket = TokenBucket(rate=1, capacity=3)
    assert isinstance(store.check("host", bucket, 0.0, cost=2.0), Allow)
    assert store.check("host", bucket, 0.0, cost=2.0) == Wait(1.0)


def test_denied_check_persists_updated_state_and_refreshes_ttl(backend: RedisBackend) -> None:
    client = backend.client()
    store = build(client, ttl=10)
    bucket = TokenBucket(rate=0.01, capacity=1)
    key = "safefetch:token_bucket:host"
    assert isinstance(store.check("host", bucket, 0.0), Allow)
    assert backend.values[key][1] == 10.0
    backend.clock.advance(3.0)
    decision = store.check("host", bucket, 3.0)
    assert isinstance(decision, Wait)
    assert decision.seconds == pytest.approx(97.0)
    client.set.assert_called_with(key, encode(BucketState(0.03, 3.0)), ex=10)
    assert backend.values[key][1] == 13.0
    backend.clock.advance(9.0)
    assert backend.get(key) is not None
    backend.clock.advance(1.0)
    assert backend.get(key) is None
    # Expiry intentionally forgets the old state, even if the TTL was too short.
    assert isinstance(store.check("host", bucket, 13.0), Allow)


def test_reset_only_deletes_the_named_key_in_its_namespace(backend: RedisBackend) -> None:
    client = backend.client()
    first = build(client)
    other_algorithm = build(backend.client(), algorithm="other")
    bucket = TokenBucket(rate=1, capacity=1)
    first.check("a", bucket, 0.0)
    first.check("b", bucket, 0.0)
    other_algorithm.check("a", bucket, 0.0)
    first.reset("a")
    client.delete.assert_called_once_with("safefetch:token_bucket:a")
    assert isinstance(first.check("a", bucket, 0.0), Allow)
    assert first.check("b", bucket, 0.0) == Wait(1.0)
    assert other_algorithm.check("a", bucket, 0.0) == Wait(1.0)


def test_codecs_support_state_other_than_dataclasses(backend: RedisBackend) -> None:
    class CounterLimiter:
        def initial_state(self, now: float) -> int:
            return 0

        def check(self, state: int, now: float, cost: float = 1.0) -> tuple[int, Decision]:
            return state + 1, Allow() if state == 0 else Wait(1.0)

    store: Store[int] = RedisStore[int](
        algorithm="counter", encode=str, decode=int, client=backend.client()
    )
    limiter = CounterLimiter()
    assert isinstance(store.check("host", limiter, 0.0), Allow)
    assert store.check("host", limiter, 0.0) == Wait(1.0)


def test_bytes_encoder_is_supported(backend: RedisBackend) -> None:
    store = RedisStore[BucketState](
        algorithm="token_bucket",
        encode=lambda state: encode(state).encode(),
        decode=decode,
        client=backend.client(),
    )
    bucket = TokenBucket(rate=1, capacity=1)
    assert isinstance(store.check("host", bucket, 0.0), Allow)
    assert store.check("host", bucket, 0.0) == Wait(1.0)


def test_owned_client_is_lazy_and_connection_options_are_forwarded(
    backend: RedisBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = backend.client()
    factory = Mock(return_value=client)
    monkeypatch.setattr(Redis, "from_url", factory)
    with RedisStore[BucketState](
        "redis://localhost:6380/2",
        algorithm="token_bucket",
        encode=encode,
        decode=decode,
        socket_timeout=2.0,
        decode_responses=True,
    ) as store:
        factory.assert_called_once_with(
            "redis://localhost:6380/2", socket_timeout=2.0, decode_responses=True
        )
        assert client.method_calls == []
        assert isinstance(store.check("host", TokenBucket(rate=1, capacity=1), 0.0), Allow)
    client.close.assert_called_once_with()


def test_context_manager_closes_owned_client_on_error(
    backend: RedisBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = backend.client()
    monkeypatch.setattr(Redis, "from_url", Mock(return_value=client))
    with (
        pytest.raises(RuntimeError, match="caller failed"),
        RedisStore[BucketState](algorithm="token_bucket", encode=encode, decode=decode),
    ):
        raise RuntimeError("caller failed")
    client.close.assert_called_once_with()


def test_explicit_close_releases_owned_client(
    backend: RedisBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = backend.client()
    monkeypatch.setattr(Redis, "from_url", Mock(return_value=client))
    store = RedisStore[BucketState](algorithm="token_bucket", encode=encode, decode=decode)
    store.close()
    client.close.assert_called_once_with()


def test_borrowed_client_is_not_closed_or_replaced(
    backend: RedisBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory = Mock()
    monkeypatch.setattr(Redis, "from_url", factory)
    client = backend.client()
    with build(client) as store:
        assert isinstance(store.check("host", TokenBucket(rate=1, capacity=1), 0.0), Allow)
    store.close()
    client.close.assert_not_called()
    factory.assert_not_called()


@pytest.mark.parametrize("algorithm", ["", "bucket:variant"])
def test_algorithm_must_be_a_nonempty_namespace_without_colons(
    backend: RedisBackend, algorithm: str
) -> None:
    with pytest.raises(ValueError, match="algorithm"):
        build(backend.client(), algorithm=algorithm)


@pytest.mark.parametrize("ttl", [0, -1, 1.5, True, None])
def test_ttl_must_be_positive_integer_seconds(backend: RedisBackend, ttl: object) -> None:
    with pytest.raises(ValueError, match="ttl must be a positive integer"):
        build(backend.client(), ttl=ttl)  # type: ignore[arg-type]


@pytest.mark.parametrize("operation", ["get", "set"])
@pytest.mark.parametrize("error_type", [ConnectionError, TimeoutError])
def test_redis_errors_propagate(
    backend: RedisBackend, operation: str, error_type: type[Exception]
) -> None:
    client = backend.client()
    error = error_type("unavailable")
    getattr(client, operation).side_effect = error
    with pytest.raises(error_type) as raised:
        build(client).check("host", TokenBucket(rate=1, capacity=1), 0.0)
    assert raised.value is error
    if operation == "get":
        client.set.assert_not_called()


def test_decode_errors_do_not_overwrite_existing_data(backend: RedisBackend) -> None:
    client = backend.client()
    backend.set("safefetch:token_bucket:host", "not json", ex=60)
    with pytest.raises(json.JSONDecodeError):
        build(client).check("host", TokenBucket(rate=1, capacity=1), 0.0)
    client.set.assert_not_called()
    assert backend.get("safefetch:token_bucket:host") == b"not json"


def test_encode_errors_propagate_without_writing(backend: RedisBackend) -> None:
    client = backend.client()
    encoder = Mock(side_effect=ValueError("cannot encode"))
    store = RedisStore[BucketState](
        algorithm="token_bucket", encode=encoder, decode=decode, client=client
    )
    with pytest.raises(ValueError, match="cannot encode"):
        store.check("host", TokenBucket(rate=1, capacity=1), 0.0)
    client.set.assert_not_called()


def test_unexpected_redis_response_is_rejected(backend: RedisBackend) -> None:
    client = backend.client()
    client.get.side_effect = None
    client.get.return_value = 123
    with pytest.raises(TypeError, match="Redis GET must return"):
        build(client).check("host", TokenBucket(rate=1, capacity=1), 0.0)
    client.set.assert_not_called()


def test_limiter_errors_do_not_write_state(backend: RedisBackend) -> None:
    client = backend.client()
    with pytest.raises(ValueError, match="cost must be positive"):
        build(client).check("host", TokenBucket(rate=1, capacity=1), 0.0, cost=0.0)
    client.set.assert_not_called()


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="GET/SET is intentionally non-atomic; an atomic check-and-update must fix this",
)
def test_limit_holds_when_two_reads_overlap(backend: RedisBackend) -> None:
    first_client = backend.client()
    first = build(first_client)
    second = build(backend.client())
    bucket = TokenBucket(rate=1, capacity=1)
    decisions: list[Decision] = []

    def overlapping_get(key: str) -> bytes | None:
        snapshot = backend.get(key)
        # Another caller completes its check after our GET, but before our SET.
        decisions.append(second.check("shared", bucket, 0.0))
        return snapshot

    first_client.get.side_effect = overlapping_get
    decisions.append(first.check("shared", bucket, 0.0))
    assert sum(isinstance(decision, Allow) for decision in decisions) == 1
