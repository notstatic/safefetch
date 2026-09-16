import shutil
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from redis import Redis
from redis.exceptions import ConnectionError

from safefetch.limiters.bucket import BucketState
from safefetch.stores.memory import MemoryStore
from safefetch.stores.redis import RedisStore


@pytest.fixture(scope="session")
def redis_options() -> Iterator[dict[str, Any]]:
    """Own a real, disposable Redis process; never connect to a user's database."""
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.fail("Redis integration tests require redis-server on PATH (brew/apt install redis)")
    # A short socket path also works within macOS's Unix socket path limit.
    with tempfile.TemporaryDirectory(prefix="safefetch-redis-", dir="/tmp") as directory:
        socket = str(Path(directory) / "redis.sock")
        log = str(Path(directory) / "redis.log")
        process = subprocess.Popen(
            [
                executable,
                "--port",
                "0",
                "--unixsocket",
                socket,
                "--unixsocketperm",
                "700",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                directory,
                "--logfile",
                log,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        options: dict[str, Any] = {
            "unix_socket_path": socket,
            "socket_timeout": 5,
            "socket_connect_timeout": 5,
        }
        client = Redis(**options)
        try:
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    pytest.fail(f"Redis exited during startup: {Path(log).read_text()}")
                try:
                    client.ping()
                    break
                except ConnectionError:
                    if time.monotonic() >= deadline:
                        pytest.fail("Redis did not become ready within 10 seconds")
                    # Process startup only; limiter tests always use FakeClock.
                    threading.Event().wait(0.01)
            yield options
        finally:
            client.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@pytest.fixture
def redis_client(redis_options: dict[str, Any]) -> Iterator[Redis]:
    with Redis(**redis_options) as client:
        # This fixture only reaches the isolated process owned above.
        client.flushdb()
        yield client
        client.flushdb()


@pytest.fixture(params=["memory", "redis"])
def store_factory(
    request: pytest.FixtureRequest,
) -> Callable[..., MemoryStore[BucketState] | RedisStore]:
    """Run the complete store contract against both concrete backends."""
    if request.param == "memory":
        return MemoryStore[BucketState]
    client = request.getfixturevalue("redis_client")
    return lambda **kwargs: RedisStore(client=client, **kwargs)
