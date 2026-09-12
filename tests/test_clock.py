from safefetch.clock import FakeClock, SystemClock


def test_fake_clock_starts_at_zero() -> None:
    clock = FakeClock()
    assert clock.monotonic() == 0.0


def test_fake_clock_advances() -> None:
    clock = FakeClock()
    clock.advance(1.5)
    assert clock.monotonic() == 1.5
    clock.advance(0.5)
    assert clock.monotonic() == 2.0


def test_fake_clock_rejects_negative() -> None:
    clock = FakeClock()
    try:
        clock.advance(-1)
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_system_clock_never_decreases() -> None:
    clock = SystemClock()
    first = clock.monotonic()
    second = clock.monotonic()
    assert second >= first