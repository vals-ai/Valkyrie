from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import time

import pytest

SNAPSHOTS = Path(__file__).parent / "snapshots"
NOW = datetime(2026, 10, 10, 18, 0, tzinfo=timezone.utc)


class MockClock:
    """A clock tests move by hand, shared by the app and the fake source."""

    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@pytest.fixture(autouse=True)
def utc_local_time(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Log and detail times render in local time, so snapshots pin it to UTC."""
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()

    try:
        yield
    finally:
        monkeypatch.undo()
        time.tzset()


@pytest.fixture
def clock() -> MockClock:
    return MockClock()


@pytest.fixture
def assert_snapshot(tmp_path: Path) -> Callable[[str, str], None]:
    """Compare an SVG screenshot with snapshots/<name>.svg. UPDATE_SNAPSHOTS=1 rewrites the stored one."""

    def check(svg: str, name: str) -> None:
        stored = SNAPSHOTS / f"{name}.svg"
        if os.environ.get("UPDATE_SNAPSHOTS"):
            stored.write_text(svg)
            return
        if not stored.exists():
            pytest.fail(f"No snapshot at {stored}; rerun with UPDATE_SNAPSHOTS=1 and check the new file.")
        if svg != stored.read_text():
            actual = tmp_path / f"{name}.svg"
            actual.write_text(svg)
            pytest.fail(f"{name} changed. Compare {actual} with {stored}; UPDATE_SNAPSHOTS=1 accepts it.")

    return check
