import functools

import httpx
import pytest
from click.testing import CliRunner

from tests.unit.cli.fake_tracker import FakeTracker


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide an isolated Click command runner."""
    return CliRunner()


@pytest.fixture
def fake_tracker(monkeypatch: pytest.MonkeyPatch) -> FakeTracker:
    """Point the CLI at a FakeTracker with an API key."""
    tracker = FakeTracker()
    monkeypatch.setattr(httpx, "Client", functools.partial(httpx.Client, transport=httpx.MockTransport(tracker.handle)))
    monkeypatch.setenv("VALKYRIE_URL", "https://tracker.test")
    monkeypatch.setenv("VALKYRIE_API_KEY", "test-key")
    return tracker
