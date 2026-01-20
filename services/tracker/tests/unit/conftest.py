import pytest


@pytest.fixture(autouse=True)
def unit_test_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test_key")
    monkeypatch.setenv("DAYTONA_API_URL", "http://test.url")
    monkeypatch.setenv("DAYTONA_TARGET", "test_target")
