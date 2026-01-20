import pytest


@pytest.fixture(autouse=True)
def set_unit_test_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test_key")
    monkeypatch.setenv("DAYTONA_API_URL", "http://test.url")
    monkeypatch.setenv("DAYTONA_TARGET", "test_target")
