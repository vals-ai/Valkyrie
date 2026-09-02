"""Tests for CLI environment and configuration selection.

Run: uv run pytest tests/unit/cli/test_runtime_config.py
"""

from pathlib import Path

import pytest

from valkyrie.cli import runtime_config
from valkyrie.cli.runtime_config import (
    BENCH_ENVIRONMENT,
    BENCH_TRACKER_URL,
    DEV_CONFIG_PATH,
    DEV_TRACKER_URL,
    PRODUCTION_TRACKER_URL,
    VALKYRIE_CONFIG_PATH_ENV_VAR,
    VALKYRIE_ENV_ENV_VAR,
    TRACKER_SERVICE_URL_ENV_VAR,
    config_location,
    selected_environment,
    tracker_service_url,
)


@pytest.fixture(autouse=True)
def clear_runtime_config_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv(VALKYRIE_ENV_ENV_VAR, raising=False)
    monkeypatch.delenv(TRACKER_SERVICE_URL_ENV_VAR, raising=False)
    monkeypatch.delenv(VALKYRIE_CONFIG_PATH_ENV_VAR, raising=False)
    monkeypatch.setattr(runtime_config, "HOSTED_CONFIG_PATH", tmp_path / "valkyrie.yaml")


def test_default_environment_keeps_existing_bench_config() -> None:
    assert selected_environment() == BENCH_ENVIRONMENT
    assert tracker_service_url() == BENCH_TRACKER_URL
    assert config_location() == runtime_config.HOSTED_CONFIG_PATH


@pytest.mark.parametrize(
    ("environment", "tracker_url"),
    [
        ("prod", BENCH_TRACKER_URL),
        ("external", PRODUCTION_TRACKER_URL),
    ],
)
def test_explicit_environment_preserves_public_selector_compatibility(
    monkeypatch: pytest.MonkeyPatch,
    environment: str,
    tracker_url: str,
) -> None:
    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, environment)

    assert tracker_service_url() == tracker_url


def test_dev_environment_uses_dev_tracker_and_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, "dev")

    assert tracker_service_url() == DEV_TRACKER_URL
    assert config_location() == DEV_CONFIG_PATH.expanduser()


def test_prod_environment_uses_persisted_tracker_selection() -> None:
    runtime_config.HOSTED_CONFIG_PATH.write_text("environment: prod\n", encoding="utf-8")

    assert tracker_service_url() == PRODUCTION_TRACKER_URL
    assert config_location() == runtime_config.HOSTED_CONFIG_PATH


def test_unknown_persisted_environment_is_rejected() -> None:
    runtime_config.HOSTED_CONFIG_PATH.write_text("environment: staging\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Config key 'environment' must be one of: bench, dev, prod"):
        selected_environment()


def test_explicit_tracker_url_overrides_selected_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, "dev")
    monkeypatch.setenv(TRACKER_SERVICE_URL_ENV_VAR, "https://tracker.example.com")

    assert tracker_service_url() == "https://tracker.example.com"


def test_explicit_config_path_overrides_selected_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "custom.yaml"
    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, "dev")
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))

    assert config_location() == config_path


def test_unknown_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(VALKYRIE_ENV_ENV_VAR, "staging")

    with pytest.raises(
        ValueError,
        match="Unknown VALKYRIE_ENV='staging'. Must be one of: bench, dev, external, prod",
    ):
        selected_environment()
