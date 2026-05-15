"""Unit tests for config helpers in valkyrie.cli.utils."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import pytest
import yaml

from valkyrie.cli.utils import (
    HOSTED_TRACKER_URL_DEFAULT,
    infer_mode,
    resolve_tracker_url,
    save_config,
    validate_mode_requirements,
)


@pytest.fixture
def fake_config_location(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point CONFIG_LOCATION at a tmp file for the duration of the test."""
    fake = tmp_path / "valkyrie.yaml"
    monkeypatch.setattr("valkyrie.cli.utils.CONFIG_LOCATION", fake)
    return fake


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f)


# ---------- resolve_tracker_url ----------


@pytest.mark.parametrize(
    "env_value, config, expected_url, expects_error",
    [
        # env wins over everything
        (
            "https://from-env",
            {"mode": "self-hosted", "tracker_service_url": "https://from-config"},
            "https://from-env",
            False,
        ),
        # hosted mode ignores the config field, returns hosted default
        (
            None,
            {"mode": "hosted", "tracker_service_url": "https://stale-localhost"},
            HOSTED_TRACKER_URL_DEFAULT,
            False,
        ),
        # hosted mode with no field -> hosted default
        (None, {"mode": "hosted"}, HOSTED_TRACKER_URL_DEFAULT, False),
        # self-hosted with config field -> field wins
        (
            None,
            {"mode": "self-hosted", "tracker_service_url": "http://localhost:8000"},
            "http://localhost:8000",
            False,
        ),
        # self-hosted with no URL and no env var -> error
        (None, {"mode": "self-hosted"}, None, True),
    ],
)
def test_tracker_url_resolution(
    env_value: str | None,
    config: dict[str, Any],
    expected_url: str | None,
    expects_error: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if env_value is None:
        monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)
    else:
        monkeypatch.setenv("TRACKER_SERVICE_URL", env_value)

    if expects_error:
        with pytest.raises(click.ClickException) as exc_info:
            resolve_tracker_url(config)
        assert "self-hosted" in str(exc_info.value.message).lower()
        assert "tracker_service_url" in exc_info.value.message
    else:
        assert resolve_tracker_url(config) == expected_url


# ---------- config init regression: api_key preserved on mode flip ----------


def test_config_init_preserves_api_key_on_self_hosted_reinit(
    fake_config_location: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: switching mode via init must not delete the Descope api_key."""
    from click.testing import CliRunner

    from valkyrie.cli.main import cli

    _write(
        fake_config_location,
        {
            "mode": "hosted",
            "api_key": "preserved-descope-key",
            "AWS_ACCESS_KEY_ID": "AKIA-test",
            "AWS_SECRET_ACCESS_KEY": "secret-test",
            "AWS_DEFAULT_REGION": "us-west-2",
            "S3_BUCKET": "test-bucket",
            "DAYTONA_SECRET_NAME": "test-daytona",
        },
    )
    monkeypatch.setattr("valkyrie.cli.main.CONFIG_LOCATION", fake_config_location)
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)

    runner = CliRunner()
    # Inputs: mode=self-hosted, tracker_service_url=default (Enter), then re-confirm any AWS values
    result = runner.invoke(
        cli,
        ["config", "init"],
        input="self-hosted\n\n\n\n\n\n\n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    with open(fake_config_location) as f:
        persisted = yaml.safe_load(f)

    assert persisted["api_key"] == "preserved-descope-key"
    assert persisted["mode"] == "self-hosted"
    assert persisted["tracker_service_url"] == "http://localhost:8000"


# ---------- config mode validation ----------


def test_validate_mode_hosted_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)
    with pytest.raises(click.ClickException) as exc:
        validate_mode_requirements({"mode": "self-hosted"}, "hosted")
    assert "api_key" in exc.value.message
    assert "config init" in exc.value.message or "config set api_key" in exc.value.message


def test_validate_mode_self_hosted_requires_tracker_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)
    with pytest.raises(click.ClickException) as exc:
        validate_mode_requirements({"mode": "hosted", "api_key": "x"}, "self-hosted")
    assert "tracker_service_url" in exc.value.message


def test_validate_mode_self_hosted_rejects_env_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persisting self-hosted mode requires a config-level URL.

    The env var is a runtime override only; persisting mode based on it would leave the
    config in a broken state once the env var is unset.
    """
    monkeypatch.setenv("TRACKER_SERVICE_URL", "http://localhost:8000")
    with pytest.raises(click.ClickException) as exc:
        validate_mode_requirements({"mode": "hosted", "api_key": "x"}, "self-hosted")
    assert "tracker_service_url" in exc.value.message


# ---------- legacy migration ----------


@pytest.mark.parametrize(
    "config, expected_mode",
    [
        ({"api_key": "k"}, "hosted"),
        ({}, "self-hosted"),
        ({"AWS_ACCESS_KEY_ID": "x"}, "self-hosted"),
    ],
)
def test_infer_mode_from_legacy_config(config: dict[str, Any], expected_mode: str) -> None:
    assert infer_mode(config) == expected_mode


def test_save_config_backfills_missing_mode(fake_config_location: Path) -> None:
    """save_config should write `mode` when the input config lacks it."""
    save_config({"api_key": "k", "AWS_ACCESS_KEY_ID": "x"})
    with open(fake_config_location) as f:
        persisted = yaml.safe_load(f)
    assert persisted["mode"] == "hosted"
    assert persisted["api_key"] == "k"


# ---------- config mode CLI ----------


def test_config_mode_flips_without_reprompting(
    fake_config_location: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from click.testing import CliRunner
    from valkyrie.cli.main import cli

    _write(
        fake_config_location,
        {
            "mode": "hosted",
            "api_key": "k",
            "tracker_service_url": "http://localhost:8000",
            "AWS_ACCESS_KEY_ID": "x",
            "AWS_SECRET_ACCESS_KEY": "y",
            "AWS_DEFAULT_REGION": "us-west-2",
            "S3_BUCKET": "b",
            "DAYTONA_SECRET_NAME": "d",
        },
    )
    monkeypatch.setattr("valkyrie.cli.main.CONFIG_LOCATION", fake_config_location)
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)

    result = CliRunner().invoke(cli, ["config", "mode", "self-hosted"], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert "self-hosted" in result.output
    assert "http://localhost:8000" in result.output

    persisted = yaml.safe_load(fake_config_location.read_text())
    assert persisted["mode"] == "self-hosted"
    assert persisted["api_key"] == "k"  # preserved


def test_config_mode_hosted_without_api_key_errors(
    fake_config_location: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from click.testing import CliRunner
    from valkyrie.cli.main import cli

    _write(fake_config_location, {"mode": "self-hosted", "tracker_service_url": "http://x"})
    monkeypatch.setattr("valkyrie.cli.main.CONFIG_LOCATION", fake_config_location)
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)

    result = CliRunner().invoke(cli, ["config", "mode", "hosted"])
    assert result.exit_code != 0
    assert "api_key" in result.output


def test_config_mode_self_hosted_without_url_errors(
    fake_config_location: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from click.testing import CliRunner
    from valkyrie.cli.main import cli

    _write(fake_config_location, {"mode": "hosted", "api_key": "k"})
    monkeypatch.setattr("valkyrie.cli.main.CONFIG_LOCATION", fake_config_location)
    monkeypatch.delenv("TRACKER_SERVICE_URL", raising=False)

    result = CliRunner().invoke(cli, ["config", "mode", "self-hosted"])
    assert result.exit_code != 0
    assert "tracker_service_url" in result.output
