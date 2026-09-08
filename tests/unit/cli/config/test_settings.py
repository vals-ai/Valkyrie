"""Tests for interactive CLI configuration settings.

Run: uv run pytest tests/unit/cli/config/test_settings.py
"""

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import yaml
from click.testing import CliRunner

import pytest

settings = import_module("valkyrie.cli.config.settings")


def test_init_self_hosted_strips_whitespace(config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for key in settings._REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(key, raising=False)

    runner = CliRunner()
    result = runner.invoke(
        settings.init,
        input="\n".join(
            [
                "self-hosted",
                "  aws-key  ",
                " aws-secret\t",
                " us-east-1 ",
                " bucket ",
                "  benchmarks  ",
                " 365 ",
            ]
        )
        + "\n",
    )

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert config == {
        "AWS_ACCESS_KEY_ID": "aws-key",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
        "S3_BUCKET": "bucket",
        "LOG_GROUP": "benchmarks",
        "LOG_RETENTION_POLICY": "365",
    }


@pytest.mark.parametrize(
    ("selection", "environment", "tracker_url", "tracker_url_override"),
    [
        ("bench", "bench", "https://benchmark-tracker.vals.ai", None),
        ("prod", "prod", "https://benchmark-tracker-prod.vals.ai", None),
        ("prod", "prod", "https://tracker.example.test", "https://tracker.example.test"),
    ],
)
def test_init_hosted_strips_api_key(
    config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    selection: str,
    environment: str,
    tracker_url: str,
    tracker_url_override: str | None,
) -> None:
    for key in settings._REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
    if tracker_url_override is None:
        monkeypatch.delenv(settings.TRACKER_SERVICE_URL_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(settings.TRACKER_SERVICE_URL_ENV_VAR, tracker_url_override)
    config_path.write_text(
        yaml.safe_dump(
            {
                "api_key": "old-key",
                "benchmark_auth": {
                    "raw-key-service": "old-key",
                    "bearer-service": "Bearer old-key",
                    "independent-service": "independent-key",
                },
            }
        )
    )

    init_org_calls: list[tuple[str, str]] = []
    runtime_metadata_calls: list[tuple[str, str]] = []

    def mock_init_org(api_key: str, base_url: str) -> dict[str, object]:
        init_org_calls.append((api_key, base_url))

        return {"org_name": "test-org"}

    def mock_aws_runtime_metadata(api_key: str, base_url: str) -> SimpleNamespace:
        runtime_metadata_calls.append((api_key, base_url))
        return SimpleNamespace(mode="access_key", region=None, s3_bucket=None)

    monkeypatch.setattr(settings.TrackerService, "init_org", mock_init_org)
    monkeypatch.setattr(settings.TrackerService, "aws_runtime_metadata", mock_aws_runtime_metadata)

    runner = CliRunner()
    result = runner.invoke(
        settings.init,
        input="\n".join(
            [
                "hosted",
                selection,
                "  secret-key  ",
                "aws-key",
                "aws-secret",
                "us-east-1",
                "bucket",
                "benchmarks",
                "365",
            ]
        )
        + "\n",
    )

    assert result.exit_code == 0, result.output
    assert init_org_calls == [("secret-key", tracker_url)]
    assert runtime_metadata_calls == [("secret-key", tracker_url)]
    config = yaml.safe_load(config_path.read_text())
    assert config["api_key"] == "secret-key"
    assert config["environment"] == environment
    assert config["benchmark_auth"] == {
        "raw-key-service": "secret-key",
        "bearer-service": "Bearer secret-key",
        "independent-service": "independent-key",
    }


def test_init_hosted_managed_aws_omits_static_keys(config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Managed hosted setup stores deployment resources without static AWS credentials."""
    for key in settings._REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
    config_path.write_text(
        yaml.safe_dump(
            {
                "AWS_ACCESS_KEY_ID": "old-key",
                "AWS_SECRET_ACCESS_KEY": "old-secret",
                "AWS_SESSION_TOKEN": "old-session",
            }
        )
    )
    monkeypatch.setattr(
        settings.TrackerService,
        "init_org",
        lambda _api_key, _base_url: {"org_name": "test-org"},
    )
    monkeypatch.setattr(
        settings.TrackerService,
        "aws_runtime_metadata",
        lambda _api_key, _base_url: SimpleNamespace(mode="managed", region="us-east-1", s3_bucket="managed-bucket"),
    )

    result = CliRunner().invoke(settings.init, input="hosted\nbench\nvals-key\n\n\n")

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert config["api_key"] == "vals-key"
    assert config["AWS_DEFAULT_REGION"] == "us-east-1"
    assert config["S3_BUCKET"] == "managed-bucket"
    assert config["LOG_GROUP"] == "benchmarks"
    assert config["LOG_RETENTION_POLICY"] == "365"
    assert not settings._STATIC_AWS_CREDENTIAL_KEYS.intersection(config)
    assert "Local AWS operations will use the AWS SDK credential chain" in result.output
    assert "Restore them before retrying or resuming an access-key run" in result.output


@pytest.mark.usefixtures("config_path")
def test_init_hosted_names_incomplete_managed_aws_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
    monkeypatch.setattr(
        settings.TrackerService,
        "init_org",
        lambda _api_key, _base_url: {"org_name": "test-org"},
    )
    monkeypatch.setattr(
        settings.TrackerService,
        "aws_runtime_metadata",
        lambda _api_key, _base_url: SimpleNamespace(mode="managed", region=None, s3_bucket="managed-bucket"),
    )

    result = CliRunner().invoke(settings.init, input="hosted\nbench\nvals-key\n")

    assert result.exit_code == 1
    assert "Managed AWS configuration is missing its Region or S3 bucket" in result.output


@pytest.mark.usefixtures("config_path")
def test_init_hosted_names_runtime_discovery_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
    monkeypatch.setattr(
        settings.TrackerService,
        "init_org",
        lambda _api_key, _base_url: {"org_name": "test-org"},
    )

    def fail_runtime_discovery(_api_key: str, _base_url: str) -> None:
        raise settings.TrackerServiceError("Failed to resolve AWS runtime: service unavailable")

    monkeypatch.setattr(settings.TrackerService, "aws_runtime_metadata", fail_runtime_discovery)

    result = CliRunner().invoke(settings.init, input="hosted\nbench\nvals-key\n")

    assert result.exit_code == 1
    assert "Error: Failed to resolve AWS runtime: service unavailable" in result.output


@pytest.mark.usefixtures("config_path")
def test_init_whitespace_only_required_value_aborts(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in settings._REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(key, raising=False)

    runner = CliRunner()
    result = runner.invoke(settings.init, input="self-hosted\n   \n")

    assert result.exit_code != 0
    assert "AWS_ACCESS_KEY_ID is required" in result.output


def test_set_api_key_rotates_matching_benchmark_auth(config_path: Path) -> None:
    config_path.write_text(
        yaml.safe_dump(
            {
                "api_key": "old-key",
                "benchmark_auth": {
                    "raw-key-service": "old-key",
                    "bearer-service": "Bearer old-key",
                    "independent-service": "independent-key",
                },
            }
        )
    )

    result = CliRunner().invoke(settings.set, ["api_key", "new-key"])

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert config["api_key"] == "new-key"
    assert config["benchmark_auth"] == {
        "raw-key-service": "new-key",
        "bearer-service": "Bearer new-key",
        "independent-service": "independent-key",
    }
    assert "Updated benchmark service auth for 2 benchmarks." in result.output


def test_set_api_key_without_previous_key_preserves_benchmark_auth(config_path: Path) -> None:
    config_path.write_text(yaml.safe_dump({"benchmark_auth": {"independent-service": "independent-key"}}))

    result = CliRunner().invoke(settings.set, ["api_key", "new-key"])

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert config["api_key"] == "new-key"
    assert config["benchmark_auth"] == {"independent-service": "independent-key"}
    assert "Updated benchmark service auth" not in result.output


def test_set_aws_session_token_without_printing_value(config_path: Path) -> None:
    """Temporary AWS credentials can be configured without echoing the token."""
    config_path.write_text(yaml.safe_dump({"AWS_ACCESS_KEY_ID": "ASIAEXAMPLE"}))

    result = CliRunner().invoke(settings.set, ["AWS_SESSION_TOKEN", "temporary-token"])

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert config["AWS_SESSION_TOKEN"] == "temporary-token"
    assert "temporary-token" not in result.output
