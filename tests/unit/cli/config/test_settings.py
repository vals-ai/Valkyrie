"""Tests for interactive CLI configuration settings.

Run: uv run pytest tests/unit/cli/config/test_settings.py
"""

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import yaml
from click.testing import CliRunner

import pytest
from valkyrie.sdk import ValkyrieConfig


settings = import_module("valkyrie.cli.config.settings")


def test_self_hosted_setup_preserves_provider_order_without_aws_prompts(
    config_path: Path, cli_runner: CliRunner
) -> None:
    providers = {"modal": "ModalSecrets", "daytona": "DaytonaSecrets"}
    config_path.write_text(
        yaml.safe_dump({"sandbox_providers": providers, "aws": {"S3_BUCKET": "unused"}}, sort_keys=False)
    )

    result = cli_runner.invoke(settings.init, input="self-hosted\n")

    assert result.exit_code == 0, result.output
    config = ValkyrieConfig.from_yaml(config_path)
    assert config.resolve_sandbox_provider() == ("modal", "ModalSecrets")
    assert "AWS_DEFAULT_REGION" not in result.output
    assert yaml.safe_load(config_path.read_text()) == {"sandbox_providers": providers}


@pytest.mark.parametrize("key", ["AWS_DEFAULT_REGION", "S3_BUCKET", "LOG_GROUP", "LOG_RETENTION_POLICY"])
def test_client_config_rejects_aws_resource_keys(key: str, config_path: Path, cli_runner: CliRunner) -> None:
    config_path.write_text("api_key: test-key\n")

    result = cli_runner.invoke(settings.set, [key, "resource"])

    assert result.exit_code == 1
    assert "not a valid config key" in result.output
    assert config_path.read_text() == "api_key: test-key\n"


def test_provider_commands_preserve_api_key_and_default(config_path: Path, cli_runner: CliRunner) -> None:
    provider_group = import_module("valkyrie.cli.config.providers").provider
    config_path.write_text("api_key: test-key\n")
    for arguments in [["set", "modal", "ModalSecrets"], ["set", "daytona", "DaytonaSecrets"], ["default", "daytona"]]:
        result = cli_runner.invoke(provider_group, arguments)
        assert result.exit_code == 0, result.output

    assert ValkyrieConfig.from_yaml(config_path).resolve_sandbox_provider() == ("daytona", "DaytonaSecrets")
    result = cli_runner.invoke(provider_group, ["list"])
    assert "modal: ModalSecrets" in result.output
    assert "daytona: DaytonaSecrets" in result.output
    result = cli_runner.invoke(provider_group, ["remove", "daytona"])
    assert result.exit_code == 0, result.output
    assert ValkyrieConfig.from_yaml(config_path).resolve_sandbox_provider() == ("modal", "ModalSecrets")
    assert yaml.safe_load(config_path.read_text())["api_key"] == "test-key"


@pytest.mark.parametrize(
    "arguments", [["auth", "set", "swebench", "service-key"], ["service", "set", "swebench", "https://bench.example"]]
)
def test_config_commands_preserve_implicit_provider_default(
    arguments: list[str], config_path: Path, cli_runner: CliRunner
) -> None:
    config_group = import_module("valkyrie.cli.config").config
    config_path.write_text(
        yaml.safe_dump({"sandbox_providers": {"modal": "ModalSecrets", "daytona": "DaytonaSecrets"}}, sort_keys=False)
    )

    result = cli_runner.invoke(config_group, arguments)

    assert result.exit_code == 0, result.output
    assert ValkyrieConfig.from_yaml(config_path).resolve_sandbox_provider() == ("modal", "ModalSecrets")


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
        return SimpleNamespace(mode="unavailable", region=None, s3_bucket=None)

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


def test_init_hosted_managed_needs_only_api_key(config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
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

    result = CliRunner().invoke(settings.init, input="hosted\nbench\nvals-key\n")

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert config == {"api_key": "vals-key", "environment": "bench"}
    assert "Managed AWS execution is enabled" in result.output


def test_init_hosted_preserves_provider_choices_and_drops_client_aws_settings(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
    config_path.write_text(
        yaml.safe_dump(
            {
                "api_key": "old-key",
                "aws": {
                    "AWS_DEFAULT_REGION": "us-east-1",
                    "S3_BUCKET": "managed-bucket",
                    "LOG_GROUP": "benchmarks",
                    "LOG_RETENTION_POLICY": 365,
                },
                "sandbox_providers": {"daytona": "AgenticHarnessSecrets"},
                "default_sandbox_provider": "daytona",
                "benchmark_auth": {"svc": "old-key"},
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

    result = CliRunner().invoke(settings.init, input="hosted\nbench\nnew-key\n")

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(config_path.read_text())
    assert "aws" not in config
    assert config["sandbox_providers"] == {"daytona": "AgenticHarnessSecrets"}
    assert config["default_sandbox_provider"] == "daytona"
    assert config["api_key"] == "new-key"
    assert config["environment"] == "bench"
    assert config["benchmark_auth"] == {"svc": "new-key"}
    assert "new-key" not in result.output


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


def test_set_unknown_key_preserves_config(config_path: Path) -> None:
    saved = {"api_key": "test-key"}
    config_path.write_text(yaml.safe_dump(saved))

    result = CliRunner().invoke(settings.set, ["unknown-key", "new-value"])

    assert result.exit_code != 0
    assert "not a valid config key" in result.output
    assert yaml.safe_load(config_path.read_text()) == saved


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


def test_init_hosted_rejects_blank_environment_key(
    config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
) -> None:
    monkeypatch.setenv("VALKYRIE_API_KEY", "   ")

    result = cli_runner.invoke(settings.init, input="hosted\nbench\n")

    assert result.exit_code == 1
    assert "API key must not be blank" in result.output
    assert not config_path.exists()
