from importlib import import_module
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from valkyrie.cli.runtime_config import VALKYRIE_CONFIG_PATH_ENV_VAR

settings = import_module("valkyrie.cli.config.settings")


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "valkyrie.yaml"
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(path))
    return path


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


def test_init_hosted_strips_api_key(config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for key in settings._REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
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

    init_org_calls: list[str] = []

    class FakeTrackerService:
        @staticmethod
        def init_org(api_key: str) -> dict[str, object]:
            init_org_calls.append(api_key)
            return {"org_name": "test-org"}

    monkeypatch.setattr(settings, "TrackerService", FakeTrackerService)

    runner = CliRunner()
    result = runner.invoke(
        settings.init,
        input="\n".join(
            [
                "hosted",
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
    assert init_org_calls == ["secret-key"]
    config = yaml.safe_load(config_path.read_text())
    assert config["api_key"] == "secret-key"
    assert config["benchmark_auth"] == {
        "raw-key-service": "secret-key",
        "bearer-service": "Bearer secret-key",
        "independent-service": "independent-key",
    }


def test_init_whitespace_only_required_value_aborts(config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
