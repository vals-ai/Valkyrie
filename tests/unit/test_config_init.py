from importlib import import_module
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

settings = import_module("valkyrie.cli.config.settings")
state = import_module("valkyrie.cli.config.state")


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "valkyrie.yaml"
    monkeypatch.setattr(state, "CONFIG_LOCATION", path)
    monkeypatch.setattr(settings, "CONFIG_LOCATION", path)
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


def test_init_whitespace_only_required_value_aborts(config_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for key in settings._REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(key, raising=False)

    runner = CliRunner()
    result = runner.invoke(settings.init, input="self-hosted\n   \n")

    assert result.exit_code != 0
    assert "AWS_ACCESS_KEY_ID is required" in result.output
