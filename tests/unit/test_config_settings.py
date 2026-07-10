from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from tracker.types import InitResponse, ManagedRuntimeReadiness

import valkyrie.cli.config.settings as settings
import valkyrie.cli.config.state as config_state
from valkyrie.cli.exceptions import TrackerServiceError


def _config(tmp_path: Path) -> Path:
    path = tmp_path / "valkyrie.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "AWS_ACCESS_KEY_ID": "legacy-key",
                "AWS_SECRET_ACCESS_KEY": "legacy-secret",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "legacy-bucket",
                "LOG_GROUP": "legacy-logs",
                "LOG_RETENTION_POLICY": 30,
                "DAYTONA_SECRET_NAME": "legacy-provider",
            }
        )
    )
    return path


def _patch_config_path(monkeypatch: pytest.MonkeyPatch, path: Path) -> None:
    monkeypatch.setattr(settings, "CONFIG_LOCATION", path)
    monkeypatch.setattr(config_state, "CONFIG_LOCATION", path)


def test_hosted_init_is_default_and_preserves_legacy_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _config(tmp_path)
    _patch_config_path(monkeypatch, path)
    monkeypatch.delenv("VALKYRIE_API_KEY", raising=False)
    observed_keys: list[str] = []

    def init_org(api_key: str) -> InitResponse:
        observed_keys.append(api_key)
        return InitResponse(
            org_name="vals",
            created=False,
            email_claim_missing=False,
            runtime=ManagedRuntimeReadiness(),
        )

    monkeypatch.setattr(settings.TrackerService, "init_org", init_org)

    result = CliRunner().invoke(settings.init, input="\npersonal-key\n")

    assert result.exit_code == 0
    assert observed_keys == ["personal-key"]
    assert yaml.safe_load(path.read_text()) == {
        "AWS_ACCESS_KEY_ID": "legacy-key",
        "AWS_SECRET_ACCESS_KEY": "legacy-secret",
        "AWS_DEFAULT_REGION": "us-east-1",
        "S3_BUCKET": "legacy-bucket",
        "LOG_GROUP": "legacy-logs",
        "LOG_RETENTION_POLICY": 30,
        "DAYTONA_SECRET_NAME": "legacy-provider",
        "api_key": "personal-key",
    }
    assert "AWS_ACCESS_KEY_ID" not in result.output
    assert "personal-key" not in result.output
    assert "Organization 'vals' configured successfully" in result.output
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def test_hosted_init_does_not_write_when_runtime_is_not_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _config(tmp_path)
    original = path.read_text()
    _patch_config_path(monkeypatch, path)
    monkeypatch.setenv("VALKYRIE_API_KEY", "personal-key")

    def fail_init(_api_key: str) -> InitResponse:
        raise TrackerServiceError("Managed runtime is not ready")

    monkeypatch.setattr(settings.TrackerService, "init_org", fail_init)

    result = CliRunner().invoke(settings.init, input="\n")

    assert result.exit_code != 0
    assert "Managed runtime is not ready" in result.output
    assert path.read_text() == original


def test_self_hosted_init_removes_api_key_without_changing_aws_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _config(tmp_path)
    config = yaml.safe_load(path.read_text())
    config["api_key"] = "personal-key"
    path.write_text(yaml.safe_dump(config))
    _patch_config_path(monkeypatch, path)

    result = CliRunner().invoke(settings.init, input="self-hosted\n")

    assert result.exit_code == 0
    saved = yaml.safe_load(path.read_text())
    assert "api_key" not in saved
    assert saved["AWS_ACCESS_KEY_ID"] == "legacy-key"
    assert saved["S3_BUCKET"] == "legacy-bucket"
