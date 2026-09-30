"""Tests for CLI S3 credential selection.

Run: uv run pytest tests/unit/cli/test_s3_config.py
"""

from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import yaml
import click
import pytest
from tracker.aws.clients import LocalChainAWSClientProvider, ExplicitCredentialsAWSClientProvider
from tracker.aws.s3 import download_from_s3

from valkyrie.cli import s3_config

_BASE_CONFIG = {
    "AWS_DEFAULT_REGION": "us-east-1",
    "S3_BUCKET": "bucket",
}


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    s3_config._aws_runtime.cache_clear()
    path = tmp_path / "config.yaml"
    monkeypatch.setattr(s3_config, "config_location", lambda: path)
    return path


@pytest.mark.parametrize("session_token", [None, "token123"])
def test_aws_runtime_uses_configured_credentials(
    config_path: Path,
    session_token: str | None,
) -> None:
    config = {"AWS_ACCESS_KEY_ID": "ASIAEXAMPLE", "AWS_SECRET_ACCESS_KEY": "secret"}
    if session_token is not None:
        config["AWS_SESSION_TOKEN"] = session_token
    config_path.write_text(
        yaml.safe_dump({"aws": {**_BASE_CONFIG, "credentials": config}, "sandbox_providers": {"modal": "TestSecret"}})
    )

    runtime = s3_config.aws_runtime()

    assert isinstance(runtime.clients, ExplicitCredentialsAWSClientProvider)
    assert runtime.clients.credentials.aws_access_key_id == "ASIAEXAMPLE"
    assert runtime.clients.credentials.aws_secret_access_key == "secret"
    assert runtime.clients.credentials.aws_session_token == session_token


def test_aws_runtime_uses_sdk_credential_chain_without_configured_keys(
    config_path: Path,
) -> None:
    config_path.write_text(yaml.safe_dump({"aws": _BASE_CONFIG}))

    runtime = s3_config.aws_runtime()

    assert isinstance(runtime.clients, LocalChainAWSClientProvider)
    assert runtime.clients.region == "us-east-1"


async def test_profile_credentials_can_read_without_deployment_account_configuration(
    config_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path.write_text(yaml.safe_dump({"aws": _BASE_CONFIG}))
    stream = AsyncMock()
    stream.__aenter__.return_value = stream
    stream.read.return_value = b"agent bundle"
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get_object.return_value = {"Body": stream}
    monkeypatch.setattr(LocalChainAWSClientProvider, "s3_client", lambda _provider: client)
    configured_runtime = s3_config.aws_runtime()
    runtime = configured_runtime.with_resources(replace(configured_runtime.resources, region="us-west-2"))

    assert await download_from_s3("agents/example.zip", runtime) == b"agent bundle"
    client.get_object.assert_awaited_once_with(Bucket="bucket", Key="agents/example.zip")


def test_aws_runtime_names_missing_region_configuration(config_path: Path) -> None:
    config_path.write_text(yaml.safe_dump({"aws": {"S3_BUCKET": "bucket"}}))

    with pytest.raises(click.ClickException, match="AWS_DEFAULT_REGION"):
        s3_config.aws_runtime()


@pytest.mark.parametrize(
    "config",
    [
        {"AWS_ACCESS_KEY_ID": "aws-key"},
        {"AWS_SECRET_ACCESS_KEY": "aws-secret"},
    ],
)
def test_aws_runtime_rejects_partial_explicit_credentials(
    config_path: Path,
    config: dict[str, str],
) -> None:
    config_path.write_text(
        yaml.safe_dump({"aws": {**_BASE_CONFIG, "credentials": config}, "sandbox_providers": {"modal": "TestSecret"}})
    )

    with pytest.raises(
        click.ClickException,
        match="Field required",
    ):
        s3_config.aws_runtime()
