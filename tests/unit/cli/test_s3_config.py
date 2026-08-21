"""Tests for CLI S3 credential selection.

Run: uv run pytest tests/unit/cli/test_s3_config.py
"""

import click
import pytest
from tracker.aws.clients import DefaultChainAWSClientProvider, ExplicitCredentialsAWSClientProvider

from valkyrie.cli import s3_config

_BASE_CONFIG = {
    "AWS_DEFAULT_REGION": "us-east-1",
    "S3_BUCKET": "bucket",
}


@pytest.fixture(autouse=True)
def clear_aws_runtime_cache() -> None:
    s3_config._aws_runtime.cache_clear()


@pytest.mark.parametrize("session_token", [None, "token123"])
def test_aws_runtime_uses_configured_credentials(
    monkeypatch: pytest.MonkeyPatch,
    session_token: str | None,
) -> None:
    config = {
        **_BASE_CONFIG,
        "AWS_ACCESS_KEY_ID": "ASIAEXAMPLE",
        "AWS_SECRET_ACCESS_KEY": "secret",
    }
    if session_token is not None:
        config["AWS_SESSION_TOKEN"] = session_token
    monkeypatch.setattr(s3_config, "load_config", lambda: config)

    runtime = s3_config.aws_runtime()

    assert isinstance(runtime.clients, ExplicitCredentialsAWSClientProvider)
    assert runtime.clients.credentials.aws_access_key_id == "ASIAEXAMPLE"
    assert runtime.clients.credentials.aws_secret_access_key == "secret"
    assert runtime.clients.credentials.aws_session_token == session_token


def test_aws_runtime_uses_sdk_credential_chain_without_configured_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s3_config, "load_config", lambda: dict(_BASE_CONFIG))

    runtime = s3_config.aws_runtime()

    assert isinstance(runtime.clients, DefaultChainAWSClientProvider)
    assert runtime.clients.region == "us-east-1"


def test_aws_runtime_names_missing_region_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_config, "load_config", lambda: {"S3_BUCKET": "bucket"})

    with pytest.raises(click.ClickException, match="AWS_DEFAULT_REGION key not found"):
        s3_config.aws_runtime()


@pytest.mark.parametrize(
    "config",
    [
        {**_BASE_CONFIG, "AWS_ACCESS_KEY_ID": "aws-key"},
        {**_BASE_CONFIG, "AWS_SECRET_ACCESS_KEY": "aws-secret"},
    ],
)
def test_aws_runtime_rejects_partial_explicit_credentials(
    monkeypatch: pytest.MonkeyPatch,
    config: dict[str, str],
) -> None:
    monkeypatch.setattr(s3_config, "load_config", lambda: config)

    with pytest.raises(
        click.ClickException,
        match="AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together",
    ):
        s3_config.aws_runtime()
