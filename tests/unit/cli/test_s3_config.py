"""Tests for CLI S3 credential construction.

Run: uv run pytest tests/unit/cli/test_s3_config.py
"""

import pytest

from valkyrie.cli import s3_config

_BASE_CONFIG = {
    "AWS_ACCESS_KEY_ID": "ASIAEXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "secret",
    "AWS_DEFAULT_REGION": "us-east-1",
    "S3_BUCKET": "bucket",
}


def test_aws_credentials_passes_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Temporary credentials (SSO / assumed role) must keep their session token."""
    monkeypatch.setattr(s3_config, "load_config", lambda: {**_BASE_CONFIG, "AWS_SESSION_TOKEN": "token123"})
    creds = s3_config.aws_credentials()
    assert creds.aws_session_token == "token123"
    assert creds.aws_access_key_id == "ASIAEXAMPLE"


def test_aws_credentials_without_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Static credentials keep working with no token configured."""
    monkeypatch.setattr(s3_config, "load_config", lambda: dict(_BASE_CONFIG))
    creds = s3_config.aws_credentials()
    assert creds.aws_session_token is None
