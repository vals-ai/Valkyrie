"""Tests for CLI S3 credential construction.

Run: uv run pytest tests/unit/cli/test_s3_config.py
"""

from datetime import datetime, timezone
from types import TracebackType
from typing import Any

import pytest
from botocore.exceptions import ClientError

from valkyrie.cli import s3_config as cli_s3

_BASE_CONFIG = {
    "AWS_ACCESS_KEY_ID": "ASIAEXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "secret",
    "AWS_DEFAULT_REGION": "us-east-1",
    "S3_BUCKET": "bucket",
}


class FakeS3Client:
    def __init__(self, *, head_error: ClientError | None = None) -> None:
        self.head_error = head_error
        self.head_calls: list[dict[str, str]] = []
        self.copy_calls: list[dict[str, object]] = []
        self.paginate_calls: list[dict[str, str]] = []

    async def __aenter__(self) -> "FakeS3Client":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def head_object(self, *, Bucket: str, Key: str) -> None:
        self.head_calls.append({"Bucket": Bucket, "Key": Key})
        if self.head_error is not None:
            raise self.head_error

    async def copy_object(self, *, Bucket: str, CopySource: dict[str, str], Key: str) -> None:
        self.copy_calls.append({"Bucket": Bucket, "CopySource": CopySource, "Key": Key})

    def get_paginator(self, name: str) -> "FakeS3Client":
        assert name == "list_objects_v2"
        return self

    async def paginate(self, *, Bucket: str, Prefix: str) -> Any:
        self.paginate_calls.append({"Bucket": Bucket, "Prefix": Prefix})
        modified = datetime(2026, 1, 2, tzinfo=timezone.utc)
        yield {
            "Contents": [
                {"Key": "agents/alpha.zip", "LastModified": modified},
                {"Key": "agents/README.md", "LastModified": modified},
                {"Key": "agents/beta.zip"},
            ]
        }


def test_aws_credentials_passes_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Temporary credentials (SSO / assumed role) must keep their session token."""
    monkeypatch.setattr(cli_s3, "load_config", lambda: {**_BASE_CONFIG, "AWS_SESSION_TOKEN": "token123"})
    credentials = cli_s3.aws_credentials()
    assert credentials.aws_session_token == "token123"
    assert credentials.aws_access_key_id == "ASIAEXAMPLE"


def test_aws_credentials_without_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Static credentials keep working with no token configured."""
    monkeypatch.setattr(cli_s3, "load_config", lambda: dict(_BASE_CONFIG))
    credentials = cli_s3.aws_credentials()
    assert credentials.aws_session_token is None


def test_s3_client_reuses_one_session_with_pool_config(monkeypatch: pytest.MonkeyPatch) -> None:
    session_constructions: list[dict[str, object]] = []
    client_configs: list[Any] = []
    client = object()

    class FakeSession:
        def __init__(self, **kwargs: object) -> None:
            session_constructions.append(kwargs)

        def client(self, service_name: str, config: Any = None) -> object:
            assert service_name == "s3"
            client_configs.append(config)
            return client

    monkeypatch.setattr(
        cli_s3,
        "load_config",
        lambda: {
            "AWS_ACCESS_KEY_ID": "aws-key-cached",
            "AWS_SECRET_ACCESS_KEY": "aws-secret-cached",
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    )
    monkeypatch.setattr(cli_s3.aioboto3, "Session", FakeSession)

    assert cli_s3.s3_client() is client
    assert cli_s3.s3_client() is client

    assert session_constructions == [
        {
            "aws_access_key_id": "aws-key-cached",
            "aws_secret_access_key": "aws-secret-cached",
            "region_name": "us-west-2",
        }
    ]
    assert [config.max_pool_connections for config in client_configs] == [200, 200]


@pytest.mark.asyncio
async def test_agent_s3_operations_preserve_existing_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeS3Client()
    monkeypatch.setattr(cli_s3, "s3_client", lambda: client)

    assert await cli_s3.s3_object_exists("agents/alpha.zip", "test-bucket")
    await cli_s3.copy_s3_object(
        "agents/alpha.zip",
        "benchmarks/benchmark-1/alpha.zip",
        "test-bucket",
    )
    agents = await cli_s3.list_agents("test-bucket")

    assert client.head_calls == [{"Bucket": "test-bucket", "Key": "agents/alpha.zip"}]
    assert client.copy_calls == [
        {
            "Bucket": "test-bucket",
            "CopySource": {"Bucket": "test-bucket", "Key": "agents/alpha.zip"},
            "Key": "benchmarks/benchmark-1/alpha.zip",
        }
    ]
    assert client.paginate_calls == [{"Bucket": "test-bucket", "Prefix": "agents/"}]
    assert agents == [
        ("alpha", datetime(2026, 1, 2, tzinfo=timezone.utc)),
        ("beta", None),
    ]


@pytest.mark.asyncio
async def test_s3_object_exists_returns_false_for_404(monkeypatch: pytest.MonkeyPatch) -> None:
    error = ClientError({"Error": {"Code": "404", "Message": "Not found")}, "HeadObject")
    monkeypatch.setattr(cli_s3, "s3_client", lambda: FakeS3Client(head_error=error))

    assert not await cli_s3.s3_object_exists("agents/missing.zip", "test-bucket")
