"""Tests for internal run snapshot storage.

Run: uv run pytest tests/unit/cli/agent/test_storage.py
"""

import io
import zipfile
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.exceptions import S3Error

from valkyrie.cli.agent import storage


class MockAsyncClientContext:
    """Provide an async context around a configured client mock."""

    def __init__(self, client: AsyncMock) -> None:
        self.client = client

    async def __aenter__(self) -> AsyncMock:
        return self.client

    async def __aexit__(self, *_exc_info: object) -> None:
        return None


def _configure_s3_clients(monkeypatch: pytest.MonkeyPatch, clients: list[AsyncMock]) -> None:
    def s3_client(provider: ExplicitCredentialsAWSClientProvider) -> MockAsyncClientContext:
        assert provider.credentials.aws_access_key_id == "key"
        assert provider.credentials.aws_secret_access_key == "secret"
        assert provider.credentials.aws_default_region == "us-east-1"

        return MockAsyncClientContext(clients.pop(0))

    monkeypatch.setattr(
        storage.cli_s3,
        "load_config",
        lambda: {
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "agent-bucket",
        },
    )
    monkeypatch.setattr(ExplicitCredentialsAWSClientProvider, "s3_client", s3_client)


async def test_ingest_reads_current_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("demo/contract.yaml", "ingest_lambda: demo-ingest")
    monkeypatch.setattr(storage.cli_s3, "aws_runtime", object)
    monkeypatch.setattr(storage, "s3_object_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(storage, "download_from_s3", AsyncMock(return_value=buffer.getvalue()))

    assert await storage.get_ingest_lambda_from_s3("demo") == "demo-ingest"

    monkeypatch.setattr(storage, "s3_object_exists", AsyncMock(return_value=False))

    with pytest.raises(S3Error, match="not found"):
        await storage.get_ingest_lambda_from_s3("missing")


async def test_update_benchmark_agent_version_copies_configured_agent_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    client.copy_object.return_value = {}
    _configure_s3_clients(monkeypatch, [client, client])

    await storage.update_benchmark_agent_version("alpha", "benchmark-1")

    client.copy_object.assert_awaited_once_with(
        Bucket="agent-bucket",
        CopySource={"Bucket": "agent-bucket", "Key": "agents/alpha.zip"},
        Key="benchmarks/benchmark-1/alpha.zip",
    )


async def test_run_start_publish_atomically_refuses_alias_collision(monkeypatch: pytest.MonkeyPatch) -> None:
    archive = b"agent archive"
    successful_client = AsyncMock()
    conflicting_client = AsyncMock()
    conflicting_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed"}},
        "PutObject",
    )
    _configure_s3_clients(monkeypatch, [successful_client, conflicting_client])

    def mock_zip_stream(agent_name: str, agent_path: Path) -> nullcontext[io.BytesIO]:
        assert agent_name == "demo"
        assert agent_path == Path("/unused")

        return nullcontext(io.BytesIO(archive))

    monkeypatch.setattr(storage, "get_agent_zip_stream", mock_zip_stream)

    assert await storage.push_agent_if_absent("demo", Path("/unused")) is True
    assert await storage.push_agent_if_absent("demo", Path("/unused")) is False

    successful_call = successful_client.put_object.await_args
    assert successful_call is not None
    assert successful_call.kwargs["Bucket"] == "agent-bucket"
    assert successful_call.kwargs["Key"] == "agents/demo.zip"
    assert successful_call.kwargs["ContentLength"] == len(archive)
    assert successful_call.kwargs["IfNoneMatch"] == "*"
