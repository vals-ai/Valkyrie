"""Tests for CLI agent storage behavior.

Run: uv run pytest tests/unit/cli/agent/test_storage.py
"""

import io
import zipfile
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
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


def _agent_zip(agent_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr(
            f"{agent_name}/contract.yaml",
            "name: demo\ninstall_cmd: 'true'\nrun_cmd: 'true'\ningest_lambda: demo-ingest\n",
        )
    return buffer.getvalue()


def _configure_s3_clients(
    monkeypatch: pytest.MonkeyPatch,
    clients: list[AsyncMock],
) -> None:
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


async def test_agent_download_ingest_and_remove_use_configured_s3(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Agent storage should preserve download, ingest lookup, remove, and missing-agent behavior.

    Test cases:
    - Existing agent downloads, extracts, reads ingest metadata, and deletes from the configured bucket.
    - Missing agent raises the user-facing S3Error without mutating downloaded or deleted keys.
    """
    existing_keys = {"agents/demo.zip"}
    downloaded_keys: list[str] = []
    deleted_keys: list[str] = []
    runtime = object()

    async def s3_object_exists(key: str, runtime_arg: object) -> bool:
        assert runtime_arg is runtime
        return key in existing_keys

    async def download_from_s3(key: str, runtime_arg: object) -> bytes:
        assert runtime_arg is runtime
        downloaded_keys.append(key)
        return _agent_zip("demo")

    async def delete_from_s3(key: str, runtime_arg: object) -> None:
        assert runtime_arg is runtime
        deleted_keys.append(key)
        existing_keys.remove(key)

    monkeypatch.setattr(storage.cli_s3, "aws_runtime", lambda: runtime)
    monkeypatch.setattr(storage, "s3_object_exists", s3_object_exists)
    monkeypatch.setattr(storage, "download_from_s3", download_from_s3)
    monkeypatch.setattr(storage, "delete_from_s3", delete_from_s3)

    await storage.download_agent("demo", tmp_path)
    assert (tmp_path / "demo" / "contract.yaml").exists()
    assert await storage.get_ingest_lambda_from_s3("demo") == "demo-ingest"
    await storage.remove_agent("demo")

    assert downloaded_keys == ["agents/demo.zip", "agents/demo.zip"]
    assert deleted_keys == ["agents/demo.zip"]
    with pytest.raises(S3Error, match="Agent 'missing' not found in S3"):
        await storage.download_agent("missing", tmp_path)
    with pytest.raises(S3Error, match="Agent 'missing' could not be found"):
        await storage.remove_agent("missing")

    assert downloaded_keys == ["agents/demo.zip", "agents/demo.zip"]
    assert deleted_keys == ["agents/demo.zip"]


async def test_update_benchmark_agent_version_copies_configured_agent_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    client.copy_object.return_value = {}
    _configure_s3_clients(monkeypatch, [client, client])

    await storage.update_benchmark_agent_version("alpha", "benchmark-1")

    client.head_object.assert_awaited_once_with(
        Bucket="agent-bucket",
        Key="agents/alpha.zip",
    )
    client.copy_object.assert_awaited_once_with(
        Bucket="agent-bucket",
        CopySource={"Bucket": "agent-bucket", "Key": "agents/alpha.zip"},
        Key="benchmarks/benchmark-1/alpha.zip",
    )


class TestInstallAgent:
    """GitHub URL parsing and sparse-checkout installation behavior."""

    async def test_subfolder_install_clones_resolves_and_pushes_contract(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Subfolder installs must check out the requested branch and push the resolved agent.

        Test cases:
        - GitHub tree URLs use sparse checkout for the requested folder and branch.
        - The contract name is used when no CLI name override is provided.
        - Invalid non-GitHub URLs fail before any git or storage work begins.
        """
        git_calls: list[tuple[Path | None, tuple[str, ...]]] = []
        pushed_agents: list[tuple[str, Path]] = []

        async def mock_git(repo_path: Path | None, *arguments: str) -> None:
            git_calls.append((repo_path, arguments))
            if arguments[0] == "clone":
                checkout_root = Path(arguments[-1])
                agent_path = checkout_root / "agents" / "demo"
                agent_path.mkdir(parents=True)
                (agent_path / "contract.yaml").write_text(
                    "name: contract-agent\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n",
                    encoding="utf-8",
                )

        async def mock_push(agent_name: str, agent_path: Path) -> None:
            pushed_agents.append((agent_name, agent_path))

        monkeypatch.setattr(storage, "_run_git_command", mock_git)
        monkeypatch.setattr(storage, "push_agent", mock_push)

        resolved_name = await storage.install_agent(
            None,
            "https://github.com/vals-ai/agent-registry/tree/feature/agents/demo",
        )

        assert resolved_name == "contract-agent"
        assert [arguments[:3] for _, arguments in git_calls] == [
            ("clone", "--no-checkout", "--filter=blob:none"),
            ("sparse-checkout", "init", "--cone"),
            ("sparse-checkout", "set", "agents/demo"),
            ("checkout", "feature"),
            ("submodule", "update", "--init"),
        ]
        assert pushed_agents[0][0] == "contract-agent"
        assert pushed_agents[0][1].parts[-2:] == ("agents", "demo")

        with pytest.raises(RuntimeError, match="Only github is supported"):
            await storage.install_agent(None, "https://gitlab.com/vals-ai/agent")

        assert len(git_calls) == 5


class TestPushAgent:
    """Multipart agent uploads and failure cleanup."""

    async def test_multipart_upload_completes_or_aborts_atomically(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Multipart uploads must complete with ordered parts or abort after a failed part.

        Test cases:
        - A multi-part archive sends each ETag and part number to completion.
        - A failed part aborts the upload and preserves the original error.
        """
        archive = b"a" * (5 * 1024 * 1024 + 1)
        successful_client = AsyncMock()
        successful_client.create_multipart_upload.return_value = {"UploadId": "upload-success"}
        successful_client.upload_part.side_effect = [{"ETag": "etag-1"}, {"ETag": "etag-2"}]
        failing_client = AsyncMock()
        failing_client.create_multipart_upload.return_value = {"UploadId": "upload-failure"}
        failing_client.upload_part.side_effect = RuntimeError("upload failed")
        clients: list[AsyncMock] = [successful_client, failing_client]

        def mock_zip_stream(**_kwargs: Any) -> nullcontext[io.BytesIO]:
            return nullcontext(io.BytesIO(archive))

        _configure_s3_clients(monkeypatch, clients)
        monkeypatch.setattr(storage, "get_agent_zip_stream", mock_zip_stream)

        await storage.push_agent("demo", Path("/unused"))

        successful_client.complete_multipart_upload.assert_awaited_once_with(
            Bucket="agent-bucket",
            Key="agents/demo.zip",
            UploadId="upload-success",
            MultipartUpload={
                "Parts": [
                    {"ETag": "etag-1", "PartNumber": 1},
                    {"ETag": "etag-2", "PartNumber": 2},
                ]
            },
        )
        successful_client.abort_multipart_upload.assert_not_awaited()

        with pytest.raises(RuntimeError, match="upload failed"):
            await storage.push_agent("demo", Path("/unused"))

        failing_client.abort_multipart_upload.assert_awaited_once_with(
            Bucket="agent-bucket",
            Key="agents/demo.zip",
            UploadId="upload-failure",
        )
        failing_client.complete_multipart_upload.assert_not_awaited()
