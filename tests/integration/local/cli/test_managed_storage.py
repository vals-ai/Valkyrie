"""Integration tests for keyless CLI storage through the tracker app.

Run: uv run pytest tests/integration/local/cli/test_managed_storage.py

Exercises agent storage, run-scoped output downloads, and contract parsing with
the real CLI and FastAPI routes while replacing only the external S3 boundary.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
import yaml
from click.testing import CliRunner
from fastapi import FastAPI
from sqlmodel import Session

import tracker.api.agents as agents_api
import tracker.api.benchmark_storage as benchmark_storage_api
import tracker.config as tracker_config
from tracker.agent.schemas import AgentConfig
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark
from valkyrie.cli import remote_storage, s3_config
from valkyrie.cli.agent.storage import get_contract_from_s3
from valkyrie.cli.main import cli
from valkyrie.cli.runtime_config import config_location

_S3_URL = "https://bucket.s3.test"


class MockManagedStorage:
    """Store objects transferred over presigned URLs in memory."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def handle_transfer(self, request: httpx.Request) -> httpx.Response:
        key = request.url.path.lstrip("/")
        if request.method == "PUT":
            self.objects[key] = request.read()
            return httpx.Response(200)
        if key not in self.objects:
            return httpx.Response(404)

        return httpx.Response(200, content=self.objects[key])


@pytest.fixture
def managed_storage(
    local_tracker_app: FastAPI,
    test_org_id: UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> MockManagedStorage:
    """Route keyless CLI storage through the real app and an in-memory S3 boundary."""
    config_location().write_text(
        yaml.safe_dump(
            {
                "api_key": "test-api-key",
                "sandbox_providers": {"daytona": "test-provider-secret"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(test_org_id))
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_REGION", "us-east-1")
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_S3_BUCKET", "test-bucket")
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_LOG_GROUP", "test-log-group")
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "1")

    storage = MockManagedStorage()
    real_async_client = httpx.AsyncClient

    def async_client(**kwargs: Any) -> httpx.AsyncClient:
        if kwargs.get("base_url"):
            transport = httpx.ASGITransport(app=local_tracker_app)
        else:
            transport = httpx.MockTransport(storage.handle_transfer)

        return real_async_client(transport=transport, **kwargs)

    monkeypatch.setattr(remote_storage.httpx, "AsyncClient", async_client)

    async def list_agents(runtime: AWSRuntime) -> list[tuple[str, datetime | None]]:
        assert runtime.resources.s3_bucket == "test-bucket"
        return [
            (Path(key).stem, datetime(2026, 1, 2, tzinfo=timezone.utc))
            for key in sorted(storage.objects)
            if key.startswith("agents/")
        ]

    async def object_exists(s3_key: str, runtime: AWSRuntime) -> bool:
        assert runtime.resources.s3_bucket == "test-bucket"
        return s3_key in storage.objects

    async def presigned_url(
        s3_key: str,
        runtime: AWSRuntime,
        expiration: int = 86_400,
        *,
        client_method: str = "get_object",
    ) -> str:
        assert runtime.resources.s3_bucket == "test-bucket"
        assert expiration == agents_api.PRESIGNED_URL_EXPIRES_SECONDS
        assert client_method in {"get_object", "put_object"}
        return f"{_S3_URL}/{s3_key}"

    async def delete_object(s3_key: str, runtime: AWSRuntime) -> None:
        assert runtime.resources.s3_bucket == "test-bucket"
        del storage.objects[s3_key]

    async def copy_object(source_key: str, destination_key: str, runtime: AWSRuntime) -> None:
        assert runtime.resources.s3_bucket == "test-bucket"
        storage.objects[destination_key] = storage.objects[source_key]

    monkeypatch.setattr(agents_api, "list_agents", list_agents)
    monkeypatch.setattr(agents_api, "s3_object_exists", object_exists)
    monkeypatch.setattr(agents_api, "create_presigned_url", presigned_url)
    monkeypatch.setattr(agents_api, "delete_from_s3", delete_object)
    monkeypatch.setattr(benchmark_storage_api, "s3_object_exists", object_exists)
    monkeypatch.setattr(benchmark_storage_api, "copy_s3_object", copy_object)

    def forbid_local_aws_runtime() -> AWSRuntime:
        raise AssertionError("keyless CLI storage must not construct a local AWS runtime")

    monkeypatch.setattr(s3_config, "aws_runtime", forbid_local_aws_runtime)

    return storage


def test_keyless_resume_updates_frozen_agent(
    cli_runner: CliRunner,
    managed_storage: MockManagedStorage,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    """Resume --update-agent copies the current agent through Tracker without local AWS."""
    benchmark, _finished = seeded_runs
    benchmark.aws_managed = True
    database_session.add(benchmark)
    database_session.commit()
    source_key = "agents/cli-agent.zip"
    destination_key = f"benchmarks/{benchmark.id}/cli-agent.zip"
    managed_storage.objects[source_key] = b"current-agent"

    result = cli_runner.invoke(cli, ["run", "resume", str(benchmark.id), "--update-agent"])

    assert result.exit_code == 0, result.output
    assert managed_storage.objects[destination_key] == b"current-agent"


def test_keyless_agent_storage_crosses_cli_and_tracker(
    cli_runner: CliRunner,
    managed_storage: MockManagedStorage,
    tmp_path: Path,
) -> None:
    """Agent push, contract read, list, download, and remove use Tracker without local AWS."""
    agent_path = tmp_path / "demo"
    agent_path.mkdir()
    (agent_path / "contract.yaml").write_text(
        "name: demo\ninstall_cmd: install\nrun_cmd: run {problem_statement_path}\n",
        encoding="utf-8",
    )

    push_result = cli_runner.invoke(cli, ["agent", "push", str(agent_path)])
    assert push_result.exit_code == 0, push_result.output
    assert "agents/demo.zip" in managed_storage.objects

    contract = asyncio.run(get_contract_from_s3("demo", AgentConfig()))
    assert contract.name == "demo"
    assert contract.install_cmd == "install"
    assert contract.run_cmd == "run {problem_statement_path}"

    list_result = cli_runner.invoke(cli, ["agent", "list"])
    assert list_result.exit_code == 0, list_result.output
    assert "demo" in list_result.output

    download_path = tmp_path / "downloaded"
    download_result = cli_runner.invoke(
        cli,
        ["agent", "download", "demo", "--output-dir", str(download_path)],
    )
    assert download_result.exit_code == 0, download_result.output
    assert (download_path / "demo" / "contract.yaml").exists()

    remove_result = cli_runner.invoke(cli, ["agent", "remove", "demo"], input="y\n")
    assert remove_result.exit_code == 0, remove_result.output
    assert managed_storage.objects == {}


def test_keyless_run_output_uses_managed_run_authority(
    cli_runner: CliRunner,
    managed_storage: MockManagedStorage,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run output crosses the real managed-run lookup before downloading through a presigned URL."""
    benchmark, _finished = seeded_runs
    benchmark.aws_managed = True
    database_session.add(benchmark)
    database_session.commit()
    key = f"benchmarks/{benchmark.id}/task-a/output.json"
    managed_storage.objects[key] = b"{}"

    async def list_objects(prefix: str, runtime: AWSRuntime) -> AsyncIterator[str]:
        assert prefix == f"benchmarks/{benchmark.id}/task-a/"
        assert runtime.resources.s3_bucket == "test-bucket"
        yield key

    async def presigned_urls(
        s3_keys: list[str],
        runtime: AWSRuntime,
        expiration: int = 86_400,
        *,
        client_method: str = "get_object",
    ) -> list[str]:
        assert s3_keys == [key]
        assert runtime.resources.s3_bucket == "test-bucket"
        assert expiration == benchmark_storage_api.OUTPUT_URL_EXPIRES_SECONDS
        assert client_method == "get_object"
        return [f"{_S3_URL}/{key}"]

    monkeypatch.setattr(benchmark_storage_api, "list_s3_objects", list_objects)
    monkeypatch.setattr(benchmark_storage_api, "create_presigned_urls", presigned_urls)
    output_path = tmp_path / "outputs"

    result = cli_runner.invoke(
        cli,
        ["run", "output", str(benchmark.id), "task-a", "--output-dir", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert (output_path / "output.json").read_bytes() == b"{}"
