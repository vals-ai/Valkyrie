import math
from collections.abc import AsyncIterator, Mapping
from typing import Any, Self

import pytest
from benchmark_service.sandbox import Sandbox, SandboxCreateRequest, SandboxFile, SandboxProvider, SandboxQuery
from sqlmodel import select

from tracker.database.models import Benchmark, Task, TaskStatus
from tracker.types import AWSCredentials
from tracker.utils import _SANDBOX_DELETE_BATCH_SIZE, force_stop_sandboxes


class FakeSandbox(Sandbox):
    def __init__(self, provider: SandboxProvider, sandbox_id: str, name: str) -> None:
        super().__init__(provider=provider, id=sandbox_id, name=name)

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout=None,
        on_stderr=None,
    ):
        raise NotImplementedError

    async def upload_file(self, file: SandboxFile) -> None:
        raise NotImplementedError

    async def upload_files(self, files: list[SandboxFile]) -> None:
        raise NotImplementedError

    async def download_file(self, remote_path: str) -> bytes:
        raise NotImplementedError

    async def create_folder(self, remote_path: str) -> None:
        raise NotImplementedError

    async def wait_until_ready(self) -> None:
        raise NotImplementedError


class FakeProvider(SandboxProvider):
    def __init__(self, sandboxes: list[FakeSandbox]) -> None:
        self._sandboxes = sandboxes
        self.list_calls = 0
        self.deleted_ids: list[str] = []

    @classmethod
    async def from_headers(cls, headers: Mapping[str, str], **kwargs: Any) -> Self:
        raise NotImplementedError

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        raise NotImplementedError

    async def get_sandbox(self, id: str) -> Sandbox:
        raise NotImplementedError

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        self.deleted_ids.append(sandbox.id)
        self._sandboxes = [item for item in self._sandboxes if item.id != sandbox.id]

    async def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[Sandbox]:
        self.list_calls += 1
        limit = query.limit if query else len(self._sandboxes)
        for sandbox in self._sandboxes[:limit]:
            yield sandbox


class FakeBenchmarkService:
    def __init__(self, provider: SandboxProvider) -> None:
        self._provider = provider
        self.closed = False

    async def get_sandbox_provider(self) -> SandboxProvider:
        return self._provider

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


@pytest.mark.asyncio
async def test_force_stop_sandboxes_deletes_in_batches(
    example_benchmark_object: Benchmark,
    database_session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_row = example_benchmark_object
    database_session.add(benchmark_row)
    database_session.commit()

    task_rows = [
        Task(benchmark=benchmark_row.id, task_id=f"task-{index}", status=TaskStatus.IN_PROGRESS)
        for index in range(3)
    ]
    database_session.add_all(task_rows)
    database_session.commit()

    provider = FakeProvider([])
    sandboxes = [FakeSandbox(provider, f"sandbox-{index}", f"sandbox-{index}") for index in range(125)]
    provider._sandboxes = sandboxes

    fake_benchmark_service = FakeBenchmarkService(provider)

    def _mock_benchmark_service(
        self: Benchmark, daytona_secret_name: str, aws: AWSCredentials
    ) -> FakeBenchmarkService:
        return fake_benchmark_service

    monkeypatch.setattr(Benchmark, "benchmark_service", _mock_benchmark_service)

    await force_stop_sandboxes(
        benchmark_row,
        database_session,
        daytona_secret_name="test-secret",
        aws=AWSCredentials(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_default_region="us-east-1",
        ),
    )

    assert len(provider.deleted_ids) == 125
    assert provider.list_calls == math.ceil(len(sandboxes) / _SANDBOX_DELETE_BATCH_SIZE) + 1
    assert provider._sandboxes == []
    assert fake_benchmark_service.closed is True

    updated_tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
    assert all(task.status == TaskStatus.STOPPED for task in updated_tasks)
