from asyncio import Semaphore
from collections.abc import AsyncIterator, Callable, Mapping
from pathlib import Path

import pytest
from benchmark_service.sandbox import (
    ExecResult,
    ImageSandboxCreateRequest,
    Sandbox,
    SandboxCreateRequest,
    SandboxFile,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SnapshotSandboxCreateRequest,
)
from benchmark_service.schemas import Resources

from tracker.database.models import AgentCausedExitReason
from tracker.exceptions import SandboxError
from tracker.sandbox import create_sandbox, stream_command_output


class FakeSandbox(Sandbox):
    def __init__(self, provider: SandboxProvider, result: ExecResult | None = None) -> None:
        super().__init__(provider=provider, id="sandbox-id", name="sandbox-name")
        self.result = result or ExecResult(exit_code=0, stdout="ok")

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        if on_stdout and self.result.stdout:
            on_stdout(self.result.stdout)
        if on_stderr and self.result.stderr:
            on_stderr(self.result.stderr)
        return self.result

    async def upload_file(self, file: SandboxFile) -> None:
        raise AssertionError(file)

    async def upload_local_file(self, local_path: Path, remote_path: str) -> None:
        raise AssertionError((local_path, remote_path))

    async def upload_files(self, files: list[SandboxFile]) -> None:
        raise AssertionError(files)

    async def download_file(self, remote_path: str) -> bytes:
        raise AssertionError(remote_path)

    async def wait_until_ready(self) -> None:
        return None

    async def wait_until_stopped(self) -> None:
        return None


class FakeProvider(SandboxProvider):
    def __init__(self, result: ExecResult | None = None) -> None:
        self.sandbox = FakeSandbox(self, result)
        self.create_request: SandboxCreateRequest | None = None
        self.deleted: Sandbox | None = None

    @classmethod
    async def from_headers(cls, headers: Mapping[str, str]) -> "FakeProvider":
        raise AssertionError(headers)

    async def get_sandbox(self, id: str) -> FakeSandbox:
        raise SandboxNotFoundError()

    async def create_sandbox(self, request: SandboxCreateRequest) -> FakeSandbox:
        self.create_request = request
        return self.sandbox

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        self.deleted = sandbox

    def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[Sandbox]:
        raise AssertionError(query)


async def test_create_sandbox_uses_image_request() -> None:
    provider = FakeProvider()

    async with create_sandbox(
        provider=provider,
        sandbox_name="task-alias",
        image="python:3.12",
        resources=Resources(vcpu=2, memory=4, disk=8),
        creation_semaphore=Semaphore(1),
        labels={"Task": "task-id"},
        env_vars={"A": "B"},
    ) as sandbox:
        assert sandbox is provider.sandbox

    assert isinstance(provider.create_request, ImageSandboxCreateRequest)
    assert provider.create_request.image == "python:3.12"
    assert provider.create_request.name == "task-alias"
    assert provider.create_request.resources
    assert provider.create_request.resources.cpu == 2
    assert provider.deleted is provider.sandbox


async def test_create_sandbox_uses_snapshot_request() -> None:
    provider = FakeProvider()

    async with create_sandbox(
        provider=provider,
        sandbox_name="task-alias",
        image="snapshot:vcb-ready",
        resources=Resources(vcpu=2, memory=4, disk=8),
        creation_semaphore=Semaphore(1),
        labels={},
        env_vars={},
    ):
        pass

    assert isinstance(provider.create_request, SnapshotSandboxCreateRequest)
    assert provider.create_request.snapshot == "vcb-ready"


async def test_stream_command_output_maps_agent_exit_reasons() -> None:
    assert await stream_command_output(FakeProvider(ExecResult(exit_code=0)).sandbox, "cmd", lambda _data: None) is None
    assert (
        await stream_command_output(FakeProvider(ExecResult(exit_code=124)).sandbox, "cmd", lambda _data: None)
        == AgentCausedExitReason.TIMEOUT
    )
    assert (
        await stream_command_output(FakeProvider(ExecResult(exit_code=137)).sandbox, "cmd", lambda _data: None)
        == AgentCausedExitReason.OS_KILLED
    )


async def test_stream_command_output_raises_on_unknown_failure() -> None:
    with pytest.raises(SandboxError, match="exit code: 128"):
        await stream_command_output(
            FakeProvider(ExecResult(exit_code=128, stderr="control server refused")).sandbox,
            "cmd",
            lambda _data: None,
        )
