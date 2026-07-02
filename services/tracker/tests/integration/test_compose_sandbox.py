"""Integration tests for compose-backed sandbox operations."""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import pytest
from benchmark_service import ComposeSource, ImageSource, Sandbox, SandboxProvider
from benchmark_service.schemas import RetrieveTaskResponse

from tracker.sandbox import create_sandbox, runtime_sandbox

_DIND_IMAGE = "docker:28.3.3-dind"
_COMPOSE_SERVICE_IMAGE = "alpine:3.20"


def _compose_task_response(compose_command: str) -> RetrieveTaskResponse:
    return RetrieveTaskResponse.model_validate(
        {
            "source": {
                "type": "compose",
                "outer": {"type": "image", "image": _DIND_IMAGE},
                "compose_command": compose_command,
            },
            "problem_path": "/workspace/problem.txt",
            "cwd": "/workspace",
            "resources": {"vcpu": 1, "memory": 2, "disk": 15},
        }
    )


async def _exec_required(sandbox: Sandbox, command: str, *, timeout: float = 60) -> None:
    result = await sandbox.exec(command, timeout=timeout)
    assert result.exit_code == 0, result.output


async def _wait_for_docker(sandbox: Sandbox) -> None:
    for _ in range(60):
        result = await sandbox.exec("docker info", timeout=10)
        if result.exit_code == 0:
            return
        await asyncio.sleep(1)

    raise AssertionError("Docker daemon did not become ready inside Daytona sandbox")


async def _wait_for_compose_service(sandbox: Sandbox, compose_command: str) -> None:
    for _ in range(60):
        result = await sandbox.exec(f"{compose_command} exec -T main sh -lc 'echo ready'", timeout=10)
        if result.exit_code == 0 and result.stdout.strip() == "ready":
            return
        await asyncio.sleep(1)

    raise AssertionError("Compose main service did not become ready inside Daytona sandbox")


async def _start_compose_runtime(sandbox: Sandbox, source: ComposeSource, compose_file: str) -> None:
    await _exec_required(sandbox, "dockerd-entrypoint.sh dockerd > /var/log/dockerd.log 2>&1 &", timeout=10)
    await _wait_for_docker(sandbox)
    await sandbox.upload_file(
        compose_file,
        "\n".join(
            [
                "services:",
                "  main:",
                f"    image: {_COMPOSE_SERVICE_IMAGE}",
                "    command: sh -lc 'while true; do sleep 3600; done'",
                "    working_dir: /workspace",
            ]
        ).encode(),
    )
    await _exec_required(sandbox, f"docker pull {_COMPOSE_SERVICE_IMAGE}", timeout=120)
    await _exec_required(sandbox, f"{source.compose_command} up -d", timeout=120)
    await _wait_for_compose_service(sandbox, source.compose_command)


@pytest.fixture
async def compose_sandbox(
    sandbox_provider: SandboxProvider,
    random_sandbox_name: str,
    creation_semaphore: asyncio.Semaphore,
) -> AsyncGenerator[tuple[Sandbox, Sandbox, RetrieveTaskResponse], None]:
    project_name = f"tracker-compose-{uuid.uuid4().hex[:8]}"
    compose_file = f"/tmp/{project_name}.compose.yaml"
    task_data = _compose_task_response(f"docker compose -p {project_name} -f {compose_file}")

    assert isinstance(task_data.source, ComposeSource)
    assert isinstance(task_data.source.outer, ImageSource)

    async with create_sandbox(
        sandbox_provider,
        random_sandbox_name,
        task_data.source,
        task_data.resources,
        creation_semaphore,
    ) as outer_sandbox:
        try:
            await _start_compose_runtime(outer_sandbox, task_data.source, compose_file)
            yield runtime_sandbox(outer_sandbox, task_data.source), outer_sandbox, task_data
        finally:
            await outer_sandbox.exec(f"{task_data.source.compose_command} down -v --remove-orphans", timeout=120)


async def test_compose_sandbox_methods_use_daytona_outer_from_retrieve_task(
    compose_sandbox: tuple[Sandbox, Sandbox, RetrieveTaskResponse],
) -> None:
    """Compose sandbox methods should work through the Daytona-created outer sandbox.

    Test cases:
    - The benchmark-service retrieve-task response creates the Daytona outer DinD sandbox.
    - Exec, streaming command, upload, download, file deletion, and temporary file cleanup work through compose.
    """
    sandbox, outer_sandbox, task_data = compose_sandbox

    assert isinstance(task_data.source, ComposeSource)
    assert sandbox.id == outer_sandbox.id
    assert sandbox.name == outer_sandbox.name
    assert sandbox.state == outer_sandbox.state

    exec_result = await sandbox.exec(
        (
            "mkdir -p /workspace/subdir && "
            "printf 'from-exec' > /workspace/subdir/value.txt && "
            "cat /workspace/subdir/value.txt"
        ),
        timeout=30,
    )
    assert exec_result.exit_code == 0
    assert exec_result.stdout == "from-exec"

    streamed = [chunk async for chunk in sandbox.command("printf 'stream-one\\nstream-two\\n'", timeout=30)]
    streamed_output = "".join(streamed)
    assert "stream-one" in streamed_output
    assert "stream-two" in streamed_output

    await sandbox.upload_file("/workspace/copied/note.txt", b"copied through compose")
    uploaded = await sandbox.exec("cat /workspace/copied/note.txt", timeout=30)
    assert uploaded.stdout == "copied through compose"
    assert await sandbox.download_file("/workspace/copied/note.txt") == b"copied through compose"

    deleted = await sandbox.exec("rm /workspace/copied/note.txt && test ! -e /workspace/copied/note.txt")
    assert deleted.exit_code == 0

    temp_files = await outer_sandbox.exec(
        "find /var/tmp -maxdepth 1 \\( -name 'compose-upload-*' -o -name 'compose-download-*' \\) -print",
        timeout=30,
    )
    assert temp_files.stdout.strip() == ""
