"""Integration tests for compose-backed sandbox operations.

Run: uv run pytest tests/integration/live/sandbox/test_compose.py
"""

import asyncio
import io
import tarfile
import uuid
from collections.abc import AsyncGenerator, AsyncIterable
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import ComposeSource, CreditedGeneration, ImageSource, Sandbox, SandboxProvider
from benchmark_service.schemas import RetrieveTaskResponse

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.s3 import S3ObjectStore
from tracker.database.models import AgentCausedExitReason, AgentContractRequest
from tracker.egress import combine_run_egress_policies
from tracker.external_service_gateway import ExternalServiceDeadlineController
from tracker.generation_plan import GenerationPlan
from tracker.runtime.storage import ObjectStore
from tracker.sandbox import (
    SandboxCreationState,
    apply_egress_policy,
    create_sandbox,
    install_agent_dependencies,
    run_agent,
    runtime_sandbox,
)

_DIND_IMAGE = "docker:28.3.3-dind"
_COMPOSE_SERVICE_IMAGE = "alpine:3.20"
_COMPOSE_RUN_ID = "compose-run-id"
_COMPOSE_TASK_ID = "compose-task-id"
_COMPOSE_SECRET = "compose-secret"
_COMPOSE_IDENTITY = '{"agent_name":"compose-agent","benchmark_name":"compose-benchmark"}'


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

    dockerd_logs = await sandbox.exec("tail -n 200 /var/log/dockerd.log", timeout=10)
    raise AssertionError(f"Docker daemon did not become ready inside Daytona sandbox:\n{dockerd_logs.stdout}")


async def _wait_for_compose_service(sandbox: Sandbox, compose_command: str) -> None:
    for service in ("main", "sidecar"):
        for _ in range(60):
            result = await sandbox.exec(f"{compose_command} exec -T {service} sh -lc 'echo ready'", timeout=10)
            if result.exit_code == 0 and result.stdout.strip() == "ready":
                break
            await asyncio.sleep(1)
        else:
            raise AssertionError(f"Compose {service} service did not become ready inside Daytona sandbox")


async def _start_compose_runtime(sandbox: Sandbox, source: ComposeSource, compose_file: str) -> None:
    await _exec_required(sandbox, "mkdir -p /bundle", timeout=30)
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
                "    volumes:",
                "      - /bundle:/bundle",
                "  sidecar:",
                f"    image: {_COMPOSE_SERVICE_IMAGE}",
                "    command: sh -lc 'while true; do sleep 3600; done'",
            ]
        ).encode(),
    )
    await _exec_required(sandbox, f"docker pull {_COMPOSE_SERVICE_IMAGE}", timeout=120)
    await _exec_required(sandbox, f"{source.compose_command} up -d", timeout=120)
    await _wait_for_compose_service(sandbox, source.compose_command)


def _unused_object_store() -> S3ObjectStore:
    return S3ObjectStore(
        AWSRuntime(
            resources=AWSResources(region="us-east-1", s3_bucket="unused", log_group="unused", log_retention_days=1),
            clients=AWSClientProvider("us-east-1"),
        )
    )


def _archive_store(uploads: list[tuple[str, bytes]]) -> Mock:
    store = Mock(spec=ObjectStore)

    async def put_stream(key: str, chunks: AsyncIterable[bytes], **_kwargs: object) -> int:
        content = b"".join([chunk async for chunk in chunks])
        uploads.append((key, content))
        return len(content)

    store.put_stream = AsyncMock(side_effect=put_stream)
    return store


def _archived_file(uploads: list[tuple[str, bytes]], key: str, path: str) -> bytes:
    assert len(uploads) == 1
    assert uploads[0][0] == key
    with tarfile.open(fileobj=io.BytesIO(uploads[0][1]), mode="r:gz") as archive:
        member = next(member for member in archive if member.name.endswith(path))
        extracted = archive.extractfile(member)
        assert extracted is not None
        return extracted.read()


@pytest.fixture
async def compose_sandbox(
    sandbox_provider: SandboxProvider,
    random_sandbox_name: str,
    creation_semaphore: asyncio.Semaphore,
) -> AsyncGenerator[tuple[Sandbox, Sandbox, RetrieveTaskResponse], None]:
    project_name = f"tracker-compose-{uuid.uuid4().hex[:8]}"
    compose_file = f"/tmp/{project_name}.compose.yaml"
    task_data = _compose_task_response(f"docker compose -p {project_name} -f {compose_file}")
    env_vars = {
        "RUN_ID": _COMPOSE_RUN_ID,
        "TASK_ID": _COMPOSE_TASK_ID,
        "IDENTITY": _COMPOSE_IDENTITY,
        "TRACKER_COMPOSE_SECRET": _COMPOSE_SECRET,
    }

    assert isinstance(task_data.source, ComposeSource)
    assert isinstance(task_data.source.outer, ImageSource)

    runtime_started = False
    async with create_sandbox(
        sandbox_provider,
        random_sandbox_name,
        task_data.source,
        task_data.resources,
        creation_semaphore,
        creation_state=SandboxCreationState(cleanup_pending=False),
        env_vars=env_vars,
    ) as outer_sandbox:
        try:
            await _start_compose_runtime(outer_sandbox, task_data.source, compose_file)
            runtime_started = True
            yield runtime_sandbox(outer_sandbox, task_data.source), outer_sandbox, task_data
        finally:
            if runtime_started:
                await outer_sandbox.exec(f"{task_data.source.compose_command} down -v --remove-orphans", timeout=120)
                containers = await outer_sandbox.exec(
                    f"docker ps -a --filter label=com.docker.compose.project={project_name} -q",
                    timeout=30,
                )
                assert containers.stdout.strip() == ""


async def test_compose_sandbox_methods_use_daytona_outer_from_retrieve_task(
    compose_sandbox: tuple[Sandbox, Sandbox, RetrieveTaskResponse],
) -> None:
    """Compose sandbox methods should work through the Daytona-created outer sandbox.

    Test cases:
    - The benchmark-service retrieve-task response creates the Daytona outer DinD sandbox.
    - Exec, streaming command, upload, download, file deletion, and temporary file cleanup work through compose.
    - Runtime env vars, agent install, agent run, and a benchmark-style evaluation command run inside `main`.
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

    contract_name = "compose_contract"
    await outer_sandbox.exec(f"mkdir -p /bundle/{contract_name}", timeout=30)
    await outer_sandbox.upload_file(
        f"/bundle/{contract_name}/setup.sh",
        (
            "#!/bin/sh\n"
            'printf \'%s:%s:%s\' "$RUN_ID" "$TRACKER_COMPOSE_SECRET" "$(pwd)" '
            "> /workspace/install-proof.txt\n"
        ).encode(),
    )

    logs: list[str] = []
    contract = AgentContractRequest(
        name=contract_name,
        install_cmd="sh setup.sh",
        egress_allowlist=["example.com"],
        run_cmd=(
            "printf 'agent-run' > /workspace/agent-run.txt && "
            f"test \"$RUN_ID\" = '{_COMPOSE_RUN_ID}' && "
            f"test \"$TASK_ID\" = '{_COMPOSE_TASK_ID}' && "
            f"test \"$TRACKER_COMPOSE_SECRET\" = '{_COMPOSE_SECRET}' && "
            'case "$IDENTITY" in *compose-agent*) true;; *) exit 1;; esac'
        ),
    )

    agent_sandbox = runtime_sandbox(outer_sandbox, task_data.source)
    await apply_egress_policy(agent_sandbox, contract.install_egress_policy)
    await install_agent_dependencies(agent_sandbox, contract, logs.append)
    await apply_egress_policy(
        agent_sandbox,
        combine_run_egress_policies(task_data.egress.run, contract.egress_allowlist),
    )
    plan = GenerationPlan.from_task(task_data)
    exit_reason, agent_run_time = await run_agent(
        agent_sandbox,
        contract,
        task_data.problem_path,
        _COMPOSE_TASK_ID,
        logs.append,
        task_data.cwd,
        _unused_object_store(),
        plan,
        deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=plan.allowance_seconds),
    )

    assert exit_reason is None
    assert agent_run_time >= 0
    assert any("Installing dependencies" in message for message in logs)

    install_proof = await sandbox.exec("cat /workspace/install-proof.txt", timeout=30)
    assert install_proof.stdout == f"{_COMPOSE_RUN_ID}:{_COMPOSE_SECRET}:/bundle/{contract_name}"

    run_proof = await sandbox.exec("cat /workspace/agent-run.txt", timeout=30)
    assert run_proof.stdout == "agent-run"

    evaluation = await sandbox.exec(
        "test -s /workspace/agent-run.txt && printf '{\"score\":1}' > /workspace/evaluation.json",
        timeout=30,
    )
    assert evaluation.exit_code == 0
    assert await sandbox.download_file("/workspace/evaluation.json") == b'{"score":1}'


async def test_controlled_compose_agent_output_and_evaluation(
    compose_sandbox: tuple[Sandbox, Sandbox, RetrieveTaskResponse],
) -> None:
    sandbox, outer_sandbox, task_data = compose_sandbox
    task_data = task_data.model_copy(
        update={"credited_generation": CreditedGeneration(allowance_seconds=30.0, stage_protocol=None)}
    )
    logs: list[str] = []
    uploads: list[tuple[str, bytes]] = []
    contract = AgentContractRequest(
        name="compose_controlled_normal",
        run_cmd=(
            f"test \"$RUN_ID\" = '{_COMPOSE_RUN_ID}' && "
            f"test \"$TASK_ID\" = '{_COMPOSE_TASK_ID}' && "
            f"test \"$TRACKER_COMPOSE_SECRET\" = '{_COMPOSE_SECRET}' && "
            'case "$IDENTITY" in *compose-agent*) true;; *) exit 1;; esac && '
            "printf 'controlled-output\\n' && "
            "printf 'controlled-result' > /workspace/controlled-result.txt"
        ),
        final_output="/workspace/controlled-result.txt",
    )

    plan = GenerationPlan.from_task(task_data)
    exit_reason, elapsed = await run_agent(
        sandbox,
        contract,
        task_data.problem_path,
        _COMPOSE_TASK_ID,
        logs.append,
        task_data.cwd,
        _archive_store(uploads),
        plan,
        deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=plan.allowance_seconds),
        agent_output_s3_key="controlled/normal",
    )

    assert exit_reason is None
    assert elapsed >= 0
    assert any("controlled-output" in message for message in logs)
    assert await sandbox.download_file("/workspace/controlled-result.txt") == b"controlled-result"
    assert _archived_file(uploads, "controlled/normal", "controlled-result.txt") == b"controlled-result"

    evaluation = await sandbox.exec(
        "test -s /workspace/controlled-result.txt && printf '{\"score\":1}' > /workspace/controlled-evaluation.json",
        timeout=30,
    )
    assert evaluation.exit_code == 0
    assert await sandbox.download_file("/workspace/controlled-evaluation.json") == b'{"score":1}'
    assert isinstance(task_data.source, ComposeSource)
    sidecar = await outer_sandbox.exec(
        f"{task_data.source.compose_command} exec -T sidecar sh -lc 'echo sidecar-alive'", timeout=30
    )
    assert sidecar.exit_code == 0
    assert sidecar.stdout.strip() == "sidecar-alive"


async def test_controlled_compose_deadline_stops_only_agent_group(
    compose_sandbox: tuple[Sandbox, Sandbox, RetrieveTaskResponse],
) -> None:
    sandbox, outer_sandbox, task_data = compose_sandbox
    assert isinstance(task_data.source, ComposeSource)
    compose_command = task_data.source.compose_command
    service_ids: dict[str, str] = {}
    for service in ("main", "sidecar"):
        result = await outer_sandbox.exec(f"{compose_command} ps -q {service}", timeout=30)
        assert result.exit_code == 0 and result.stdout.strip()
        service_ids[service] = result.stdout.strip()

    task_data = task_data.model_copy(
        update={"credited_generation": CreditedGeneration(allowance_seconds=45.0, stage_protocol=None)}
    )
    uploads: list[tuple[str, bytes]] = []
    contract = AgentContractRequest(
        name="compose_controlled_deadline",
        run_cmd="sh -c 'echo $$ > /workspace/controlled-agent.pid; echo deadline-output; exec sleep 3600'",
        final_output="/workspace/controlled-agent.pid",
    )
    logs: list[str] = []
    started = asyncio.Event()

    def record_output(message: str) -> None:
        logs.append(message)
        if "deadline-output" in message:
            started.set()

    plan = GenerationPlan.from_task(task_data)
    agent = asyncio.create_task(
        run_agent(
            sandbox,
            contract,
            task_data.problem_path,
            _COMPOSE_TASK_ID,
            record_output,
            task_data.cwd,
            _archive_store(uploads),
            plan,
            deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=plan.allowance_seconds),
            agent_output_s3_key="controlled/deadline",
        )
    )
    visible_while_running = False
    try:
        await asyncio.wait_for(started.wait(), timeout=25)
        visible = await sandbox.exec(
            'test -s /workspace/controlled-agent.pid && kill -0 "$(cat /workspace/controlled-agent.pid)"',
            timeout=10,
        )
        visible_while_running = visible.exit_code == 0
    finally:
        exit_reason, elapsed = await asyncio.wait_for(agent, timeout=80)

    assert visible_while_running
    assert exit_reason == AgentCausedExitReason.TIMEOUT
    assert elapsed > 0
    assert any("deadline-output" in message for message in logs)
    stopped = await sandbox.exec(
        "test -s /workspace/controlled-agent.pid || exit 1; "
        "pid=$(cat /workspace/controlled-agent.pid); "
        'case "$(cat "/proc/$pid/stat" 2>/dev/null)" in *") Z "*|*") X "*|"") exit 0;; *) exit 1;; esac',
        timeout=30,
    )
    assert stopped.exit_code == 0
    assert _archived_file(uploads, "controlled/deadline", "controlled-agent.pid").strip().isdigit()

    for service, original_id in service_ids.items():
        current = await outer_sandbox.exec(f"{compose_command} ps -q {service}", timeout=30)
        assert current.exit_code == 0
        assert current.stdout.strip() == original_id
    sidecar = await outer_sandbox.exec(f"{compose_command} exec -T sidecar sh -lc 'echo sidecar-alive'", timeout=30)
    assert sidecar.exit_code == 0
    assert sidecar.stdout.strip() == "sidecar-alive"
    evaluation = await sandbox.exec(
        "test -s /workspace/controlled-agent.pid && printf '{\"score\":0}' > /workspace/deadline-evaluation.json",
        timeout=30,
    )
    assert evaluation.exit_code == 0
    assert await sandbox.download_file("/workspace/deadline-evaluation.json") == b'{"score":0}'
