"""Exercise a built PEX through real local Tracker/Redis/Postgres and live sandboxes.

Run: TEST_EXECUTOR_PEX=/path/executor.pex uv run pytest tests/integration/live/orchestration/test_executor_continuity.py -s
Uses the existing live AWS/provider fixtures. All created agents, run artifacts, logs, and sandboxes are cleaned up.
"""

import asyncio
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import zipfile
from collections.abc import AsyncGenerator, Generator
from contextlib import AsyncExitStack, closing
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Protocol, cast
from uuid import UUID, uuid4

import httpx
import pytest
from benchmark_service import Sandbox, SandboxProvider, SandboxProviderConfig, SandboxQuery
from botocore.exceptions import ClientError
from daytona import SandboxState
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, select, create_engine
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer

from tests.integration.seed_agent_artifacts import create_s3_client
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.database.models import (
    DEFAULT_ORG_NAME,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchAccess,
    ExecutorDispatchStatus,
    ExecutorRelease,
    FinalEvaluation,
    Org,
    Task,
)
from tracker.executor.maintenance_control import begin_maintenance, finish_maintenance
from tracker.executor.release_control import register_release, promote_release
from tracker.runtime.artifacts import agent_bundle_key, benchmark_prefix
from tracker.types import HarnessConfig


@dataclass(frozen=True)
class ContinuityRuntime:
    engine: Engine
    database_url: str
    redis_url: str
    artifact: Path
    digest: str


@dataclass(frozen=True)
class RunEvidence:
    task_id: UUID
    started_at: datetime
    status: BenchmarkStatus
    dispatch_status: ExecutorDispatchStatus
    claimant_id: UUID | None
    result_count: int
    score: float | None


class LogCleanupClient(Protocol):
    def delete_log_group(self, *, logGroupName: str) -> object: ...

    def close(self) -> None: ...


@pytest.fixture
def continuity_runtime(harness_config: HarnessConfig) -> Generator[ContinuityRuntime]:
    """Use isolated local databases and a caller-built executor artifact."""
    configured = os.environ.get("TEST_EXECUTOR_PEX")
    if not configured:
        pytest.fail("TEST_EXECUTOR_PEX must point to the executor PEX built from the checkout under test")
    artifact = Path(configured).resolve(strict=True)
    check = subprocess.run(
        [sys.executable, str(artifact), "--check"], capture_output=True, text=True, check=True, timeout=60
    )
    assert json.loads(check.stdout)["protocol_version"] == "4", "The E2E test requires a protocol-4 executor"
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    with (
        PostgresContainer("postgres:16-alpine") as postgres,
        DockerContainer("redis:7-alpine").with_exposed_ports(6379) as redis,
    ):
        database_url = postgres.get_connection_url()
        engine = create_engine(database_url)
        SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            session.add(Org(name=DEFAULT_ORG_NAME))
            session.add(ExecutorAdmission())
            session.commit()
            release = ExecutorRelease(
                id=f"continuity-{digest[:16]}",
                artifact_uri=f"s3://{harness_config.s3_bucket}/releases/continuity/executor.pex",
                artifact_digest=digest,
                protocol_version="4",
                readiness_verified=True,
            )
            register_release(session, release)
            promote_release(session, release.id)
            session.commit()
        try:
            yield ContinuityRuntime(
                engine,
                database_url,
                f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}",
                artifact,
                digest,
            )
        finally:
            engine.dispose()


def _evidence(engine: Engine, run_id: UUID) -> RunEvidence:
    with Session(engine) as session:
        run = session.get(Benchmark, run_id)
        assert run is not None
        task = session.exec(select(Task).where(Task.benchmark == run_id)).one()
        dispatch = session.exec(select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == run_id)).one()
        access = session.get(ExecutorDispatchAccess, dispatch.id)
        assert access is not None
        results = session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all()
        score = session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == run_id)).first()

        return RunEvidence(
            task.id,
            task.started_at,
            run.status,
            dispatch.status,
            access.claimant_id,
            len(results),
            score.final_score if score else None,
        )


def _maintenance(engine: Engine, *, begin: bool) -> None:
    with Session(engine) as session:
        if begin:
            begin_maintenance(session, target_sha="a" * 40)
        else:
            finish_maintenance(session, target_sha="a" * 40)
        session.commit()


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))

        return int(listener.getsockname()[1])


async def _stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), 15)
    except TimeoutError:
        os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


async def _spawn(
    stack: AsyncExitStack, arguments: list[str], environment: dict[str, str], log_path: Path
) -> asyncio.subprocess.Process:
    log = await asyncio.to_thread(log_path.open, "wb")
    stack.callback(log.close)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        *arguments,
        env=environment,
        stdout=log,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )
    stack.push_async_callback(_stop, process)

    return process


async def _healthy(client: httpx.AsyncClient, url: str, process: asyncio.subprocess.Process) -> None:
    async with asyncio.timeout(45):
        while True:
            assert process.returncode is None, "Local service exited; inspect its E2E log"
            try:
                response = await client.get(f"{url}/health")
                if response.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)


async def _running_sandbox(provider: SandboxProvider, run_id: UUID) -> tuple[Sandbox, str]:
    async with asyncio.timeout(240):
        while True:
            async for sandbox in provider.list_sandboxes(SandboxQuery(labels={"Id": str(run_id)})):
                assert sandbox.state not in (str(SandboxState.ERROR), str(SandboxState.BUILD_FAILED)), (
                    f"Sandbox {sandbox.id} failed to start: {sandbox.state}"
                )
                if sandbox.state != str(SandboxState.STARTED):
                    continue
                result = await sandbox.exec(
                    "test -f /tmp/continuity-agent-pid && cat /tmp/continuity-agent-pid", timeout=10
                )
                if result.exit_code == 0:
                    return sandbox, result.stdout.strip()
            await asyncio.sleep(1)


async def _finished(engine: Engine, run_id: UUID) -> RunEvidence:
    async with asyncio.timeout(180):
        while True:
            evidence = await asyncio.to_thread(_evidence, engine, run_id)
            assert evidence.status not in (BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED), (
                f"Run {run_id} failed; inspect its E2E log"
            )
            if evidence.dispatch_status == ExecutorDispatchStatus.FINISHED:
                return evidence
            await asyncio.sleep(0.5)


async def _executor_pid(host: asyncio.subprocess.Process) -> int:
    process = await asyncio.create_subprocess_exec("pgrep", "-P", str(host.pid), stdout=asyncio.subprocess.PIPE)
    output, _ = await process.communicate()
    children = output.decode().split()
    assert process.returncode == 0 and len(children) == 1, "Expected exactly one packaged executor under the host"

    return int(children[0])


def _verify_artifacts(harness_config: HarnessConfig, run_id: UUID, sandbox_id: str) -> None:
    with closing(create_s3_client(harness_config.aws)) as s3:
        prefix = benchmark_prefix(str(run_id))
        report_object = s3.get_object(Bucket=harness_config.s3_bucket, Key=f"{prefix}executor-continuity.json")
        with report_object["Body"] as body:
            report = json.loads(body.read())
        assert report["final_evaluation"]["final_score"] == 1
        assert report["evaluation_results"]["task-0"]["sandbox_id"] == sandbox_id
        answer_object = s3.get_object(Bucket=harness_config.s3_bucket, Key=f"{prefix}task-0/answer.txt")
        with answer_object["Body"] as body:
            assert body.read() == b"continuity-ok"


@pytest.fixture
async def continuity_storage(harness_config: HarnessConfig) -> AsyncGenerator[tuple[str, list[UUID]], None]:
    """Keep remote writes under fresh test-only agent and run prefixes."""
    agent_name = f"executor-continuity-{uuid4().hex}"
    run_ids: list[UUID] = []
    agent = b"""import os, pathlib, time
pathlib.Path('/tmp/continuity-agent-pid').write_text(str(os.getpid()))
while not pathlib.Path('/tmp/continuity-release').exists():
    time.sleep(0.2)
output = pathlib.Path('/workspace/final_output')
output.mkdir(parents=True, exist_ok=True)
(output / 'answer.txt').write_text('continuity-ok')
"""
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{agent_name}/run.py", agent)
    s3 = create_s3_client(harness_config.aws)
    logs = cast(LogCleanupClient, ExplicitCredentialsAWSClientProvider(harness_config.aws).cloudwatch_logs_client())
    print(json.dumps({"event": "e2e-test-resources", "agent": agent_name, "bucket": harness_config.s3_bucket}))
    try:
        await asyncio.to_thread(
            s3.put_object, Bucket=harness_config.s3_bucket, Key=agent_bundle_key(agent_name), Body=buffer.getvalue()
        )
        yield agent_name, run_ids
    finally:
        for run_id in run_ids:

            def clean_run() -> None:
                for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=harness_config.s3_bucket, Prefix=benchmark_prefix(str(run_id))
                ):
                    for item in page.get("Contents", []):
                        key = item.get("Key")
                        assert key is not None
                        s3.delete_object(Bucket=harness_config.s3_bucket, Key=key)
                try:
                    logs.delete_log_group(logGroupName=f"{harness_config.log_group}/{run_id}")
                except ClientError as error:
                    if error.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                        raise

            await asyncio.to_thread(clean_run)
        await asyncio.to_thread(s3.delete_object, Bucket=harness_config.s3_bucket, Key=agent_bundle_key(agent_name))
        s3.close()
        logs.close()


async def test_executor_survives_tracker_restart_and_host_drain(
    continuity_runtime: ContinuityRuntime,
    continuity_storage: tuple[str, list[UUID]],
    harness_config: HarnessConfig,
    sandbox_provider_config: SandboxProviderConfig,
    tmp_path: Path,
) -> None:
    """Prove before/after deployment behavior with real processes, requests, and sandbox execution.

    Test cases:
    - The old deployment-maintenance operation stops a live attempt.
    - Restarting Tracker preserves the original PEX, sandbox, claimant, and attempt.
    - A draining host finishes its old run while its replacement completes a new run.
    - Executors have no usable SQL connection; each successful run produces one result.
    """
    runtime = continuity_runtime
    agent_name, run_ids = continuity_storage
    directory = Path(__file__).parent
    tracker_directory = directory.parents[3]
    tracker_port, service_port = _port(), _port()
    tracker_url, service_url = f"http://127.0.0.1:{tracker_port}", f"http://127.0.0.1:{service_port}"
    cache = tmp_path / "executor-cache"
    await asyncio.to_thread(cache.mkdir)
    await asyncio.to_thread(shutil.copyfile, runtime.artifact, cache / f"{runtime.digest}.pex")
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            (str(tracker_directory.parents[1]), str(tracker_directory), str(tracker_directory / "src"))
        ),
        "AUTH_REQUIRED": "false",
        "BENCHMARK_API_KEY": "",
        "BROKER_ENVIRONMENT": "production",
        "DATABASE_URL": runtime.database_url,
        "REDIS_URL": runtime.redis_url,
        "STABLE_QUEUE_NAME": "continuity",
        "SANDBOX_QUEUE_ENABLED": "false",
        "SENTRY_DSN": "",
        "LOGFIRE_TOKEN": "",
        "LOGFIRE_SEND_TO_LOGFIRE": "false",
        "EXECUTOR_CACHE_DIR": str(cache),
        "EXECUTOR_RELEASE_BUCKET": harness_config.s3_bucket,
        "EXECUTOR_TRACKER_URL": tracker_url,
    }
    host_environment = {
        **environment,
        "DATABASE_URL": "postgresql://unused:unused@127.0.0.1:1/unused",
        "DB_HOST": "127.0.0.1",
        "DB_PORT": "1",
    }
    for key in ("ECS_AGENT_URI", "ECS_CONTAINER_METADATA_URI_V4"):
        host_environment.pop(key, None)
    tracker_arguments = ["-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(tracker_port)]
    service_environment = {**environment, "AUTH_DISABLED": "true"}
    service_environment.pop("AUTH_REQUIRED", None)
    async with AsyncExitStack() as stack:
        provider = await stack.enter_async_context(sandbox_provider_config.create_provider())
        client = await stack.enter_async_context(httpx.AsyncClient(timeout=60))

        async def clean_sandboxes() -> None:
            for run_id in run_ids:
                async for sandbox in provider.list_sandboxes(SandboxQuery(labels={"Id": str(run_id)})):
                    await provider.delete_sandbox(sandbox.id)

        stack.push_async_callback(clean_sandboxes)
        service = await _spawn(
            stack,
            [str(directory / "continuity_service.py"), str(service_port)],
            service_environment,
            tmp_path / "service.log",
        )
        tracker = await _spawn(stack, tracker_arguments, environment, tmp_path / "tracker-before.log")
        await _healthy(client, service_url, service)
        await _healthy(client, tracker_url, tracker)
        host = await _spawn(
            stack, [str(directory / "continuity_host.py")], host_environment, tmp_path / "host-before.log"
        )

        async def start_run() -> UUID:
            response = await client.post(
                f"{tracker_url}/start-benchmark",
                json={
                    "benchmark_name": "executor-continuity",
                    "concurrency": 1,
                    "task_ids": ["task-0"],
                    "custom_benchmark_service": service_url,
                    "harness_config": harness_config.model_dump(mode="json"),
                    "contract": {
                        "name": agent_name,
                        "install_cmd": "true",
                        "run_cmd": f"python3 /bundle/{agent_name}/run.py",
                        "final_output": "/workspace/final_output",
                        "output_artifacts": [{"path": "answer.txt", "source": "/workspace/final_output/answer.txt"}],
                    },
                },
            )
            assert response.status_code == 200, f"Run admission returned {response.status_code}; inspect Tracker log"
            run_id = UUID(response.json()["benchmark_id"])
            run_ids.append(run_id)
            print(json.dumps({"event": "run-started", "run_id": str(run_id)}))

            return run_id

        baseline = await start_run()
        await _running_sandbox(provider, baseline)
        await asyncio.to_thread(_maintenance, runtime.engine, begin=True)
        assert (await asyncio.to_thread(_evidence, runtime.engine, baseline)).status == BenchmarkStatus.STOPPED
        await _stop(host)
        await asyncio.to_thread(_maintenance, runtime.engine, begin=False)
        print(json.dumps({"event": "baseline-stopped", "run_id": str(baseline)}))

        host = await _spawn(
            stack, [str(directory / "continuity_host.py")], host_environment, tmp_path / "host-draining.log"
        )
        original = await start_run()
        original_sandbox, agent_pid = await _running_sandbox(provider, original)
        before = await asyncio.to_thread(_evidence, runtime.engine, original)
        executor_pid = await _executor_pid(host)
        await _stop(tracker)
        tracker = await _spawn(stack, tracker_arguments, environment, tmp_path / "tracker-after.log")
        await _healthy(client, tracker_url, tracker)
        assert await _executor_pid(host) == executor_pid
        host.send_signal(signal.SIGUSR1)
        replacement = await _spawn(
            stack, [str(directory / "continuity_host.py")], host_environment, tmp_path / "host-replacement.log"
        )
        second = await start_run()
        second_sandbox, _ = await _running_sandbox(provider, second)
        assert await _executor_pid(host) == executor_pid
        assert await _executor_pid(replacement) != executor_pid
        await second_sandbox.exec("touch /tmp/continuity-release", timeout=10)
        second_result = await _finished(runtime.engine, second)
        assert second_result.result_count == 1 and second_result.score == 1
        assert host.returncode is None
        current_pid = await original_sandbox.exec("cat /tmp/continuity-agent-pid", timeout=10)
        assert current_pid.stdout.strip() == agent_pid
        await original_sandbox.exec("touch /tmp/continuity-release", timeout=10)
        after = await _finished(runtime.engine, original)
        assert (after.task_id, after.started_at, after.claimant_id) == (
            before.task_id,
            before.started_at,
            before.claimant_id,
        )
        assert after.result_count == 1 and after.score == 1
        assert await asyncio.wait_for(host.wait(), 30) == 0
        await asyncio.to_thread(_verify_artifacts, harness_config, original, original_sandbox.id)
        await asyncio.to_thread(_verify_artifacts, harness_config, second, second_sandbox.id)
        print(
            json.dumps(
                {
                    "event": "continuity-passed",
                    "run_id": str(original),
                    "replacement_run_id": str(second),
                    "executor_pid": executor_pid,
                    "sandbox_id": original_sandbox.id,
                }
            )
        )
