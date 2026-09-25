"""Run one unchanged workload using the selected revision's services and deployment classifier."""

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import shutil
import signal
import socket
import sys
import traceback
import zipfile
from contextlib import AsyncExitStack, closing
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import asdict
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

import boto3
import httpx
from benchmark_service import Sandbox, SandboxProvider, SandboxQuery
from botocore.exceptions import ClientError
from dotenv import dotenv_values
from daytona import SandboxState
from sqlmodel import Session, SQLModel, create_engine, select
from sqlalchemy.engine import Engine
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.aws.secrets import SecretsManagerStore
from tracker.database.models import (
    Benchmark,
    DEFAULT_ORG_NAME,
    EvaluationResult,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorRelease,
    FinalEvaluation,
    Org,
    Task,
)
from tracker.executor.maintenance_control import begin_maintenance, finish_maintenance
from tracker.executor.release_control import promote_release, register_release
from tracker.runtime.artifacts import agent_bundle_key, benchmark_prefix
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import fetch_sandbox_provider_config

HERE = Path(__file__).resolve().parent


class ClientSession(Protocol):
    """The SDK client factory used across revisions with different installed service stubs."""

    def client(self, service_name: str) -> Any: ...


def classify(source: Path, scenario: str) -> dict[str, Any]:
    """Feed the revision's real classifier the same isolated task-definition change."""
    path = source / "infra/classify_executor_template_change.py"
    spec = importlib.util.spec_from_file_location("comparison_classifier", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No template classifier at {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    stack_source = (source / "infra/executor_stack.py").read_text()
    supports_drain = '"EXECUTOR_HOST_DRAIN_PROTOCOL": "1"' in stack_source
    environment = [{"Name": "EXECUTOR_HOST_DRAIN_PROTOCOL", "Value": "1"}] if supports_drain else []
    before: dict[str, Any] = {
        "Resources": {
            "HostTask": {
                "Type": "AWS::ECS::TaskDefinition",
                "Metadata": {"aws:cdk:path": "Comparison/ExecutorHostTaskDef/Resource"},
                "Properties": {
                    "TaskRoleArn": "comparison-role",
                    "ContainerDefinitions": [
                        {"Name": "host", "Image": "comparison:before", "Environment": environment}
                    ],
                },
            },
            "HostService": {
                "Type": "AWS::ECS::Service",
                "Metadata": {"aws:cdk:path": "Comparison/ExecutorHostService/Service"},
                "Properties": {"TaskDefinition": {"Ref": "HostTask"}},
            },
        }
    }
    after = deepcopy(before)
    properties = after["Resources"]["HostTask"]["Properties"]
    if scenario == "replacement":
        properties["ContainerDefinitions"][0]["Image"] = "comparison:after"
    else:
        properties["TaskRoleArn"] = "comparison-replacement-role"
    effect = module.classify_executor_host_template_change(before, after, expected_stack_id="Comparison")
    maintenance = getattr(effect, "maintenance_required", effect.redeploy_required)
    if not effect.redeploy_required:
        raise ValueError("The chosen input did not exercise host replacement")

    return {
        "effect": asdict(effect),
        "maintenance_required": maintenance,
        "advertises_drain": supports_drain,
        "before_template": before,
        "after_template": after,
    }


def configuration(env_file: Path, profile: str) -> tuple[HarnessConfig, dict[str, str]]:
    settings = {key: value for key, value in dotenv_values(env_file).items() if value is not None}
    required = ["TEST_AWS_S3_BUCKET", "TEST_LOG_GROUP", "TEST_DAYTONA_SECRET_NAME"]
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise ValueError(f"Missing test settings: {', '.join(missing)}")
    session = boto3.Session(
        profile_name=profile,
        region_name=settings.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1",
    )
    credentials = session.get_credentials()
    if credentials is None:
        raise ValueError("The selected AWS profile has no credentials")
    frozen = credentials.get_frozen_credentials()
    if not frozen.access_key or not frozen.secret_key:
        raise ValueError("The selected profile returned incomplete credentials")
    with closing(cast(ClientSession, session).client("sts")) as sts:
        sts.get_caller_identity()
    aws = AWSCredentials(
        aws_access_key_id=frozen.access_key,
        aws_secret_access_key=frozen.secret_key,
        aws_session_token=frozen.token,
        aws_default_region=session.region_name,
    )
    harness = HarnessConfig(
        sandbox_provider_secret_name=settings["TEST_DAYTONA_SECRET_NAME"],
        aws=aws,
        s3_bucket=settings["TEST_AWS_S3_BUCKET"],
        log_group=settings["TEST_LOG_GROUP"],
        log_retention_policy=int(settings.get("TEST_LOG_RETENTION") or 1),
    )
    environment = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": frozen.access_key,
        "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
        "AWS_DEFAULT_REGION": session.region_name,
    }
    environment.pop("AWS_SESSION_TOKEN", None)
    if frozen.token:
        environment["AWS_SESSION_TOKEN"] = frozen.token

    return harness, environment


def evidence(engine: Engine, run_id: UUID) -> dict[str, Any]:
    with Session(engine) as session:
        run = session.get(Benchmark, run_id)
        if run is None:
            raise LookupError("The admitted run disappeared")
        task = session.exec(select(Task).where(Task.benchmark == run_id)).first()
        dispatch = session.exec(select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == run_id)).first()
        results: Sequence[EvaluationResult] = (
            session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all() if task else []
        )
        score = session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == run_id)).first()

        return {
            "run_id": str(run_id),
            "run_status": run.status.value,
            "task_id": str(task.id) if task else None,
            "task_status": task.status.value if task else None,
            "started_at": task.started_at.isoformat() if task else None,
            "dispatch_status": dispatch.status.value if dispatch else None,
            "result_count": len(results),
            "score": score.final_score if score else None,
        }


def maintenance(engine: Engine, begin: bool) -> None:
    with Session(engine) as session:
        operation = begin_maintenance if begin else finish_maintenance
        operation(session, target_sha="a" * 40)
        session.commit()


def port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), 15)
    except TimeoutError:
        children = await asyncio.create_subprocess_exec("pgrep", "-P", str(process.pid), stdout=asyncio.subprocess.PIPE)
        output, _ = await children.communicate()
        for child in output.decode().split():
            try:
                child_pid = int(child)
                if os.getpgid(child_pid) == child_pid:
                    os.killpg(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


async def spawn(
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
    stack.push_async_callback(stop, process)
    return process


async def healthy(client: httpx.AsyncClient, url: str, process: asyncio.subprocess.Process) -> None:
    async with asyncio.timeout(60):
        while True:
            if process.returncode is not None:
                raise ChildProcessError("Local service exited; inspect its log")
            try:
                if (await client.get(f"{url}/health")).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.3)


async def running_agent(provider: SandboxProvider, run_id: UUID) -> tuple[Sandbox, str]:
    async with asyncio.timeout(240):
        while True:
            async for sandbox in provider.list_sandboxes(SandboxQuery(labels={"Id": str(run_id)})):
                if sandbox.state in {str(SandboxState.ERROR), str(SandboxState.BUILD_FAILED)}:
                    raise ChildProcessError("Sandbox failed to start")
                if sandbox.state != str(SandboxState.STARTED):
                    continue
                result = await sandbox.exec(
                    "test -f /tmp/comparison-identity && cat /tmp/comparison-identity", timeout=10
                )
                if result.exit_code == 0:
                    return sandbox, result.stdout.strip()
            await asyncio.sleep(1)


async def executor_pid(host: asyncio.subprocess.Process) -> int:
    child = await asyncio.create_subprocess_exec("pgrep", "-P", str(host.pid), stdout=asyncio.subprocess.PIPE)
    output, _ = await child.communicate()
    children = output.decode().split()
    if child.returncode != 0 or len(children) != 1:
        raise ChildProcessError("Expected exactly one packaged executor below the receiver")
    return int(children[0])


async def terminal(engine: Engine, run_id: UUID) -> dict[str, Any]:
    async with asyncio.timeout(180):
        while True:
            state = await asyncio.to_thread(evidence, engine, run_id)
            if state["run_status"] in {"STOPPED", "ERROR"} or state["dispatch_status"] in {"FINISHED", "FAILED"}:
                return state
            await asyncio.sleep(0.5)


def verify_artifacts(harness: HarnessConfig, run_id: UUID) -> None:
    with closing(cast(ClientSession, boto3.Session()).client("s3")) as s3:
        prefix = benchmark_prefix(str(run_id))
        answer = s3.get_object(Bucket=harness.s3_bucket, Key=f"{prefix}task-0/answer.txt")
        with answer["Body"] as body:
            if body.read() != b"continuity-ok":
                raise AssertionError("The completed agent output is wrong")
        report = s3.get_object(Bucket=harness.s3_bucket, Key=f"{prefix}executor-continuity.json")
        with report["Body"] as body:
            if json.loads(cast(bytes, body.read()))["final_evaluation"]["final_score"] != 1:
                raise AssertionError("The uploaded final report is wrong")


def seed_agent(harness: HarnessConfig, agent_name: str) -> None:
    agent = b"""import os, pathlib, time, uuid
pathlib.Path('/tmp/comparison-identity').write_text(f'{os.getpid()}:{uuid.uuid4()}')
deadline = time.monotonic() + 600
while not pathlib.Path('/tmp/comparison-release').exists():
    if time.monotonic() > deadline:
        raise TimeoutError('Comparison controller did not release agent')
    time.sleep(0.2)
output = pathlib.Path('/workspace/final_output')
output.mkdir(parents=True, exist_ok=True)
(output / 'answer.txt').write_text('continuity-ok')
"""
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{agent_name}/run.py", agent)
    with closing(cast(ClientSession, boto3.Session()).client("s3")) as s3:
        s3.put_object(Bucket=harness.s3_bucket, Key=agent_bundle_key(agent_name), Body=buffer.getvalue())


def clean_artifacts(harness: HarnessConfig, agent_name: str, run_ids: list[UUID]) -> None:
    with (
        closing(cast(ClientSession, boto3.Session()).client("s3")) as s3,
        closing(cast(ClientSession, boto3.Session()).client("logs")) as logs,
    ):
        for run_id in run_ids:
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=harness.s3_bucket, Prefix=benchmark_prefix(str(run_id))
            ):
                for item in page.get("Contents", []):
                    s3.delete_object(Bucket=harness.s3_bucket, Key=item["Key"])
            try:
                logs.delete_log_group(logGroupName=f"{harness.log_group}/{run_id}")
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                    raise
        s3.delete_object(Bucket=harness.s3_bucket, Key=agent_bundle_key(agent_name))


def initialize_database(engine: Engine, digest: str, bucket: str) -> None:
    with Session(engine) as session:
        session.add(Org(name=DEFAULT_ORG_NAME))
        session.add(ExecutorAdmission())
        session.commit()
        release = ExecutorRelease(
            id=f"comparison-{digest[:16]}",
            artifact_uri=f"s3://{bucket}/releases/comparison/executor.pex",
            artifact_digest=digest,
            protocol_version=SUPPORTED_PROTOCOL_VERSION,
            readiness_verified=True,
        )
        register_release(session, release)
        promote_release(session, release.id)
        session.commit()


async def exercise(
    args: argparse.Namespace,
    engine: Engine,
    database_url: str,
    redis_url: str,
    harness: HarnessConfig,
    environment: dict[str, str],
    result: dict[str, Any],
) -> None:
    decision = classify(args.source, args.scenario)
    result["classification"] = decision
    provider_config = await asyncio.to_thread(
        fetch_sandbox_provider_config,
        harness.sandbox_provider_secret_name,
        SecretsManagerStore(ExplicitCredentialsAWSClientProvider(harness.aws)),
        "daytona",
    )
    digest = await asyncio.to_thread(lambda: hashlib.sha256(args.artifact.read_bytes()).hexdigest())
    cache = args.evidence / "cache"
    cache.mkdir()
    await asyncio.to_thread(shutil.copyfile, args.artifact, cache / f"{digest}.pex")
    await asyncio.to_thread(initialize_database, engine, digest, harness.s3_bucket)
    result["protocol"] = SUPPORTED_PROTOCOL_VERSION
    result["artifact_digest"] = digest
    tracker_port, service_port = port(), port()
    tracker_url, service_url = f"http://127.0.0.1:{tracker_port}", f"http://127.0.0.1:{service_port}"
    environment = {
        **environment,
        "AUTH_REQUIRED": "false",
        "BENCHMARK_API_KEY": "",
        "BROKER_ENVIRONMENT": "production",
        "DATABASE_URL": database_url,
        "REDIS_URL": redis_url,
        "STABLE_QUEUE_NAME": "comparison",
        "SANDBOX_QUEUE_ENABLED": "false",
        "SENTRY_DSN": "",
        "LOGFIRE_TOKEN": "",
        "LOGFIRE_SEND_TO_LOGFIRE": "false",
        "EXECUTOR_CACHE_DIR": str(cache),
        "EXECUTOR_RELEASE_BUCKET": harness.s3_bucket,
        "EXECUTOR_TRACKER_URL": tracker_url,
    }
    database = urlparse(database_url)
    host_environment = {
        **environment,
        "DB_HOST": str(database.hostname),
        "DB_PORT": str(database.port),
        "DB_NAME": database.path.lstrip("/"),
        "DB_USERNAME": str(database.username),
        "DB_PASSWORD": str(database.password),
    }
    for key in ("ECS_AGENT_URI", "ECS_CONTAINER_METADATA_URI_V4"):
        host_environment.pop(key, None)
    service_environment = {**environment, "AUTH_DISABLED": "true"}
    service_environment.pop("AUTH_REQUIRED", None)
    agent_name = f"executor-comparison-{uuid4().hex}"
    run_ids: list[UUID] = []
    result.update(agent=agent_name, run_ids=[], endpoint=tracker_url)
    print(json.dumps({"event": "test-resources", "agent": agent_name, "bucket": harness.s3_bucket}), flush=True)
    tracker_arguments = ["-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(tracker_port)]
    try:
        async with provider_config.create_provider() as provider:
            try:
                async with AsyncExitStack() as stack:
                    await asyncio.to_thread(seed_agent, harness, agent_name)
                    client = await stack.enter_async_context(httpx.AsyncClient(timeout=60))
                    service = await spawn(
                        stack,
                        [str(HERE / "service.py"), str(service_port)],
                        service_environment,
                        args.evidence / "service.log",
                    )
                    tracker = await spawn(stack, tracker_arguments, environment, args.evidence / "tracker-before.log")
                    await healthy(client, service_url, service)
                    await healthy(client, tracker_url, tracker)
                    host = await spawn(
                        stack, [str(HERE / "host.py")], host_environment, args.evidence / "host-before.log"
                    )

                    async def start_run() -> UUID:
                        response = await client.post(
                            f"{tracker_url}/start-benchmark",
                            json={
                                "benchmark_name": "executor-continuity",
                                "concurrency": 1,
                                "task_ids": ["task-0"],
                                "custom_benchmark_service": service_url,
                                "harness_config": harness.model_dump(mode="json"),
                                "contract": {
                                    "name": agent_name,
                                    "install_cmd": "true",
                                    "run_cmd": f"python3 /bundle/{agent_name}/run.py",
                                    "final_output": "/workspace/final_output",
                                    "output_artifacts": [
                                        {"path": "answer.txt", "source": "/workspace/final_output/answer.txt"}
                                    ],
                                },
                            },
                        )
                        response.raise_for_status()
                        run_id = UUID(response.json()["benchmark_id"])
                        run_ids.append(run_id)
                        result["run_ids"] = [str(value) for value in run_ids]
                        print(json.dumps({"event": "run-started", "run_id": str(run_id)}), flush=True)
                        return run_id

                    original = await start_run()
                    sandbox, agent_identity = await running_agent(provider, original)
                    pid = await executor_pid(host)
                    before = await asyncio.to_thread(evidence, engine, original)
                    result.update(before=before, agent_identity=agent_identity, sandbox_id=sandbox.id, executor_pid=pid)
                    if before["task_status"] != "IN_PROGRESS":
                        raise AssertionError("The original task was not running at the deployment boundary")
                    result["phase"] = "replacement"
                    await stop(tracker)
                    tracker = await spawn(stack, tracker_arguments, environment, args.evidence / "tracker-after.log")
                    await healthy(client, tracker_url, tracker)
                    if decision["maintenance_required"]:
                        await asyncio.to_thread(maintenance, engine, True)
                        await stop(host)
                        await asyncio.to_thread(maintenance, engine, False)
                        result["operation"] = "maintenance_then_replace"
                    else:
                        host.send_signal(signal.SIGUSR1)
                        result["operation"] = "drain_then_retire"
                    replacement = await spawn(
                        stack, [str(HERE / "host.py")], host_environment, args.evidence / "host-after.log"
                    )
                    second = await start_run()
                    second_sandbox, _ = await running_agent(provider, second)
                    await second_sandbox.exec("touch /tmp/comparison-release", timeout=10)
                    second_state = await terminal(engine, second)
                    result["replacement"] = second_state
                    if second_state["result_count"] != 1 or second_state["score"] != 1:
                        raise AssertionError("The replacement host could not finish a healthy control run")
                    await asyncio.to_thread(verify_artifacts, harness, second)
                    after = await asyncio.to_thread(evidence, engine, original)
                    result["after"] = after
                    if after["run_status"] in {"STOPPED", "ERROR"} or after["task_status"] in {"STOPPED", "ERROR"}:
                        result.update(
                            outcome="interrupted",
                            reason="Original run/task became terminal without a successful result",
                        )
                        return
                    if host.returncode is not None or await executor_pid(host) != pid:
                        result.update(outcome="interrupted", reason="Original executor process exited or changed")
                        return
                    live_identity = await sandbox.exec("cat /tmp/comparison-identity", timeout=10)
                    if live_identity.exit_code != 0 or live_identity.stdout.strip() != agent_identity:
                        result.update(outcome="interrupted", reason="Original agent identity changed")
                        return
                    await sandbox.exec("touch /tmp/comparison-release", timeout=10)
                    after = await terminal(engine, original)
                    result["after"] = after
                    if (after["task_id"], after["started_at"], after["result_count"], after["score"]) != (
                        before["task_id"],
                        before["started_at"],
                        1,
                        1,
                    ):
                        result.update(outcome="interrupted", reason="Original attempt did not finish exactly once")
                        return
                    await asyncio.to_thread(verify_artifacts, harness, original)
                    exit_code = await asyncio.wait_for(host.wait(), 30)
                    if exit_code != 0 or replacement.returncode is not None:
                        raise ChildProcessError("Host retirement or replacement health check failed")
                    result.update(outcome="survived", artifacts_verified=True, original_host_retired=True)
            finally:
                try:
                    async with asyncio.timeout(90):
                        for run_id in run_ids:
                            async for sandbox in provider.list_sandboxes(SandboxQuery(labels={"Id": str(run_id)})):
                                await provider.delete_sandbox(sandbox.id)
                    result["sandboxes_cleaned"] = True
                except Exception as error:
                    result.update(outcome="cleanup_error", cleanup_error_type=type(error).__name__)
    finally:
        try:
            await asyncio.to_thread(clean_artifacts, harness, agent_name, run_ids)
            result["artifacts_cleaned"] = True
        except Exception as error:
            result.update(outcome="cleanup_error", cleanup_error_type=type(error).__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "artifact", "evidence", "env-file"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--scenario", choices=["replacement", "maintenance"], required=True)
    parser.add_argument("--aws-profile", required=True)
    args = parser.parse_args()
    result: dict[str, Any] = {"outcome": "setup_error", "phase": "setup"}
    try:
        harness, environment = configuration(args.env_file, args.aws_profile)
        os.environ.update(environment)
        with (
            PostgresContainer("postgres:16-alpine") as postgres,
            DockerContainer("redis:7-alpine").with_exposed_ports(6379) as redis,
        ):
            database_url = postgres.get_connection_url()
            engine = create_engine(database_url)
            try:
                SQLModel.metadata.create_all(engine)
                asyncio.run(
                    exercise(
                        args,
                        engine,
                        database_url,
                        f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}",
                        harness,
                        environment,
                        result,
                    )
                )
            finally:
                engine.dispose()
    except Exception as error:
        if result["outcome"] != "cleanup_error":
            result["outcome"] = "setup_error" if result["phase"] == "setup" else "probe_error"
        result["error_type"] = type(error).__name__
        frame = traceback.extract_tb(error.__traceback__)[-1]
        result["error_location"] = {"file": frame.filename, "line": frame.lineno, "function": frame.name}
        if isinstance(error, (AssertionError, ChildProcessError)):
            result["reason"] = str(error)
    finally:
        (args.evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"outcome": result["outcome"], "phase": result["phase"]}), flush=True)


if __name__ == "__main__":
    main()
