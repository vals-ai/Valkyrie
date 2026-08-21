"""Shared process-task setup for tracker unit tests."""

from asyncio import Semaphore
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from benchmark_service import ImageSource, Resources
from benchmark_service.schemas import RetrieveTaskResponse
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    Task,
)
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import fetch_sandbox_provider_config, process_task, start_benchmark_request_to_benchmark

TEST_ORG = Org(id=TEST_ORG_ID, name="default")
_TEST_STARTER = RequestIdentity(org=TEST_ORG, access_key_id=None, email=None, name=None)


class MockKicker:
    """Record task dispatches without starting a worker."""

    def __init__(self) -> None:
        self.queued_calls: list[dict[str, Any]] = []

    def with_labels(self, **_labels: str) -> "MockKicker":
        return self

    async def kiq(self, **kwargs: Any) -> None:
        self.queued_calls.append(kwargs)


def make_retrieve_task_response(problem_path: str = "/tmp/problem_statement.txt") -> RetrieveTaskResponse:
    """Build the benchmark task response shared by process-task tests."""
    return RetrieveTaskResponse(
        source=ImageSource(image="test-image:latest"),
        problem_path=problem_path,
        cwd="/testbed",
        resources=Resources(vcpu=2, memory=4, disk=5),
    )


def create_task_environment(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    run_starter: RequestIdentity | None = None,
) -> tuple[StartBenchmarkRequest, Task, UUID, ExecutionAuthority]:
    """Persist the benchmark and task rows required by process-task tests.

    Arguments
    - contract: Agent contract used by the benchmark request.
    - database_session: Test database session receiving the rows.
    - harness_config: Harness configuration stored with the request.
    - run_starter: Optional identity that started the benchmark.

    Returns
    - The request, task row, and benchmark ID needed to run the task.
    """
    start_benchmark_request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=1,
        task_ids=["task_0"],
        harness_config=harness_config,
    )
    benchmark_row = start_benchmark_request_to_benchmark(
        start_benchmark_request,
        run_starter or _TEST_STARTER,
        aws_managed=False,
    )
    benchmark_row.status = BenchmarkStatus.IN_PROGRESS
    database_session.add(benchmark_row)
    database_session.commit()

    task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
    database_session.add(task_row)
    database_session.commit()

    release = ExecutorRelease(
        id="task-execution-test-release",
        artifact_uri="s3://artifacts/task-execution-test.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
    )
    database_session.add(release)
    database_session.flush()
    dispatch = ExecutorDispatch(
        benchmark_id=benchmark_row.id,
        kind=ExecutorDispatchKind.START,
        status=ExecutorDispatchStatus.RUNNING,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
        started_at=datetime.now(UTC),
    )
    database_session.add(dispatch)
    database_session.commit()

    authority = ExecutionAuthority(benchmark_id=benchmark_row.id, dispatch_id=dispatch.id)
    return start_benchmark_request, task_row, benchmark_row.id, authority


async def run_process_task(
    start_benchmark_request: StartBenchmarkRequest,
    task_row: Task,
    benchmark_id: UUID,
    aws_runtime: AWSRuntime,
    authority: ExecutionAuthority,
) -> dict[str, dict[str, Any] | None]:
    """Run process_task with the shared deterministic unit-test dependencies.

    Arguments
    - start_benchmark_request: Request that created the benchmark.
    - task_row: Persisted task being processed.
    - benchmark_id: Parent benchmark identifier.
    - aws_runtime: Shared AWS runtime used for provider resolution.
    - authority: Execution authority for the dispatch being exercised.

    Returns
    - The task result mapping returned by process_task.
    """
    return await process_task(
        task_row=task_row,
        start_benchmark_request=start_benchmark_request,
        benchmark_service=start_benchmark_request.benchmark_service,
        benchmark_id=benchmark_id,
        task_id="task_0",
        aws_runtime=aws_runtime,
        org=TEST_ORG,
        sandbox_provider_config=fetch_sandbox_provider_config(
            start_benchmark_request.harness_config.sandbox_provider_secret_name,
            aws_runtime.clients,
            start_benchmark_request.sandbox_provider,
        ),
        creation_semaphore=Semaphore(1),
        authority=authority,
    )
