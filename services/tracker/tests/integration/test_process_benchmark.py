from asyncio import Semaphore, gather
from collections.abc import Callable
from sqlite3 import OperationalError
from typing import Any

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import SetupTaskResponse
from pytest import MonkeyPatch
from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.cloudwatch_logs import create_benchmark_log_group
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.aws.s3 import copy_agent_to_benchmark
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark, process_task, start_benchmark_request_to_benchmark

_TASK_ID: str = "astropy__astropy-12907"
_TASK_IDS: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]
_BENCHMARK: str = "swebench"


def _create_benchmark(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    session: Session,
    service_headers: dict[str, str],
    _TASK_IDS: list[str] | None = None,
    concurrency: int = 5,
) -> tuple[Benchmark, StartBenchmarkRequest]:
    """Create a benchmark row and matching StartBenchmarkRequest."""
    request = StartBenchmarkRequest(
        benchmark_name=_BENCHMARK,
        contract=contract,
        concurrency=concurrency,
        task_ids=_TASK_IDS,
        harness_config=harness_config,
        service_headers=service_headers,
    )
    benchmark = start_benchmark_request_to_benchmark(
        request,
        RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), access_key_id=None, email=None, name=None),
    )
    session.add(benchmark)
    session.commit()
    return benchmark, request


async def test_process_task(
    contract: AgentContractRequest,
    database_session: Session,
    benchmark_service: BenchmarkServiceClient,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
):
    """Single task runs through the full pipeline and finishes with an evaluation result."""
    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, _TASK_IDS=[_TASK_ID]
    )

    task_row = Task(org_id=TEST_ORG_ID, task_id=_TASK_ID, benchmark=benchmark.id)
    database_session.add(task_row)
    database_session.commit()

    # process_task expects the log group to already exist
    create_benchmark_log_group(
        str(benchmark.id), harness_config.aws, harness_config.log_group, harness_config.log_retention_policy
    )

    # Need to copy agent inside subdir before we start the task
    await copy_agent_to_benchmark(str(benchmark.id), contract.name, harness_config.aws, harness_config.s3_bucket)

    await process_task(
        task_row,
        request,
        benchmark_service,
        benchmark.id,
        _TASK_ID,
        harness_config,
        Org(id=TEST_ORG_ID, name="default"),
        creation_semaphore=Semaphore(10),
    )

    database_session.refresh(task_row)
    assert task_row.status == TaskStatus.FINISHED

    evaluation = database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_row.id)).first()
    assert evaluation is not None
    assert evaluation.result is not None

    # Task breakdown is tracked while the task runs
    database_session.refresh(task_row)
    task_breakdown = database_session.get(TaskBreakdown, task_row.task_breakdown)
    assert task_breakdown is not None
    for attr in task_breakdown.__dict__:
        if not attr.startswith("_"):
            assert getattr(task_breakdown, attr) is not None


async def test_process_benchmark(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
):
    """Multiple tasks run concurrently, all finish, final score is calculated."""
    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, _TASK_IDS=_TASK_IDS
    )

    await process_benchmark(request.model_dump(), str(benchmark.id), _TASK_IDS)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED

    # All tasks finished
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    assert len(tasks) == len(_TASK_IDS)
    assert all(task.status == TaskStatus.FINISHED for task in tasks)

    # Evaluation results exist for every task
    results = benchmark.fetch_evaluation_results(database_session)
    assert set(results.keys()) == set(_TASK_IDS)

    # Final evaluation was calculated
    assert benchmark.final_evaluation is not None


async def test_process_benchmark_error(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: MonkeyPatch,
    service_headers: dict[str, str],
):
    """Benchmark-level error (e.g. database failure) sets benchmark status to ERROR."""
    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, _TASK_IDS=[_TASK_ID]
    )

    original_commit = Session.commit
    commit_count = {"n": 0}

    def failing_commit(self: Session) -> None:
        commit_count["n"] += 1
        if commit_count["n"] == 1:
            raise OperationalError("Simulated database error")
        original_commit(self)

    monkeypatch.setattr(Session, "commit", failing_commit)

    await process_benchmark(request.model_dump(), str(benchmark.id), [_TASK_ID])

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "Simulated database error" in (benchmark.error_message or "")


async def test_process_task_error(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: MonkeyPatch,
    service_headers: dict[str, str],
):
    """One task errors during setup while the other succeeds — benchmark still finishes."""
    failing_task = "astropy__astropy-13033"
    _TASK_IDS = [_TASK_ID, failing_task]

    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, _TASK_IDS=_TASK_IDS
    )

    original_setup_task = BenchmarkServiceClient.setup_task

    async def setup_task_with_failure(
        self: Any, task_id: str, instance_id: str, on_message: Callable[[str], None] | None = None, **kwargs: Any
    ) -> SetupTaskResponse:
        if task_id == failing_task:
            raise Exception("Simulated setup failure")
        return await original_setup_task(self, task_id, instance_id, on_message, **kwargs)

    monkeypatch.setattr(BenchmarkServiceClient, "setup_task", setup_task_with_failure)

    await process_benchmark(request.model_dump(), str(benchmark.id), _TASK_IDS)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED, benchmark.error_message

    # The failing task errored with our message
    error_tasks = database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.status == TaskStatus.ERROR)
    ).all()
    assert len(error_tasks) == 1
    assert error_tasks[0].task_id == failing_task
    assert "Simulated setup failure" in (error_tasks[0].error_message or "")

    # The valid task still finished
    finished_tasks = database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.status == TaskStatus.FINISHED)
    ).all()
    assert len(finished_tasks) == 1

    # Evaluation exists for the successful task only
    results = benchmark.fetch_evaluation_results(database_session)
    assert len(results) == 1
    assert _TASK_ID in results


async def test_concurrent_benchmarks_same_task(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
):
    """Same task ID can run across two separate benchmarks concurrently without conflicts."""
    benchmarks: list[Benchmark] = []
    for _ in range(2):
        benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name=_BENCHMARK,
            arguments=BenchmarkArguments(contract=contract, concurrency=5, task_ids=[_TASK_ID]),
        )
        benchmarks.append(benchmark)
        database_session.add(benchmark)

    database_session.commit()

    await gather(
        *[
            process_benchmark(
                benchmark.start_benchmark_request(harness_config, service_headers=service_headers).model_dump(),
                str(benchmark.id),
                [_TASK_ID],
            )
            for benchmark in benchmarks
        ]
    )

    for benchmark in benchmarks:
        database_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED, (
            f"Benchmark {benchmark.id} error: {benchmark.error_message}"
        )
        task = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).first()
        assert task and task.task_id == _TASK_ID
