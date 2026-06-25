from asyncio import gather
from collections.abc import Callable
from sqlite3 import OperationalError
from typing import Any

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import SetupTaskResponse
from pytest import MonkeyPatch
from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark, start_benchmark_request_to_benchmark

_TASK_ID: str = "astropy__astropy-12907"
_TASK_IDS: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]
_BENCHMARK: str = "swebench"


def _create_benchmark(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    session: Session,
    service_headers: dict[str, str],
    task_ids: list[str] | None = None,
    concurrency: int = 5,
) -> tuple[Benchmark, StartBenchmarkRequest]:
    """Create a benchmark row and matching StartBenchmarkRequest."""
    request = StartBenchmarkRequest(
        benchmark_name=_BENCHMARK,
        contract=contract,
        concurrency=concurrency,
        task_ids=task_ids,
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


def _task_rows(benchmark: Benchmark, session: Session) -> list[Task]:
    return list(session.exec(select(Task).where(Task.benchmark == benchmark.id)).all())


def _assert_no_task_errors(benchmark: Benchmark, session: Session) -> None:
    tasks = _task_rows(benchmark, session)
    task_errors = [f"{task.task_id}: {task.error_message}" for task in tasks if task.error_message]
    assert task_errors == []


def _assert_task_breakdown_complete(task_breakdown: TaskBreakdown) -> None:
    assert task_breakdown.sandbox_build_duration is not None
    assert task_breakdown.agent_run_duration is not None
    assert task_breakdown.evaluation_run_duration is not None
    assert task_breakdown.sandbox_run_duration is not None


async def test_process_benchmark(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
):
    """Multiple tasks run concurrently and produce a complete benchmark result.

    Test cases:
    - Every task finishes without a captured task error.
    - Evaluation results, task breakdowns, and final evaluation exist for the completed benchmark.
    """
    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, task_ids=_TASK_IDS
    )

    await process_benchmark(request.model_dump(), str(benchmark.id), _TASK_IDS)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert benchmark.error_message is None

    # All tasks finished
    tasks = _task_rows(benchmark, database_session)
    assert len(tasks) == len(_TASK_IDS)
    assert all(task.status == TaskStatus.FINISHED for task in tasks)
    _assert_no_task_errors(benchmark, database_session)
    for task in tasks:
        task_breakdown = database_session.get(TaskBreakdown, task.task_breakdown)
        assert task_breakdown is not None
        _assert_task_breakdown_complete(task_breakdown)

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
    """Benchmark-level errors set the benchmark status and error message.

    Test cases:
    - A database commit failure during benchmark startup marks the benchmark as ERROR.
    - The benchmark error message includes the underlying failure.
    """
    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, task_ids=[_TASK_ID]
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
    """One task can fail setup while the other still completes through the real service path.

    Test cases:
    - The patched setup failure marks only the targeted task as ERROR.
    - The non-failing task still finishes, evaluates, and has no captured error.
    """
    failing_task = "astropy__astropy-13033"
    task_ids = [_TASK_ID, failing_task]

    benchmark, request = _create_benchmark(
        contract, harness_config, database_session, service_headers, task_ids=task_ids
    )

    original_setup_task = BenchmarkServiceClient.setup_task

    async def setup_task_with_failure(
        self: Any,
        task_id: str,
        instance_id: str,
        sandbox_provider: Any = None,
        on_message: Callable[[str], None] | None = None,
        **kwargs: Any,
    ) -> SetupTaskResponse:
        if task_id == failing_task:
            raise Exception("Simulated setup failure")
        return await original_setup_task(
            self,
            task_id,
            instance_id,
            sandbox_provider=sandbox_provider,
            on_message=on_message,
            **kwargs,
        )

    monkeypatch.setattr(BenchmarkServiceClient, "setup_task", setup_task_with_failure)

    await process_benchmark(request.model_dump(), str(benchmark.id), task_ids)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED, benchmark.error_message
    assert benchmark.error_message is None

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
    assert finished_tasks[0].task_id == _TASK_ID
    assert finished_tasks[0].error_message is None

    # Evaluation exists for the successful task only
    results = benchmark.fetch_evaluation_results(database_session)
    assert len(results) == 1
    assert _TASK_ID in results
    assert failing_task not in results


async def test_process_benchmark_errors_when_all_tasks_fail_before_evaluation(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
):
    """A run with no successful task results fails before final scoring.

    Test cases:
    - A missing declared output artifact marks the task as ERROR through the real sandbox path.
    - The benchmark is marked ERROR instead of attempting final scoring with only failed task inputs.
    """
    failing_contract = contract.model_copy(update={"output_artifacts": ["missing-artifact.json"]})
    benchmark, request = _create_benchmark(
        failing_contract, harness_config, database_session, service_headers, task_ids=[_TASK_ID]
    )

    await process_benchmark(request.model_dump(), str(benchmark.id), [_TASK_ID])

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert "No tasks were completed successfully" in (benchmark.error_message or "")
    assert benchmark.final_evaluation is None

    tasks = _task_rows(benchmark, database_session)
    assert len(tasks) == 1
    assert tasks[0].status == TaskStatus.ERROR
    assert "Required output artifact missing" in (tasks[0].error_message or "")
    assert benchmark.fetch_evaluation_results(database_session) == {}


async def test_concurrent_benchmarks_same_task(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
):
    """Same task ID can run across two separate benchmarks concurrently without result collisions.

    Test cases:
    - Both benchmarks finish without benchmark or task errors.
    - Each benchmark has its own task row, evaluation result, and final evaluation.
    """
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
        assert benchmark.error_message is None
        assert benchmark.final_evaluation is not None

        tasks = _task_rows(benchmark, database_session)
        assert len(tasks) == 1
        assert tasks[0].task_id == _TASK_ID
        assert tasks[0].status == TaskStatus.FINISHED
        assert tasks[0].error_message is None

        results = benchmark.fetch_evaluation_results(database_session)
        assert set(results.keys()) == {_TASK_ID}
