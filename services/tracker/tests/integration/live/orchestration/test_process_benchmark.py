"""Run with `uv run pytest tests/integration/live/orchestration/test_process_benchmark.py`.

Exercise tracker orchestration against real services and sandboxes.
"""

from __future__ import annotations

from asyncio import gather
from collections.abc import AsyncGenerator, Callable
from sqlite3 import OperationalError
from typing import Any

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import SetupTaskResponse
import pytest
from sqlmodel import Session, select

import tracker.utils as tracker_utils
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.s3 import copy_agent_to_benchmark, delete_from_s3, get_benchmark_contract_s3_key
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import start_benchmark_request_to_benchmark

process_benchmark = getattr(tracker_utils, "process_benchmark")

_TASK_ID: str = "astropy__astropy-12907"
_TASK_IDS: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]
_BENCHMARK: str = "swebench"

pytestmark = pytest.mark.usefixtures("tracker_database")


@pytest.fixture
async def frozen_contract_keys(harness_config: HarnessConfig) -> AsyncGenerator[set[str], None]:
    """Delete benchmark-scoped contract copies created by each live test."""
    keys: set[str] = set()
    try:
        yield keys
    finally:
        for key in sorted(keys):
            await delete_from_s3(key, harness_config.aws, harness_config.s3_bucket)


async def _create_benchmark(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    frozen_contract_keys: set[str],
    session: Session,
    service_headers: dict[str, str],
    task_ids: list[str] | None = None,
    concurrency: int = 5,
) -> tuple[Benchmark, StartBenchmarkRequest]:
    """Create an admitted benchmark and matching StartBenchmarkRequest."""
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
    copied = await copy_agent_to_benchmark(
        str(benchmark.id),
        contract.name,
        harness_config.aws,
        harness_config.s3_bucket,
    )
    if copied:
        frozen_contract_keys.add(get_benchmark_contract_s3_key(str(benchmark.id), contract.name))
    session.add(benchmark)
    session.commit()
    return benchmark, request


def _task_rows(benchmark: Benchmark, session: Session) -> list[Task]:
    return list(session.exec(select(Task).where(Task.benchmark == benchmark.id)).all())


def _assert_no_task_errors(benchmark: Benchmark, session: Session) -> None:
    assert benchmark.fetch_tasks_with_errors(session) is None


def _assert_task_breakdown_complete(task_breakdown: TaskBreakdown) -> None:
    assert task_breakdown.sandbox_build_duration is not None
    assert task_breakdown.agent_run_duration is not None
    assert task_breakdown.evaluation_run_duration is not None
    assert task_breakdown.sandbox_run_duration is not None


class TestProcessBenchmark:
    """Live benchmark orchestration success, failure, and concurrency flows."""

    async def test_process_benchmark(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        frozen_contract_keys: set[str],
        service_headers: dict[str, str],
        executor_authority_kwargs: Any,
    ) -> None:
        """Multiple tasks run concurrently and produce a complete benchmark result.

        Test cases:
        - Every task finishes without a captured task error.
        - Evaluation results, task breakdowns, and final evaluation exist for the completed benchmark.
        """
        benchmark, request = await _create_benchmark(
            contract,
            harness_config,
            frozen_contract_keys,
            database_session,
            service_headers,
            task_ids=_TASK_IDS,
        )

        authority_kwargs = executor_authority_kwargs(benchmark)
        await process_benchmark(request.model_dump(), str(benchmark.id), _TASK_IDS, **authority_kwargs)

        database_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED
        assert benchmark.error_message is None

        tasks = _task_rows(benchmark, database_session)
        assert len(tasks) == len(_TASK_IDS)
        assert all(task.status == TaskStatus.FINISHED for task in tasks)
        _assert_no_task_errors(benchmark, database_session)
        for task in tasks:
            task_breakdown = database_session.get(TaskBreakdown, task.task_breakdown)
            assert task_breakdown is not None
            _assert_task_breakdown_complete(task_breakdown)

        results = benchmark.fetch_evaluation_results(database_session)
        assert set(results.keys()) == set(_TASK_IDS)

        assert benchmark.final_evaluation is not None

    async def test_process_benchmark_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        frozen_contract_keys: set[str],
        monkeypatch: pytest.MonkeyPatch,
        service_headers: dict[str, str],
        executor_authority_kwargs: Any,
    ) -> None:
        """Benchmark-level errors set the benchmark status and error message.

        Test cases:
        - A database commit failure during benchmark startup marks the benchmark as ERROR.
        - The benchmark error message includes the underlying failure.
        """
        benchmark, request = await _create_benchmark(
            contract,
            harness_config,
            frozen_contract_keys,
            database_session,
            service_headers,
            task_ids=[_TASK_ID],
        )

        original_commit = Session.commit
        commit_count = 0

        def failing_commit(self: Session) -> None:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise OperationalError("Simulated database error")

            original_commit(self)

        authority_kwargs = executor_authority_kwargs(benchmark)
        monkeypatch.setattr(Session, "commit", failing_commit)

        await process_benchmark(request.model_dump(), str(benchmark.id), [_TASK_ID], **authority_kwargs)

        database_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.ERROR
        assert "Simulated database error" in (benchmark.error_message or "")

    async def test_process_task_error(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        frozen_contract_keys: set[str],
        monkeypatch: pytest.MonkeyPatch,
        service_headers: dict[str, str],
        executor_authority_kwargs: Any,
    ) -> None:
        """One task can fail setup while the other still completes through the real service path.

        Test cases:
        - The patched setup failure marks only the targeted task as ERROR.
        - The non-failing task still finishes, evaluates, and has no captured error.
        """
        failing_task = "astropy__astropy-13033"
        task_ids = [_TASK_ID, failing_task]

        benchmark, request = await _create_benchmark(
            contract,
            harness_config,
            frozen_contract_keys,
            database_session,
            service_headers,
            task_ids=task_ids,
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
                raise RuntimeError("Simulated setup failure")

            return await original_setup_task(
                self,
                task_id,
                instance_id,
                sandbox_provider=sandbox_provider,
                on_message=on_message,
                **kwargs,
            )

        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", setup_task_with_failure)
        authority_kwargs = executor_authority_kwargs(benchmark)

        await process_benchmark(request.model_dump(), str(benchmark.id), task_ids, **authority_kwargs)

        database_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED, benchmark.error_message
        assert benchmark.error_message is None

        error_tasks = database_session.exec(
            select(Task).where(Task.benchmark == benchmark.id).where(Task.status == TaskStatus.ERROR)
        ).all()
        assert len(error_tasks) == 1
        assert error_tasks[0].task_id == failing_task
        task_errors = benchmark.fetch_tasks_with_errors(database_session)
        assert task_errors is not None
        assert "Simulated setup failure" in task_errors[failing_task]

        finished_tasks = database_session.exec(
            select(Task).where(Task.benchmark == benchmark.id).where(Task.status == TaskStatus.FINISHED)
        ).all()
        assert len(finished_tasks) == 1
        assert finished_tasks[0].task_id == _TASK_ID

        results = benchmark.fetch_evaluation_results(database_session)
        assert len(results) == 1
        assert _TASK_ID in results
        assert failing_task not in results

    async def test_process_benchmark_finishes_when_output_artifact_is_missing(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        frozen_contract_keys: set[str],
        service_headers: dict[str, str],
        executor_authority_kwargs: Any,
    ) -> None:
        """A missing declared output artifact does not prevent evaluation.

        Test cases:
        - The missing artifact is skipped through the real sandbox path instead of erroring the task.
        - The task is evaluated and the benchmark finishes with a final evaluation.
        """
        failing_contract = contract.model_copy(update={"output_artifacts": ["missing-artifact.json"]})
        benchmark, request = await _create_benchmark(
            failing_contract,
            harness_config,
            frozen_contract_keys,
            database_session,
            service_headers,
            task_ids=[_TASK_ID],
        )
        authority_kwargs = executor_authority_kwargs(benchmark)

        await process_benchmark(request.model_dump(), str(benchmark.id), [_TASK_ID], **authority_kwargs)

        database_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED
        assert benchmark.error_message is None
        assert benchmark.final_evaluation is not None

        tasks = _task_rows(benchmark, database_session)
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.FINISHED
        results = benchmark.fetch_evaluation_results(database_session)
        assert _TASK_ID in results

    async def test_concurrent_benchmarks_same_task(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        frozen_contract_keys: set[str],
        service_headers: dict[str, str],
        executor_authority_kwargs: Any,
    ) -> None:
        """Same task ID can run across two separate benchmarks concurrently without result collisions.

        Test cases:
        - Both benchmarks finish without benchmark or task errors.
        - Each benchmark has its own task row, evaluation result, and final evaluation.
        """
        benchmark_requests = [
            await _create_benchmark(
                contract,
                harness_config,
                frozen_contract_keys,
                database_session,
                service_headers,
                task_ids=[_TASK_ID],
            )
            for _ in range(2)
        ]
        authority_by_benchmark = {
            benchmark.id: executor_authority_kwargs(benchmark) for benchmark, _ in benchmark_requests
        }

        await gather(
            *[
                process_benchmark(
                    request.model_dump(),
                    str(benchmark.id),
                    [_TASK_ID],
                    **authority_by_benchmark[benchmark.id],
                )
                for benchmark, request in benchmark_requests
            ]
        )

        for benchmark, _ in benchmark_requests:
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
            assert benchmark.fetch_tasks_with_errors(database_session) is None

            results = benchmark.fetch_evaluation_results(database_session)
            assert set(results.keys()) == {_TASK_ID}
