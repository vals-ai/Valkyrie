"""Run with `uv run pytest tests/integration/local/database/test_run_finalization.py`.

Exercise run-finalization concurrency against disposable Postgres.
"""

from typing import Any
from uuid import uuid4

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.sandbox import DaytonaProviderConfig
from benchmark_service.schemas import FinalScoreResponse
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

import tracker.utils.run_orchestration as run_orchestration_module
from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark
from tracker.utils.task_error_summary import summarize_task_errors


class TestRunFinalization:
    """Run finalization and concurrent retry behavior."""

    async def test_all_error_finalization_honors_concurrent_status_changes(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """All-error finalization must preserve status changes made while errors are summarized.

        Test cases:
        - A concurrent retry leaves the run in progress and defers finalization.
        - A concurrent stop marks the run stopped without committing an error summary.
        """
        org = Org(id=uuid4(), name=f"error-finalization-race-{uuid4()}")
        contract = AgentContractRequest(name="error-race-agent", install_cmd="true", run_cmd="true")
        target_statuses = {
            "retry-during-summary": TaskStatus.PENDING,
            "stop-during-summary": TaskStatus.STOPPED,
        }

        postgres_session.add(org)
        postgres_session.flush()
        benchmarks: list[Benchmark] = []
        for task_id in target_statuses:
            benchmark = make_benchmark(
                name=task_id,
                org_id=org.id,
                contract=contract,
                status=BenchmarkStatus.IN_PROGRESS,
            )
            postgres_session.add(benchmark)
            postgres_session.flush()
            task = make_task(benchmark, task_id, status=TaskStatus.ERROR)
            postgres_session.add(task)
            postgres_session.flush()
            postgres_session.add(ErrorResult(org_id=org.id, task=task.id, error_message="Agent failed"))
            benchmarks.append(benchmark)
        postgres_session.commit()

        def change_status_during_summary(task_errors: dict[str, str]) -> str:
            task_id = next(iter(task_errors))
            with Session(postgres_engine) as transition_session:
                task = transition_session.exec(
                    select(Task).where(Task.task_id == task_id).where(Task.org_id == org.id)
                ).one()
                task.status = target_statuses[task_id]
                transition_session.add(task)
                transition_session.commit()

            return summarize_task_errors(task_errors)

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "summarize_task_errors", change_status_during_summary)

        deferred_results = [
            await run_orchestration_module.finalize_all_error_run(benchmark.id, org) for benchmark in benchmarks
        ]

        with Session(postgres_engine) as assertion_session:
            persisted_benchmarks = [assertion_session.get(Benchmark, benchmark.id) for benchmark in benchmarks]

        assert deferred_results == [True, False]
        assert all(benchmark is not None for benchmark in persisted_benchmarks)
        assert [benchmark.status for benchmark in persisted_benchmarks if benchmark] == [
            BenchmarkStatus.IN_PROGRESS,
            BenchmarkStatus.STOPPED,
        ]
        assert all(benchmark.error_message is None for benchmark in persisted_benchmarks if benchmark)

    async def test_concurrent_retry_prevents_stale_final_evaluation(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A retry starting during finalization must leave the run active without a stale score.

        Test cases:
        - A task becomes runnable after scoring and the worker's initial finalization check.
        - The old worker leaves the benchmark in progress without writing its stale final score.
        """
        org = Org(id=uuid4(), name=f"finalization-race-{uuid4()}")
        contract = AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true")
        benchmark = make_benchmark(
            name="finalization-race",
            org_id=org.id,
            contract=contract,
            status=BenchmarkStatus.IN_PROGRESS,
        )
        task = make_task(benchmark, "retried-task", status=TaskStatus.FINISHED)

        postgres_session.add(org)
        postgres_session.flush()
        postgres_session.add(benchmark)
        postgres_session.flush()
        postgres_session.add(task)
        postgres_session.flush()
        postgres_session.add(
            EvaluationResult(
                org_id=org.id,
                task=task.id,
                instance_id=f"race-{task.id}",
                result={"score": 1.0},
            )
        )
        postgres_session.commit()

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name=benchmark.name,
            concurrency=1,
            harness_config=harness_config,
        )

        async def skip_cloud_operation(*_args: Any, **_kwargs: Any) -> None:
            return None

        def skip_log_group(*_args: Any, **_kwargs: Any) -> str:
            return "test-log-group"

        def provider_config(*_args: Any, **_kwargs: Any) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://example.com",
                DAYTONA_TARGET="test-target",
            )

        async def stale_final_score(*_args: Any, **_kwargs: Any) -> FinalScoreResponse:
            return FinalScoreResponse(tasks_evaluated=[task.task_id], final_score=0.25, metadata={})

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "copy_agent_to_benchmark", skip_cloud_operation)
        monkeypatch.setattr(run_orchestration_module, "create_benchmark_log_group", skip_log_group)
        monkeypatch.setattr(run_orchestration_module, "fetch_sandbox_provider_config", provider_config)
        monkeypatch.setattr(run_orchestration_module, "upload_final_view", skip_cloud_operation)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", stale_final_score)

        original_has_runnable_tasks = run_orchestration_module.has_runnable_tasks
        runnable_task_checks = 0

        def start_retry_after_check(session: Session, benchmark_row: Benchmark, current_org: Org) -> bool:
            nonlocal runnable_task_checks
            has_runnable_task = original_has_runnable_tasks(session, benchmark_row, current_org)
            runnable_task_checks += 1

            if runnable_task_checks == 2:
                with Session(postgres_engine) as retry_session:
                    retried_task = retry_session.exec(select(Task).where(Task.id == task.id)).one()
                    retried_task.status = TaskStatus.PENDING
                    retry_session.add(retried_task)
                    retry_session.commit()

            return has_runnable_task

        monkeypatch.setattr(run_orchestration_module, "has_runnable_tasks", start_retry_after_check)

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark.id),
            verified_task_ids=[],
        )

        with Session(postgres_engine) as assertion_session:
            persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)
            persisted_task = assertion_session.get(Task, task.id)
            final_evaluation = assertion_session.exec(
                select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)
            ).first()

        assert persisted_benchmark is not None
        assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
        assert persisted_task is not None
        assert persisted_task.status == TaskStatus.PENDING
        assert final_evaluation is None

    async def test_all_error_finalization_returns_distinct_representatives(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """All-error finalization should persist one representative per distinct error group.

        Test cases:
        - Eight tasks split across API, model-key, and network failures produce three representatives.
        - Eight identical failures produce one representative.
        """
        org = Org(id=uuid4(), name=f"error-summary-{uuid4()}")
        contract = AgentContractRequest(name="error-summary-agent", install_cmd="true", run_cmd="true")
        grouped_errors = [
            "Benchmark API authentication failed for key key-a",
            "Benchmark API authentication failed for key key-b",
            "Requested model key model-a is not registered",
            "Requested model key model-b is not registered",
            "Requested model key model-c is not registered",
            "Network connection to model gateway timed out after 30 seconds on attempt 1",
            "Network connection to model gateway timed out after 30 seconds on attempt 2",
            "Network connection to model gateway timed out after 30 seconds on attempt 3",
        ]
        cases = [
            (
                "grouped-errors",
                grouped_errors,
                "No tasks were completed successfully. 3 distinct errors:\n"
                "- 3/8 tasks: Requested model key model-a is not registered\n"
                "- 3/8 tasks: Network connection to model gateway timed out after 30 seconds on attempt 1\n"
                "- 2/8 tasks: Benchmark API authentication failed for key key-a",
            ),
            (
                "identical-errors",
                ["Network connection to model gateway timed out"] * 8,
                "No tasks were completed successfully. 1 distinct error:\n"
                "- 8/8 tasks: Network connection to model gateway timed out",
            ),
        ]

        postgres_session.add(org)
        postgres_session.flush()
        benchmarks: list[tuple[Benchmark, str]] = []
        for benchmark_name, error_messages, expected_summary in cases:
            benchmark = make_benchmark(
                name=benchmark_name,
                org_id=org.id,
                contract=contract,
                status=BenchmarkStatus.IN_PROGRESS,
            )
            postgres_session.add(benchmark)
            postgres_session.flush()
            for task_index, error_message in enumerate(error_messages):
                task = make_task(benchmark, f"task-{task_index}", status=TaskStatus.ERROR)
                postgres_session.add(task)
                postgres_session.flush()
                postgres_session.add(ErrorResult(org_id=org.id, task=task.id, error_message=error_message))
            benchmarks.append((benchmark, expected_summary))
        postgres_session.commit()

        async def skip_cloud_operation(*_args: Any, **_kwargs: Any) -> None:
            return None

        def skip_log_group(*_args: Any, **_kwargs: Any) -> str:
            return "test-log-group"

        def provider_config(*_args: Any, **_kwargs: Any) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://example.com",
                DAYTONA_TARGET="test-target",
            )

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "copy_agent_to_benchmark", skip_cloud_operation)
        monkeypatch.setattr(run_orchestration_module, "create_benchmark_log_group", skip_log_group)
        monkeypatch.setattr(run_orchestration_module, "fetch_sandbox_provider_config", provider_config)

        for benchmark, expected_summary in benchmarks:
            request = StartBenchmarkRequest(
                contract=contract,
                benchmark_name=benchmark.name,
                concurrency=1,
                harness_config=harness_config,
            )
            await process_benchmark(request.model_dump(), str(benchmark.id), [])

            with Session(postgres_engine) as assertion_session:
                persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)

            assert persisted_benchmark is not None
            assert persisted_benchmark.status == BenchmarkStatus.ERROR
            assert persisted_benchmark.error_message == expected_summary
