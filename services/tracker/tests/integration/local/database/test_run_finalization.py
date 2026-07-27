"""Run with `uv run pytest tests/integration/local/database/test_run_finalization.py`.

Exercise run-finalization concurrency against disposable Postgres.
"""

import asyncio
from datetime import timedelta
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
from tracker.types import AWSCredentials, HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark
from tracker.utils.task_error_summary import summarize_task_errors


async def _skip_cloud_operation(*_args: Any, **_kwargs: Any) -> None:
    return None


def _skip_log_group(*_args: Any, **_kwargs: Any) -> str:
    return "test-log-group"


def _provider_config(*_args: Any, **_kwargs: Any) -> DaytonaProviderConfig:
    return DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://example.com",
        DAYTONA_TARGET="test-target",
    )


def _patch_process_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    postgres_engine: Engine,
    *,
    upload: Any = _skip_cloud_operation,
) -> None:
    monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
    monkeypatch.setattr(run_orchestration_module, "copy_agent_to_benchmark", _skip_cloud_operation)
    monkeypatch.setattr(run_orchestration_module, "create_benchmark_log_group", _skip_log_group)
    monkeypatch.setattr(run_orchestration_module, "fetch_sandbox_provider_config", _provider_config)
    monkeypatch.setattr(run_orchestration_module, "upload_final_view", upload)


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
        - A resumed evaluation that finishes leaves the run in progress for fresh finalization.
        - A concurrent stop marks the run stopped without committing an error summary.
        """
        org = Org(id=uuid4(), name=f"error-finalization-race-{uuid4()}")
        contract = AgentContractRequest(name="error-race-agent", install_cmd="true", run_cmd="true")
        target_statuses = {
            "retry-during-summary": TaskStatus.PENDING,
            "finish-during-summary": TaskStatus.FINISHED,
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
                if task.status == TaskStatus.FINISHED:
                    transition_session.add(
                        EvaluationResult(
                            org_id=org.id,
                            task=task.id,
                            instance_id=f"summary-race-{task.id}",
                            result={"score": 1.0},
                        )
                    )
                transition_session.commit()

            return summarize_task_errors(task_errors)

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "summarize_task_errors", change_status_during_summary)

        deferred_results = [
            await run_orchestration_module.finalize_all_error_run(benchmark.id, org) for benchmark in benchmarks
        ]

        with Session(postgres_engine) as assertion_session:
            persisted_benchmarks = [assertion_session.get(Benchmark, benchmark.id) for benchmark in benchmarks]

        assert deferred_results == [True, True, False]
        assert all(benchmark is not None for benchmark in persisted_benchmarks)
        assert [benchmark.status for benchmark in persisted_benchmarks if benchmark] == [
            BenchmarkStatus.IN_PROGRESS,
            BenchmarkStatus.IN_PROGRESS,
            BenchmarkStatus.STOPPED,
        ]
        assert all(benchmark.error_message is None for benchmark in persisted_benchmarks if benchmark)

    @pytest.mark.parametrize("final_score_fails", [False, True])
    async def test_concurrent_retry_prevents_stale_final_evaluation(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
        final_score_fails: bool,
    ) -> None:
        """A retry finishing during finalization must leave the run active without a stale score.

        Test cases:
        - A task advances attempt and writes a new result while final_score awaits.
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

        async def stale_final_score(*_args: Any, **_kwargs: Any) -> FinalScoreResponse:
            with Session(postgres_engine) as retry_session:
                retried_task = retry_session.exec(select(Task).where(Task.id == task.id)).one()
                retried_task.started_at += timedelta(microseconds=1)
                retried_task.status = TaskStatus.FINISHED
                retry_session.add(retried_task)
                retry_session.add(
                    EvaluationResult(
                        org_id=org.id,
                        task=task.id,
                        instance_id=f"retry-{task.id}",
                        result={"score": 0.5},
                    )
                )
                retry_session.commit()
            if final_score_fails:
                raise RuntimeError("stale final score failed")
            return FinalScoreResponse(tasks_evaluated=[task.task_id], final_score=0.25, metadata={})

        _patch_process_dependencies(monkeypatch, postgres_engine)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", stale_final_score)

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
        assert persisted_task.status == TaskStatus.FINISHED
        assert final_evaluation is None

    async def test_overlapping_coordinators_publish_terminal_outcome_once(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        org = Org(id=uuid4(), name=f"overlapping-finalization-{uuid4()}")
        contract = AgentContractRequest(name="overlap-agent", install_cmd="true", run_cmd="true")
        benchmark = make_benchmark(
            name="overlapping-finalization",
            org_id=org.id,
            contract=contract,
            status=BenchmarkStatus.IN_PROGRESS,
        )
        benchmark.arguments = benchmark.arguments.model_copy(update={"lambda_function": "finalizer"})
        task = make_task(benchmark, "finished-task", status=TaskStatus.FINISHED)

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
                instance_id=f"overlap-{task.id}",
                result={"score": 1.0},
            )
        )
        postgres_session.commit()

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name=benchmark.name,
            concurrency=1,
            lambda_function="finalizer",
            webhook_secret_name="terminal-webhook",
            webhook_intervals=[50],
            harness_config=harness_config,
        )
        score_barrier = asyncio.Event()
        score_calls = 0
        uploaded_benchmark_ids: list[str] = []
        lambda_payloads: list[dict[str, Any]] = []
        webhook_messages: list[str] = []

        async def synchronized_final_score(*_args: Any, **_kwargs: Any) -> FinalScoreResponse:
            nonlocal score_calls
            score_calls += 1
            if score_calls == 2:
                score_barrier.set()
            await score_barrier.wait()

            return FinalScoreResponse(tasks_evaluated=[task.task_id], final_score=0.75, metadata={})

        async def record_upload(benchmark_row: Benchmark, *_args: Any, **_kwargs: Any) -> None:
            uploaded_benchmark_ids.append(str(benchmark_row.id))

        def record_lambda(_client: object, _function_name: str, payload: dict[str, Any]) -> None:
            lambda_payloads.append(payload)

        def lambda_client(_aws: AWSCredentials) -> object:
            return object()

        async def record_webhook(_notifier: object, message: str) -> None:
            webhook_messages.append(message)

        _patch_process_dependencies(monkeypatch, postgres_engine, upload=record_upload)
        monkeypatch.setattr(run_orchestration_module, "lambda_client", lambda_client)
        monkeypatch.setattr(run_orchestration_module, "invoke_lambda", record_lambda)
        monkeypatch.setattr(run_orchestration_module.SlackNotifier, "_send_webhook", record_webhook)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", synchronized_final_score)

        await asyncio.gather(
            process_benchmark(request.model_dump(), str(benchmark.id), []),
            process_benchmark(request.model_dump(), str(benchmark.id), []),
        )

        with Session(postgres_engine) as assertion_session:
            persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)
            final_evaluations = assertion_session.exec(
                select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)
            ).all()

        assert persisted_benchmark is not None
        assert persisted_benchmark.status == BenchmarkStatus.FINISHED
        assert len(final_evaluations) == 1
        assert uploaded_benchmark_ids == [str(benchmark.id)]
        assert [payload["benchmark_id"] for payload in lambda_payloads] == [str(benchmark.id)]
        assert len(webhook_messages) == 1

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

        _patch_process_dependencies(monkeypatch, postgres_engine)

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
