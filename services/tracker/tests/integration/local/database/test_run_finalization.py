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
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark


class TestRunFinalization:
    """Run finalization and concurrent retry behavior."""

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
