"""Run with `uv run pytest tests/integration/local/database/test_run_finalization.py`.

Exercise run-finalization concurrency against disposable Postgres.
"""

import asyncio
from datetime import UTC, datetime
from threading import Event, Thread
from time import monotonic, sleep
from typing import Any
from uuid import UUID, uuid4

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.sandbox import DaytonaProviderConfig
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

import tracker.utils.run_control as run_control_module
import tracker.utils.run_orchestration as run_orchestration_module
from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    FinalEvaluation,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.dispatch_control import admit_recovery_dispatch, terminalize_active_dispatches
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.release_control import promote_release
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import initiate_stop_benchmark, process_benchmark, reset_to_in_progress_status
from tracker.utils.reporting import create_final_view
from tracker.utils.resources import fetch_benchmark_row
from tracker.utils.run_orchestration import upload_final_view_if_current
from tracker.utils.task_error_summary import summarize_task_errors
from tracker.utils.task_execution import TaskMonitor


class TestRunFinalization:
    """Run finalization and concurrent retry behavior."""

    async def test_all_error_finalization_honors_concurrent_status_changes(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority_kwargs: Any,
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

        authorities = [
            ExecutionAuthority(
                benchmark_id=benchmark.id,
                dispatch_id=UUID(
                    executor_authority_kwargs(benchmark, session=postgres_session)["executor_dispatch_id"]
                ),
            )
            for benchmark in benchmarks
        ]
        deferred_results = [
            await run_orchestration_module.finalize_all_error_run(
                benchmark.id,
                org,
                authority=authority,
            )
            for benchmark, authority in zip(benchmarks, authorities, strict=True)
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

    async def test_retry_before_final_view_upload_skips_stale_publication(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        org = Org(id=uuid4(), name=f"final-view-race-{uuid4()}")
        benchmark = make_benchmark(
            name="final-view-race",
            org_id=org.id,
            contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
            status=BenchmarkStatus.FINISHED,
        )
        postgres_session.add(org)
        postgres_session.flush()
        postgres_session.add(benchmark)
        postgres_session.commit()

        authority_kwargs = executor_authority_kwargs(benchmark, session=postgres_session)
        authority = ExecutionAuthority(
            benchmark_id=benchmark.id,
            dispatch_id=UUID(str(authority_kwargs["executor_dispatch_id"])),
        )
        postgres_session.refresh(benchmark)
        final_view = create_final_view(benchmark, postgres_session, org)

        stale_dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
        assert stale_dispatch is not None
        stale_dispatch.status = ExecutorDispatchStatus.FAILED
        stale_dispatch.finished_at = datetime.now(UTC)
        retry_dispatch = ExecutorDispatch(
            benchmark_id=benchmark.id,
            kind=ExecutorDispatchKind.RETRY,
            status=ExecutorDispatchStatus.RUNNING,
            executor_release_id=stale_dispatch.executor_release_id,
            executor_artifact_uri=stale_dispatch.executor_artifact_uri,
            executor_artifact_digest=stale_dispatch.executor_artifact_digest,
            executor_protocol_version=stale_dispatch.executor_protocol_version,
            started_at=datetime.now(UTC),
        )
        postgres_session.add(stale_dispatch)
        postgres_session.add(retry_dispatch)
        postgres_session.commit()

        upload_calls: list[UUID] = []

        async def record_upload(*_args: Any, **_kwargs: Any) -> None:
            upload_calls.append(benchmark.id)

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "upload_final_view", record_upload)

        with pytest.raises(ExecutionAuthorityRevoked):
            await upload_final_view_if_current(
                benchmark,
                final_view,
                harness_config,
                authority,
            )

        assert upload_calls == []
        postgres_session.refresh(retry_dispatch)
        assert retry_dispatch.status == ExecutorDispatchStatus.RUNNING

    async def test_concurrent_retry_prevents_stale_final_evaluation(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority_kwargs: Any,
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
        task = make_task(benchmark, "retried-task", status=TaskStatus.ERROR)
        scored_task = make_task(benchmark, "scored-task", status=TaskStatus.FINISHED)

        postgres_session.add(org)
        postgres_session.flush()
        postgres_session.add(benchmark)
        postgres_session.flush()
        postgres_session.add_all([task, scored_task])
        postgres_session.flush()
        postgres_session.add(
            EvaluationResult(
                org_id=org.id,
                task=scored_task.id,
                instance_id=f"race-{scored_task.id}",
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

        retry_dispatch_ids: list[UUID] = []

        async def verify_retry_task(
            _client: BenchmarkServiceClient,
            *,
            task_ids: list[str],
            slice_str: str | None,
            dataset: str | None,
        ) -> VerifyTaskIdsResponse:
            assert slice_str is None
            assert dataset == benchmark.arguments.dataset
            return VerifyTaskIdsResponse(task_ids=task_ids)

        async def stale_final_score(*_args: Any, **_kwargs: Any) -> FinalScoreResponse:
            with Session(postgres_engine) as retry_session:
                retry_benchmark = retry_session.get(Benchmark, benchmark.id)
                assert retry_benchmark is not None
                pre_action_status = retry_benchmark.status
                benchmark_service = retry_benchmark.benchmark_service()
                try:
                    verified_task_ids = await reset_to_in_progress_status(
                        retry_benchmark,
                        retry_session,
                        benchmark_service,
                        retry=True,
                        retry_mode=RetryMode.FROM_SCRATCH,
                        rerun_task_ids=[task.task_id],
                        org=org,
                    )
                finally:
                    await benchmark_service.close()
                assert verified_task_ids == [task.task_id]
                dispatch = admit_recovery_dispatch(
                    retry_session,
                    benchmark=retry_benchmark,
                    pre_action_status=pre_action_status,
                    dispatch_id=uuid4(),
                    kind=ExecutorDispatchKind.RETRY,
                )
                retry_session.commit()
                retry_dispatch_ids.append(dispatch.id)
            return FinalScoreResponse(tasks_evaluated=[scored_task.task_id], final_score=0.25, metadata={})

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "create_benchmark_log_group", skip_log_group)
        monkeypatch.setattr(run_orchestration_module, "fetch_sandbox_provider_config", provider_config)
        monkeypatch.setattr(run_orchestration_module, "upload_final_view", skip_cloud_operation)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_retry_task)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", stale_final_score)

        authority_kwargs = executor_authority_kwargs(benchmark, session=postgres_session)
        assert benchmark.current_execution_release_id is not None
        promote_release(postgres_session, benchmark.current_execution_release_id)
        postgres_session.commit()

        await process_benchmark(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark.id),
            verified_task_ids=[],
            **authority_kwargs,
        )

        with Session(postgres_engine) as assertion_session:
            persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)
            persisted_task = assertion_session.get(Task, task.id)
            final_evaluation = assertion_session.exec(
                select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)
            ).first()
            assert len(retry_dispatch_ids) == 1, persisted_benchmark.error_message if persisted_benchmark else None
            retry_dispatch = assertion_session.get(ExecutorDispatch, retry_dispatch_ids[0])

        assert persisted_benchmark is not None
        assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
        assert persisted_task is not None
        assert persisted_task.status == TaskStatus.PENDING
        assert retry_dispatch is not None
        assert retry_dispatch.status == ExecutorDispatchStatus.QUEUED
        assert final_evaluation is None

    async def test_only_one_additive_dispatch_calls_final_score(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        org = Org(id=uuid4(), name=f"finalizer-election-{uuid4()}")
        contract = AgentContractRequest(name="finalizer-agent", install_cmd="true", run_cmd="true")
        benchmark = make_benchmark(
            name="finalizer-election",
            org_id=org.id,
            contract=contract,
            status=BenchmarkStatus.IN_PROGRESS,
        )
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
                instance_id=f"finalizer-{task.id}",
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
        first_authority = executor_authority_kwargs(benchmark, dispatch_id=uuid4(), session=postgres_session)
        second_authority = executor_authority_kwargs(benchmark, dispatch_id=uuid4(), session=postgres_session)

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

        final_score_calls = 0

        async def final_score(*_args: Any, **_kwargs: Any) -> FinalScoreResponse:
            nonlocal final_score_calls
            final_score_calls += 1
            return FinalScoreResponse(tasks_evaluated=[task.task_id], final_score=1.0, metadata={})

        original_track_tasks = TaskMonitor.track_tasks
        monitors_arrived = 0
        both_monitors_arrived = asyncio.Event()

        async def synchronized_track_tasks(monitor: TaskMonitor) -> None:
            nonlocal monitors_arrived
            await original_track_tasks(monitor)
            monitors_arrived += 1
            if monitors_arrived == 2:
                both_monitors_arrived.set()
            await both_monitors_arrived.wait()

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)
        monkeypatch.setattr(run_orchestration_module, "create_benchmark_log_group", skip_log_group)
        monkeypatch.setattr(run_orchestration_module, "fetch_sandbox_provider_config", provider_config)
        monkeypatch.setattr(run_orchestration_module, "upload_final_view", skip_cloud_operation)
        monkeypatch.setattr(TaskMonitor, "track_tasks", synchronized_track_tasks)
        monkeypatch.setattr(BenchmarkServiceClient, "final_score", final_score)

        await asyncio.gather(
            process_benchmark(
                start_benchmark_request_json=request.model_dump(),
                benchmark_id_str=str(benchmark.id),
                verified_task_ids=[],
                **first_authority,
            ),
            process_benchmark(
                start_benchmark_request_json=request.model_dump(),
                benchmark_id_str=str(benchmark.id),
                verified_task_ids=[],
                **second_authority,
            ),
        )

        with Session(postgres_engine) as assertion_session:
            persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)
            final_evaluations = assertion_session.exec(
                select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)
            ).all()

        assert final_score_calls == 1
        assert persisted_benchmark is not None
        assert persisted_benchmark.status == BenchmarkStatus.FINISHED
        assert len(final_evaluations) == 1

    async def test_all_error_finalization_returns_distinct_representatives(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority_kwargs: Any,
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

        def skip_log_group(*_args: Any, **_kwargs: Any) -> str:
            return "test-log-group"

        def provider_config(*_args: Any, **_kwargs: Any) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://example.com",
                DAYTONA_TARGET="test-target",
            )

        monkeypatch.setattr(run_orchestration_module, "engine", postgres_engine)

        monkeypatch.setattr(run_orchestration_module, "create_benchmark_log_group", skip_log_group)
        monkeypatch.setattr(run_orchestration_module, "fetch_sandbox_provider_config", provider_config)

        for benchmark, expected_summary in benchmarks:
            request = StartBenchmarkRequest(
                contract=contract,
                benchmark_name=benchmark.name,
                concurrency=1,
                harness_config=harness_config,
            )
            authority_kwargs = executor_authority_kwargs(benchmark, session=postgres_session)
            await process_benchmark(request.model_dump(), str(benchmark.id), [], **authority_kwargs)

            with Session(postgres_engine) as assertion_session:
                persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)

            assert persisted_benchmark is not None
            assert persisted_benchmark.status == BenchmarkStatus.ERROR
            assert persisted_benchmark.error_message == expected_summary

    async def test_force_stop_terminalization_serializes_with_recovery_dispatch(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority_kwargs: Any,
    ) -> None:
        org = Org(id=uuid4(), name=f"force-stop-race-{uuid4()}")
        benchmark = make_benchmark(
            name="force-stop-race",
            org_id=org.id,
            contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
            status=BenchmarkStatus.IN_PROGRESS,
        )
        postgres_session.add(org)
        postgres_session.flush()
        postgres_session.add(benchmark)
        postgres_session.flush()
        task = make_task(benchmark, "task-0", status=TaskStatus.IN_PROGRESS)
        postgres_session.add(task)
        postgres_session.commit()
        authority_kwargs = executor_authority_kwargs(benchmark, session=postgres_session)
        old_dispatch_id = UUID(str(authority_kwargs["executor_dispatch_id"]))
        assert benchmark.current_execution_release_id is not None
        promote_release(postgres_session, benchmark.current_execution_release_id)
        postgres_session.commit()

        retry_ready = Event()
        allow_retry_lock = Event()
        retry_errors: list[BaseException] = []
        retry_dispatch_ids: list[UUID] = []
        retry_backend_pids: list[int] = []

        def admit_retry() -> None:
            try:
                with Session(postgres_engine) as retry_session:
                    backend_pid = retry_session.connection().exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
                    retry_backend_pids.append(int(backend_pid))
                    retry_ready.set()
                    assert allow_retry_lock.wait(timeout=2)
                    retry_benchmark = fetch_benchmark_row(
                        benchmark.id,
                        retry_session,
                        org,
                        for_update=True,
                    )
                    pre_action_status = retry_benchmark.status
                    retry_task = retry_session.get(Task, task.id)
                    assert retry_task is not None
                    retry_task.status = TaskStatus.PENDING
                    retry_task.finished_at = None
                    retry_benchmark.status = BenchmarkStatus.IN_PROGRESS
                    retry_session.add(retry_task)
                    dispatch = admit_recovery_dispatch(
                        retry_session,
                        benchmark=retry_benchmark,
                        pre_action_status=pre_action_status,
                        dispatch_id=uuid4(),
                        kind=ExecutorDispatchKind.RESUME,
                    )
                    retry_session.commit()
                    retry_dispatch_ids.append(dispatch.id)
            except BaseException as exc:
                retry_errors.append(exc)

        original_terminalize = terminalize_active_dispatches
        retry_thread: Thread | None = None

        def terminalize_with_concurrent_retry(*args: Any, **kwargs: Any) -> None:
            nonlocal retry_thread
            retry_thread = Thread(target=admit_retry)
            retry_thread.start()
            assert retry_ready.wait(timeout=2)
            assert len(retry_backend_pids) == 1
            allow_retry_lock.set()

            force_session = args[0]
            deadline = monotonic() + 2
            blocking_pids: list[int] = []
            while monotonic() < deadline:
                blocking_pid_row = force_session.connection().exec_driver_sql(
                    "SELECT pg_blocking_pids(%s)",
                    (retry_backend_pids[0],),
                )
                blocking_pids = list(blocking_pid_row.scalar_one())
                if blocking_pids:
                    break
                sleep(0.01)
            assert blocking_pids
            original_terminalize(*args, **kwargs)

        monkeypatch.setattr(run_control_module, "terminalize_active_dispatches", terminalize_with_concurrent_retry)

        try:
            await initiate_stop_benchmark(
                benchmark,
                postgres_session,
                force=True,
                org=org,
            )
        finally:
            allow_retry_lock.set()
            try:
                if postgres_session.in_transaction():
                    postgres_session.rollback()
            except BaseException:
                postgres_session.close()
                raise
            finally:
                if retry_thread is not None:
                    await asyncio.to_thread(retry_thread.join)

        assert retry_thread is not None
        assert not retry_thread.is_alive()
        assert retry_errors == []
        assert len(retry_dispatch_ids) == 1
        with Session(postgres_engine) as assertion_session:
            persisted_benchmark = assertion_session.get(Benchmark, benchmark.id)
            old_dispatch = assertion_session.get(ExecutorDispatch, old_dispatch_id)
            retry_dispatch = assertion_session.get(ExecutorDispatch, retry_dispatch_ids[0])
            persisted_task = assertion_session.get(Task, task.id)
        assert persisted_benchmark is not None
        assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
        assert old_dispatch is not None
        assert old_dispatch.status == ExecutorDispatchStatus.FAILED
        assert retry_dispatch is not None
        assert retry_dispatch.status == ExecutorDispatchStatus.QUEUED
        assert persisted_task is not None
        assert persisted_task.status == TaskStatus.PENDING
