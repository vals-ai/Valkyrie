"""ExecutorHost dispatch-store integration against disposable PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from services.executor_host.supervisor import (  # pyright: ignore[reportMissingImports]
    ArtifactDispatch,
    PostgresExecutorDispatchStore,
)
from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    FailureCategory,
    FailureClassificationState,
    FailureRecord,
    FailureTerminalEffect,
    Org,
    Task,
    TaskAttempt,
    TaskAttemptOutcome,
    TaskStatus,
)
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release


def _persist_run(
    session: Session,
    *,
    task_ids: list[str],
    dispatch_kinds: list[ExecutorDispatchKind],
) -> tuple[Org, Benchmark, ExecutorRelease, list[Task], list[ExecutorDispatch]]:
    org = Org(id=uuid4(), name=f"executor-host-store-{uuid4()}")
    benchmark = make_benchmark(
        name="executor-host-store",
        org_id=org.id,
        contract=AgentContractRequest(name="store-agent", install_cmd="true", run_cmd="true"),
        status=BenchmarkStatus.IN_PROGRESS,
    )
    release = ExecutorRelease(
        id=f"executor-host-store-release-{uuid4()}",
        artifact_uri="s3://artifacts/executor-host-store.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )

    session.add(org)
    session.flush()
    register_release(session, release)
    pin_benchmark_to_release(benchmark, release)
    session.add(benchmark)
    session.flush()

    dispatches = [
        create_executor_dispatch(
            benchmark.id,
            release,
            kind,
            dispatch_id=uuid4(),
        )
        for kind in dispatch_kinds
    ]
    session.add_all(dispatches)
    session.flush()
    tasks = [make_task(benchmark, task_id, status=TaskStatus.IN_PROGRESS) for task_id in task_ids]
    session.add_all(tasks)
    session.flush()
    return org, benchmark, release, tasks, dispatches


def _attach_attempt(
    session: Session,
    *,
    task: Task,
    dispatch: ExecutorDispatch | None,
) -> TaskAttempt:
    attempt = TaskAttempt(
        org_id=task.org_id,
        task=task.id,
        dispatch_id=dispatch.id if dispatch is not None else None,
        started_at=task.started_at,
    )
    session.add(attempt)
    session.flush()
    task.active_attempt_id = attempt.id
    session.add(task)
    return attempt


def _store(engine: Engine) -> PostgresExecutorDispatchStore:
    url = engine.url
    assert url.host is not None
    assert url.port is not None
    assert url.database is not None
    assert url.username is not None
    assert url.password is not None
    return PostgresExecutorDispatchStore(
        host=url.host,
        port=str(url.port),
        dbname=url.database,
        user=url.username,
        password=url.password,
    )


def _artifact(release: ExecutorRelease) -> ArtifactDispatch:
    return ArtifactDispatch.from_payload(
        {
            "executor_release_id": release.id,
            "executor_artifact_uri": release.artifact_uri,
            "executor_artifact_digest": release.artifact_digest,
            "executor_protocol_version": release.protocol_version,
        }
    )


@pytest.mark.asyncio
async def test_terminalize_records_owned_and_legacy_task_failures(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    _, benchmark, release, tasks, dispatches = _persist_run(
        postgres_session,
        task_ids=["owned-task", "legacy-attempt-task", "legacy-task", "unbound-newer-task"],
        dispatch_kinds=[ExecutorDispatchKind.START],
    )
    owned_task, legacy_attempt_task, legacy_task, unbound_newer_task = tasks
    dispatch = dispatches[0]
    post_dispatch_started_at = dispatch.created_at + timedelta(seconds=1)
    owned_task.started_at = post_dispatch_started_at
    unbound_newer_task.started_at = post_dispatch_started_at
    legacy_started_at = dispatch.created_at - timedelta(seconds=1)
    legacy_attempt_task.started_at = legacy_started_at
    legacy_task.started_at = legacy_started_at
    owned_attempt = _attach_attempt(postgres_session, task=owned_task, dispatch=dispatch)
    legacy_attempt = _attach_attempt(postgres_session, task=legacy_attempt_task, dispatch=None)
    unbound_newer_attempt = _attach_attempt(postgres_session, task=unbound_newer_task, dispatch=None)
    postgres_session.commit()

    store = _store(postgres_engine)
    authority = await store.claim(str(dispatch.id), str(benchmark.id), _artifact(release))
    assert authority is not None
    assert await store.terminalize(
        authority,
        [
            owned_task.task_id,
            legacy_attempt_task.task_id,
            legacy_task.task_id,
            unbound_newer_task.task_id,
        ],
    )

    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(Benchmark, benchmark.id)
    persisted_dispatch = postgres_session.get(ExecutorDispatch, dispatch.id)
    persisted_owned_task = postgres_session.get(Task, owned_task.id)
    persisted_legacy_attempt_task = postgres_session.get(Task, legacy_attempt_task.id)
    persisted_legacy_task = postgres_session.get(Task, legacy_task.id)
    persisted_unbound_newer_task = postgres_session.get(Task, unbound_newer_task.id)
    persisted_owned_attempt = postgres_session.get(TaskAttempt, owned_attempt.id)
    persisted_legacy_attempt = postgres_session.get(TaskAttempt, legacy_attempt.id)
    persisted_unbound_newer_attempt = postgres_session.get(TaskAttempt, unbound_newer_attempt.id)
    failures = postgres_session.exec(select(FailureRecord).where(FailureRecord.benchmark_id == benchmark.id)).all()

    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.ERROR
    assert persisted_dispatch is not None
    assert persisted_dispatch.status == ExecutorDispatchStatus.FAILED
    assert persisted_owned_task is not None
    assert persisted_owned_task.status == TaskStatus.ERROR
    assert persisted_legacy_attempt_task is not None
    assert persisted_legacy_attempt_task.status == TaskStatus.ERROR
    assert persisted_legacy_task is not None
    assert persisted_legacy_task.status == TaskStatus.ERROR
    assert persisted_unbound_newer_task is not None
    assert persisted_unbound_newer_task.status == TaskStatus.IN_PROGRESS
    assert persisted_owned_attempt is not None
    assert persisted_owned_attempt.outcome == TaskAttemptOutcome.ERROR
    assert persisted_owned_attempt.finished_at is not None
    assert persisted_legacy_attempt is not None
    assert persisted_legacy_attempt.outcome == TaskAttemptOutcome.ERROR
    assert persisted_legacy_attempt.finished_at is not None
    assert persisted_unbound_newer_attempt is not None
    assert persisted_unbound_newer_attempt.outcome == TaskAttemptOutcome.PENDING
    assert persisted_unbound_newer_attempt.finished_at is None

    task_failures = {failure.task: failure for failure in failures if failure.task is not None}
    run_failures = [failure for failure in failures if failure.task is None]
    assert set(task_failures) == {owned_task.id, legacy_attempt_task.id, legacy_task.id}
    assert task_failures[owned_task.id].task_attempt_id == owned_attempt.id
    assert task_failures[legacy_attempt_task.id].task_attempt_id == legacy_attempt.id
    assert task_failures[legacy_task.id].task_attempt_id is None
    assert unbound_newer_task.id not in task_failures
    assert len(run_failures) == 1
    for failure in failures:
        assert failure.dispatch_id == dispatch.id
        assert failure.category == FailureCategory.VALKYRIE
        assert failure.producer == "executor_host"
        assert failure.operation == "run_executor_dispatch"
        assert failure.error_type == "ExecutorHostFailure"
        assert failure.error_message == "Executor host failed"
        assert failure.classification_state == FailureClassificationState.UNCLASSIFIED
        assert failure.cause_code is None
        assert failure.terminal_effect == FailureTerminalEffect.TERMINAL


@pytest.mark.asyncio
async def test_terminalize_preserves_newer_dispatch_work(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    _, benchmark, release, tasks, dispatches = _persist_run(
        postgres_session,
        task_ids=["failed-task", "newer-task"],
        dispatch_kinds=[ExecutorDispatchKind.START, ExecutorDispatchKind.RETRY],
    )
    failed_task, newer_task = tasks
    failed_dispatch, newer_dispatch = dispatches
    failed_attempt = _attach_attempt(postgres_session, task=failed_task, dispatch=failed_dispatch)
    newer_attempt = _attach_attempt(postgres_session, task=newer_task, dispatch=newer_dispatch)
    postgres_session.commit()

    store = _store(postgres_engine)
    artifact = _artifact(release)
    failed_authority = await store.claim(str(failed_dispatch.id), str(benchmark.id), artifact)
    newer_authority = await store.claim(str(newer_dispatch.id), str(benchmark.id), artifact)
    assert failed_authority is not None
    assert newer_authority is not None
    assert await store.terminalize(failed_authority, [failed_task.task_id, newer_task.task_id])

    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(Benchmark, benchmark.id)
    persisted_failed_dispatch = postgres_session.get(ExecutorDispatch, failed_dispatch.id)
    persisted_newer_dispatch = postgres_session.get(ExecutorDispatch, newer_dispatch.id)
    persisted_failed_task = postgres_session.get(Task, failed_task.id)
    persisted_newer_task = postgres_session.get(Task, newer_task.id)
    persisted_failed_attempt = postgres_session.get(TaskAttempt, failed_attempt.id)
    persisted_newer_attempt = postgres_session.get(TaskAttempt, newer_attempt.id)
    failures = postgres_session.exec(select(FailureRecord).where(FailureRecord.benchmark_id == benchmark.id)).all()

    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert persisted_failed_dispatch is not None
    assert persisted_failed_dispatch.status == ExecutorDispatchStatus.FAILED
    assert persisted_newer_dispatch is not None
    assert persisted_newer_dispatch.status == ExecutorDispatchStatus.RUNNING
    assert persisted_failed_task is not None
    assert persisted_failed_task.status == TaskStatus.ERROR
    assert persisted_newer_task is not None
    assert persisted_newer_task.status == TaskStatus.IN_PROGRESS
    assert persisted_failed_attempt is not None
    assert persisted_failed_attempt.outcome == TaskAttemptOutcome.ERROR
    assert persisted_newer_attempt is not None
    assert persisted_newer_attempt.outcome == TaskAttemptOutcome.PENDING
    assert len(failures) == 1
    assert failures[0].task == failed_task.id
    assert failures[0].task_attempt_id == failed_attempt.id
    assert all(failure.task_attempt_id != newer_attempt.id for failure in failures)


@pytest.mark.asyncio
async def test_finish_records_missing_finalization_and_owned_attempts(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    _, benchmark, release, tasks, dispatches = _persist_run(
        postgres_session,
        task_ids=["unfinished-task"],
        dispatch_kinds=[ExecutorDispatchKind.START],
    )
    task = tasks[0]
    dispatch = dispatches[0]
    attempt = _attach_attempt(postgres_session, task=task, dispatch=dispatch)
    postgres_session.commit()

    store = _store(postgres_engine)
    authority = await store.claim(str(dispatch.id), str(benchmark.id), _artifact(release))
    assert authority is not None
    assert await store.finish(authority, [task.task_id])

    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(Benchmark, benchmark.id)
    persisted_dispatch = postgres_session.get(ExecutorDispatch, dispatch.id)
    persisted_task = postgres_session.get(Task, task.id)
    persisted_attempt = postgres_session.get(TaskAttempt, attempt.id)
    failures = postgres_session.exec(select(FailureRecord).where(FailureRecord.benchmark_id == benchmark.id)).all()

    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.ERROR
    assert persisted_dispatch is not None
    assert persisted_dispatch.status == ExecutorDispatchStatus.FINISHED
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.ERROR
    assert persisted_attempt is not None
    assert persisted_attempt.outcome == TaskAttemptOutcome.ERROR
    assert persisted_attempt.finished_at is not None

    assert len(failures) == 2
    task_failure = next(failure for failure in failures if failure.task == task.id)
    run_failure = next(failure for failure in failures if failure.task is None)
    assert task_failure.task_attempt_id == attempt.id
    for failure in (task_failure, run_failure):
        assert failure.dispatch_id == dispatch.id
        assert failure.category == FailureCategory.VALKYRIE
        assert failure.producer == "executor_host"
        assert failure.operation == "finish_dispatch"
        assert failure.error_type == "ExecutorExitedWithoutFinalization"
        assert failure.error_message == "Executor exited without finalizing benchmark"
        assert failure.classification_state == FailureClassificationState.CLASSIFIED
        assert failure.cause_code == "executor_exited_without_finalization"
        assert failure.terminal_effect == FailureTerminalEffect.TERMINAL


@pytest.mark.asyncio
async def test_finish_terminalizes_owned_work_with_newer_dispatch_active(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    _, benchmark, release, tasks, dispatches = _persist_run(
        postgres_session,
        task_ids=["unfinished-task", "newer-task"],
        dispatch_kinds=[ExecutorDispatchKind.START, ExecutorDispatchKind.RETRY],
    )
    unfinished_task, newer_task = tasks
    finished_dispatch, newer_dispatch = dispatches
    unfinished_task.started_at = finished_dispatch.created_at + timedelta(seconds=1)
    newer_task.started_at = newer_dispatch.created_at + timedelta(seconds=1)
    unfinished_attempt = _attach_attempt(
        postgres_session,
        task=unfinished_task,
        dispatch=finished_dispatch,
    )
    newer_attempt = _attach_attempt(
        postgres_session,
        task=newer_task,
        dispatch=newer_dispatch,
    )
    postgres_session.commit()

    store = _store(postgres_engine)
    artifact = _artifact(release)
    finished_authority = await store.claim(
        str(finished_dispatch.id),
        str(benchmark.id),
        artifact,
    )
    newer_authority = await store.claim(
        str(newer_dispatch.id),
        str(benchmark.id),
        artifact,
    )
    assert finished_authority is not None
    assert newer_authority is not None
    assert await store.finish(finished_authority, [unfinished_task.task_id, newer_task.task_id])

    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(Benchmark, benchmark.id)
    persisted_finished_dispatch = postgres_session.get(ExecutorDispatch, finished_dispatch.id)
    persisted_newer_dispatch = postgres_session.get(ExecutorDispatch, newer_dispatch.id)
    persisted_unfinished_task = postgres_session.get(Task, unfinished_task.id)
    persisted_newer_task = postgres_session.get(Task, newer_task.id)
    persisted_unfinished_attempt = postgres_session.get(TaskAttempt, unfinished_attempt.id)
    persisted_newer_attempt = postgres_session.get(TaskAttempt, newer_attempt.id)
    failures = postgres_session.exec(select(FailureRecord).where(FailureRecord.benchmark_id == benchmark.id)).all()

    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert persisted_finished_dispatch is not None
    assert persisted_finished_dispatch.status == ExecutorDispatchStatus.FINISHED
    assert persisted_newer_dispatch is not None
    assert persisted_newer_dispatch.status == ExecutorDispatchStatus.RUNNING
    assert persisted_unfinished_task is not None
    assert persisted_unfinished_task.status == TaskStatus.ERROR
    assert persisted_newer_task is not None
    assert persisted_newer_task.status == TaskStatus.IN_PROGRESS
    assert persisted_unfinished_attempt is not None
    assert persisted_unfinished_attempt.outcome == TaskAttemptOutcome.ERROR
    assert persisted_unfinished_attempt.finished_at is not None
    assert persisted_newer_attempt is not None
    assert persisted_newer_attempt.outcome == TaskAttemptOutcome.PENDING

    assert len(failures) == 1
    assert failures[0].task == unfinished_task.id
    assert failures[0].task_attempt_id == unfinished_attempt.id
    assert failures[0].dispatch_id == finished_dispatch.id
    assert failures[0].operation == "finish_dispatch"
    assert failures[0].cause_code == "executor_exited_without_finalization"
