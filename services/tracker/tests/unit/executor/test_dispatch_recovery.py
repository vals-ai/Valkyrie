"""Run with `uv run pytest tests/unit/executor/test_dispatch_recovery.py`."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest.mock import Mock
from uuid import uuid4

import pytest
from pytest import MonkeyPatch
from sqlmodel import Session, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Task,
    TaskStatus,
)
from tracker.executor import dispatch_recovery
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release


class SessionContext:
    def __init__(self, session: Mock) -> None:
        self.session = session

    def __enter__(self) -> Mock:
        return self.session

    def __exit__(self, *_args: object) -> None:
        return None


def test_reconcile_once_commits_a_successful_pass(monkeypatch: MonkeyPatch) -> None:
    session = Mock()
    monkeypatch.setattr(dispatch_recovery, "Session", lambda _engine: SessionContext(session))
    monkeypatch.setattr(dispatch_recovery, "reconcile_expired_dispatches", lambda _session: 3)

    assert dispatch_recovery.reconcile_expired_dispatches_once() == 3

    session.commit.assert_called_once_with()
    session.rollback.assert_not_called()


def test_reconcile_once_rolls_back_and_reraises_failures(monkeypatch: MonkeyPatch) -> None:
    session = Mock()
    monkeypatch.setattr(dispatch_recovery, "Session", lambda _engine: SessionContext(session))

    def fail_reconciliation(_session: Mock) -> int:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(dispatch_recovery, "reconcile_expired_dispatches", fail_reconciliation)

    with pytest.raises(RuntimeError, match="database unavailable"):
        dispatch_recovery.reconcile_expired_dispatches_once()

    session.rollback.assert_called_once_with()
    session.commit.assert_not_called()


def test_reconcile_once_commits_database_recovery_when_ecs_check_fails(monkeypatch: MonkeyPatch) -> None:
    database_pass = Mock()
    ecs_pass = Mock()
    sessions = iter([ecs_pass, database_pass])
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setattr(dispatch_recovery, "Session", lambda _engine: SessionContext(next(sessions)))
    monkeypatch.setattr(dispatch_recovery, "reconcile_expired_dispatches", lambda _session: 2)

    def fail_describe(_session: Mock) -> int:
        raise RuntimeError("ecs unavailable")

    monkeypatch.setattr(dispatch_recovery, "_reconcile_stopped_tasks", fail_describe)

    with pytest.raises(RuntimeError, match="ecs unavailable"):
        dispatch_recovery.reconcile_expired_dispatches_once()

    database_pass.commit.assert_called_once_with()
    ecs_pass.rollback.assert_called_once_with()
    ecs_pass.commit.assert_not_called()


@pytest.mark.parametrize(
    "terminal_status",
    [ExecutorDispatchStatus.FAILED, ExecutorDispatchStatus.FINISHED, ExecutorDispatchStatus.RUNNING],
)
def test_recovery_sweeps_only_nonqueued_payloads(
    terminal_status: ExecutorDispatchStatus,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
) -> None:
    release = ExecutorRelease(
        id="payload-sweep-release",
        artifact_uri="s3://artifacts/runner.pex",
        artifact_digest="a" * 64,
        protocol_version="3",
        readiness_verified=True,
    )
    benchmark = example_benchmark_object
    database_session.add_all([release, benchmark])
    database_session.flush()
    dispatches = [
        ExecutorDispatch(
            id=uuid4(),
            benchmark_id=benchmark.id,
            kind=ExecutorDispatchKind.START,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version=release.protocol_version,
            status=status,
        )
        for status in (ExecutorDispatchStatus.QUEUED, terminal_status)
    ]
    for dispatch in dispatches:
        sealed = seal_payload(dispatch.id, {"sensitive": "secret-marker"}, generate_payload_key(dispatch.id))
        database_session.add(dispatch)
        database_session.add(
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            )
        )
    database_session.commit()
    monkeypatch.setattr(dispatch_recovery, "engine", database_session.get_bind())
    monkeypatch.setattr(dispatch_recovery, "reconcile_expired_dispatches", lambda _session: 0)

    assert dispatch_recovery.reconcile_expired_dispatches_once() == 0

    database_session.expire_all()
    queued, nonqueued = dispatches
    kept = database_session.get(ExecutorDispatchPayload, queued.id)
    assert kept is not None
    assert b"secret-marker" not in kept.ciphertext
    assert database_session.get(ExecutorDispatchPayload, nonqueued.id) is None


def test_recovery_loop_retries_after_a_failed_pass(monkeypatch: MonkeyPatch) -> None:
    stop_event = Event()
    attempts = 0

    def reconcile() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("database unavailable")
        stop_event.set()
        return 1

    logger = Mock()
    monkeypatch.setattr(dispatch_recovery, "reconcile_expired_dispatches_once", reconcile)
    monkeypatch.setattr(dispatch_recovery, "_logger", logger)

    dispatch_recovery.run_dispatch_recovery_loop(stop_event, interval_seconds=0)

    assert attempts == 2
    logger.exception.assert_called_once_with(
        "executor_dispatch_recovery_failed",
        extra={"event": "automatic_dispatch_recovery_failed"},
    )


def test_automatic_recovery_starts_and_stops_owned_thread(monkeypatch: MonkeyPatch) -> None:
    events: list[str] = []

    class FakeThread:
        def __init__(
            self,
            *,
            target: Callable[..., None],
            args: tuple[Event],
            kwargs: dict[str, float],
            name: str,
            daemon: bool,
        ) -> None:
            assert target is dispatch_recovery.run_dispatch_recovery_loop
            assert kwargs == {"interval_seconds": 60}
            assert name == "executor-dispatch-recovery"
            assert daemon
            self.stop_event = args[0]

        def start(self) -> None:
            events.append("started")

        def join(self, timeout: float) -> None:
            assert timeout == 5
            assert self.stop_event.is_set()
            events.append("joined")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(dispatch_recovery, "Thread", FakeThread)
    recovery = dispatch_recovery.AutomaticDispatchRecovery(interval_seconds=60)

    recovery.start()
    recovery.stop()

    assert events == ["started", "joined"]


def test_automatic_recovery_logs_if_owned_thread_does_not_stop(monkeypatch: MonkeyPatch) -> None:
    logger = Mock()

    class NeverStops:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            pass

        def join(self, timeout: float) -> None:
            assert timeout == 5

        def is_alive(self) -> bool:
            return True

    monkeypatch.setattr(dispatch_recovery, "Thread", NeverStops)
    monkeypatch.setattr(dispatch_recovery, "_logger", logger)

    dispatch_recovery.AutomaticDispatchRecovery().stop()

    logger.error.assert_called_once_with(
        "executor_dispatch_recovery_shutdown_timeout",
        extra={"event": "automatic_dispatch_recovery_shutdown_timeout"},
    )


@pytest.mark.parametrize(
    ("last_status", "claim_deadline_passed"),
    [("STOPPED", False), ("RUNNING", False), ("MISSING", False), ("STOPPED", True)],
)
def test_recovery_reports_only_stopped_before_claim(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
    last_status: str,
    claim_deadline_passed: bool,
) -> None:
    release = ExecutorRelease(
        id="ecs-stopped-release",
        artifact_uri="s3://artifacts/runner.pex",
        artifact_digest="a" * 64,
        protocol_version="3",
        readiness_verified=True,
    )
    benchmark = example_benchmark_object
    register_release(database_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
    )
    dispatch.ecs_task_arn = "arn:aws:ecs:us-east-1:123456789012:task/cluster/task"
    dispatch.claim_deadline_at = datetime.now(UTC) + timedelta(minutes=-1 if claim_deadline_passed else 2)
    task = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id="task-1",
        status=TaskStatus.PENDING,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    database_session.add_all([benchmark, dispatch, task])
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    monkeypatch.setattr(dispatch_recovery, "engine", database_session.get_bind())
    ecs = Mock()
    stopped = {
        "taskArn": dispatch.ecs_task_arn,
        "lastStatus": "STOPPED",
        "stoppedReason": "Task failed to start",
        "containers": [{"reason": "CannotPullContainerError: image missing"}],
    }
    ecs.describe_tasks.return_value = {
        "tasks": [] if last_status == "MISSING" else [stopped | {"lastStatus": last_status}],
        "failures": [],
    }
    monkeypatch.setattr(dispatch_recovery.boto3, "client", lambda *_args, **_kwargs: ecs)

    assert dispatch_recovery.reconcile_expired_dispatches_once() == (1 if last_status == "STOPPED" else 0)
    ecs.describe_tasks.assert_called_once_with(cluster="cluster", tasks=[dispatch.ecs_task_arn])
    database_session.expire_all()
    if last_status == "STOPPED":
        assert database_session.get(ExecutorDispatch, dispatch.id).status == ExecutorDispatchStatus.FAILED
        assert database_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.ERROR
        error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
        assert "CannotPullContainerError: image missing" in error.error_message
        assert error.error_type == "ExecutorTaskStoppedBeforeClaim"
    else:
        assert database_session.get(ExecutorDispatch, dispatch.id).status == ExecutorDispatchStatus.QUEUED
        assert database_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.IN_PROGRESS
