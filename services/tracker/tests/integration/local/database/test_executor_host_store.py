"""ExecutorHost dispatch-store integration against disposable PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlmodel import Session, col, select

from tests.factories import make_benchmark
from tracker.database.models import (
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.executor.release_control import (
    promote_release,
    register_release,
)
from tracker.executor.dispatch_control import (
    admit_recovery_dispatch,
    admit_start_dispatch,
    reconcile_expired_dispatches,
)


@pytest.mark.parametrize("dispatch_count", [1, 2])
@pytest.mark.parametrize("running", [False, True])
def test_expiry_cleans_all_admitted_assignments(postgres_session: Session, dispatch_count: int, running: bool) -> None:
    """Verify expired dispatches leave no assigned tasks active after recovery.

    Test cases:
    - Fresh START tasks use their normal model timestamp.
    - Queued and running dispatches recover alone or with an expired sibling.
    """
    org = Org(id=uuid4(), name=f"review-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"review-{uuid4()}",
        artifact_uri="s3://artifacts/review.pex",
        artifact_digest="a" * 64,
        protocol_version="4",
        readiness_verified=True,
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    promote_release(postgres_session, release.id)
    postgres_session.commit()
    tasks: list[Task] = []
    dispatches: list[ExecutorDispatch] = []
    for index in range(dispatch_count):
        task = Task(org_id=org.id, benchmark=benchmark.id, task_id=f"task-{index}")
        postgres_session.add(task)
        if index == 0:
            dispatch = admit_start_dispatch(
                postgres_session, benchmark=benchmark, dispatch_id=uuid4(), task_ids=[task.task_id]
            )
        else:
            dispatch = admit_recovery_dispatch(
                postgres_session,
                benchmark=benchmark,
                pre_action_status=benchmark.status,
                dispatch_id=uuid4(),
                kind=ExecutorDispatchKind.RESUME,
                task_ids=[task.task_id],
            )
        assert task.started_at is not None
        assert task.started_at <= dispatch.created_at
        dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(minutes=1)
        if running:
            dispatch.status = ExecutorDispatchStatus.RUNNING
            dispatch.started_at = datetime.now(UTC) - timedelta(minutes=6)
            dispatch.lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
            task.status = TaskStatus.IN_PROGRESS
        tasks.append(task)
        dispatches.append(dispatch)
    postgres_session.commit()

    assert reconcile_expired_dispatches(postgres_session) == dispatch_count
    postgres_session.commit()
    for row in [benchmark, *tasks, *dispatches]:
        postgres_session.refresh(row)

    assert benchmark.status == BenchmarkStatus.ERROR
    assert all(row.status == ExecutorDispatchStatus.FAILED for row in dispatches)
    assert [row.status for row in tasks] == [TaskStatus.ERROR] * dispatch_count
    errors = postgres_session.exec(
        select(ErrorResult).where(col(ErrorResult.task).in_([row.id for row in tasks]))
    ).all()
    assert len(errors) == dispatch_count
