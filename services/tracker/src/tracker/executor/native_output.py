"""Publish and revoke the exact sandbox attachment owned by an executor."""

from uuid import UUID

from sqlalchemy import JSON, type_coerce
from sqlmodel import Session, col, select, update

from tracker.database.models import ActiveNativeOutput, Task, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority


def publish_native_output(
    session: Session,
    task_id: UUID,
    descriptor: ActiveNativeOutput,
    authority: ExecutionAuthority,
) -> None:
    lock_execution_authority(session, authority)
    task = session.exec(
        select(Task)
        .where(col(Task.id) == task_id)
        .where(col(Task.benchmark) == authority.benchmark_id)
        .where(col(Task.started_at) == descriptor.attempt_started_at)
        .where(col(Task.status).in_((TaskStatus.BUILDING, TaskStatus.IN_PROGRESS)))
        .with_for_update()
    ).one_or_none()
    if task is None:
        raise ExecutionAuthorityRevoked("Task attempt no longer owns native output")
    task.active_native_output = descriptor
    session.add(task)
    session.commit()


def clear_native_output(session: Session, task_id: UUID, descriptor: ActiveNativeOutput) -> None:
    # Revocation must work after stop/dispatch cancellation, but never revoke a replacement.
    stored = type_coerce(col(Task.active_native_output), JSON)
    session.exec(
        update(Task)
        .where(col(Task.id) == task_id)
        .where(stored["sandbox_id"].as_string() == descriptor.sandbox_id)
        .where(stored["attempt_started_at"].as_string() == descriptor.model_dump(mode="json")["attempt_started_at"])
        .values(active_native_output=None)
        .execution_options(synchronize_session=False)
    )
    session.commit()
