"""Recover executor dispatches whose owner stopped renewing its lease."""

from __future__ import annotations

import logging
import os
from threading import Event, Thread

import boto3
from botocore.config import Config
from sqlalchemy import delete, select
from sqlmodel import Session, col

from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import Benchmark, ExecutorDispatch, ExecutorDispatchPayload
from tracker.database.session import engine
from tracker.executor.dispatch_control import record_dispatch_failure, reconcile_expired_dispatches

_RECONCILIATION_INTERVAL_SECONDS = 60.0
_SHUTDOWN_TIMEOUT_SECONDS = 5.0
_logger = logging.getLogger(__name__)
_ECS_CONFIG = Config(connect_timeout=3, read_timeout=8, retries={"max_attempts": 1})


def _reconcile_stopped_tasks(session: Session) -> int:
    candidates = session.exec(
        select(ExecutorDispatch.id, ExecutorDispatch.ecs_task_arn)
        .where(ExecutorDispatch.status == ExecutorDispatchStatus.QUEUED)
        .where(ExecutorDispatch.ecs_task_arn.is_not(None))
    ).all()
    if not candidates:
        return 0
    ecs = boto3.client("ecs", config=_ECS_CONFIG)
    recovered = 0
    for offset in range(0, len(candidates), 100):
        batch = candidates[offset : offset + 100]
        response = ecs.describe_tasks(cluster=os.environ["EXECUTOR_RUNNER_CLUSTER"], tasks=[arn for _, arn in batch])
        dispatch_ids = {arn: dispatch_id for dispatch_id, arn in batch}
        for task in response["tasks"]:
            if task["lastStatus"] != "STOPPED":
                continue
            dispatch = session.get(ExecutorDispatch, dispatch_ids[task["taskArn"]])
            benchmark = session.get(Benchmark, dispatch.benchmark_id)
            reasons = [task.get("stoppedReason"), *(c.get("reason") for c in task.get("containers", []))]
            message = "; ".join(reason for reason in reasons if reason)
            if record_dispatch_failure(
                session,
                benchmark=benchmark,
                dispatch_id=dispatch.id,
                task_ids=dispatch.assigned_task_ids or [],
                error_message=f"Executor task stopped before claim: {message}",
                producer="executor_dispatch",
                operation="dispatch_reconciliation",
                error_type="ExecutorTaskStoppedBeforeClaim",
                cause_code="ECS_TASK_STOPPED",
                failure_reason="ECS_TASK_STOPPED",
                dispatch_status=ExecutorDispatchStatus.QUEUED,
            ):
                recovered += 1
    return recovered


def reconcile_expired_dispatches_once() -> int:
    """Run one lease-reconciliation pass, then fail ECS tasks that stopped before claiming.

    Each pass commits on its own so an ECS outage never blocks database recovery.
    """
    with Session(engine) as session:
        try:
            recovered_count = reconcile_expired_dispatches(session)
            session.exec(
                delete(ExecutorDispatchPayload).where(
                    col(ExecutorDispatchPayload.dispatch_id).in_(
                        select(ExecutorDispatch.id).where(col(ExecutorDispatch.status) != ExecutorDispatchStatus.QUEUED)
                    )
                )
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
    if os.environ["EXECUTOR_LAUNCHER"] != "ecs":
        return recovered_count
    with Session(engine) as session:
        try:
            recovered_count += _reconcile_stopped_tasks(session)
            session.commit()
        except Exception:
            session.rollback()
            raise
    return recovered_count


def run_dispatch_recovery_loop(
    stop_event: Event,
    *,
    interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
) -> None:
    """Reconcile immediately and keep retrying until shutdown."""
    while not stop_event.is_set():
        try:
            reconcile_expired_dispatches_once()
        except Exception:
            _logger.exception(
                "executor_dispatch_recovery_failed",
                extra={"event": "automatic_dispatch_recovery_failed"},
            )
        stop_event.wait(interval_seconds)


class AutomaticDispatchRecovery:
    """Own the Tracker process's bounded dispatch-recovery thread lifecycle."""

    def __init__(
        self,
        *,
        interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
        shutdown_timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._stop_event = Event()
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread = Thread(
            target=run_dispatch_recovery_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="executor-dispatch-recovery",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(self._shutdown_timeout_seconds)
        if self._thread.is_alive():
            _logger.error(
                "executor_dispatch_recovery_shutdown_timeout",
                extra={"event": "automatic_dispatch_recovery_shutdown_timeout"},
            )
