"""Inspect and explicitly release settled orphan sandbox-creation reservations."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    ExecutorPoolReservation,
    Task,
)
from tracker.database.session import engine
from tracker.executor.release_control import ReleaseControlError, get_executor_admission

ProviderOutcome = Literal["absent", "cleaned"]
_TERMINAL_DISPATCH_STATUSES = (ExecutorDispatchStatus.FAILED, ExecutorDispatchStatus.FINISHED)
_logger = logging.getLogger(__name__)


class PoolReservationRecoveryError(ReleaseControlError):
    """An operator did not establish that the reserved provider operation is safe to release."""


@dataclass(frozen=True)
class PoolReservationInfo:
    pool_id: str
    reservation_id: UUID
    dispatch_id: UUID
    dispatch_status: ExecutorDispatchStatus
    benchmark_id: UUID
    benchmark_status: BenchmarkStatus
    task_id: UUID
    task_name: str
    started_at: datetime


def list_pool_reservations(session: Session) -> list[PoolReservationInfo]:
    """Return a read-only view of outstanding creation reservations."""
    reservations = session.exec(select(ExecutorPoolReservation).order_by(col(ExecutorPoolReservation.pool_id))).all()
    listed: list[PoolReservationInfo] = []
    for reservation in reservations:
        dispatch = session.get(ExecutorDispatch, reservation.dispatch_id)
        task = session.get(Task, reservation.task_id)
        if dispatch is None or task is None:
            raise PoolReservationRecoveryError(
                f"Reservation {reservation.reservation_id} refers to a missing dispatch or task"
            )
        benchmark = session.get(Benchmark, dispatch.benchmark_id)
        if benchmark is None:
            raise PoolReservationRecoveryError(f"Dispatch {dispatch.id} refers to a missing benchmark")
        listed.append(
            PoolReservationInfo(
                pool_id=reservation.pool_id,
                reservation_id=reservation.reservation_id,
                dispatch_id=reservation.dispatch_id,
                dispatch_status=dispatch.status,
                benchmark_id=benchmark.id,
                benchmark_status=benchmark.status,
                task_id=reservation.task_id,
                task_name=task.task_id,
                started_at=reservation.started_at,
            )
        )

    return listed


def release_settled_pool_reservation(
    session: Session,
    *,
    pool_id: str,
    reservation_id: UUID,
    creator_stopped: bool,
    provider_operation_settled: bool,
    provider_outcome: ProviderOutcome,
) -> bool:
    """Release one exact reservation after an operator confirms the external operation is safe.

    A terminal dispatch and an expired lease do not prove that a provider request stopped.
    The caller must confirm the creator stopped, no provider request remains in flight, and
    the sandbox is absent or has been cleaned up. The function never infers these facts from
    elapsed time.
    """
    if not creator_stopped:
        raise PoolReservationRecoveryError("Confirm that the original executor creator has stopped")
    if not provider_operation_settled:
        raise PoolReservationRecoveryError("Confirm that no provider creation request remains in flight")
    if provider_outcome not in ("absent", "cleaned"):
        raise PoolReservationRecoveryError("Provider outcome must be absent or cleaned")

    # Use the same admission -> benchmark -> dispatch -> reservation lock order as normal
    # reservation and release operations.
    get_executor_admission(session, for_update=True)
    reservation = session.exec(
        select(ExecutorPoolReservation)
        .where(ExecutorPoolReservation.pool_id == pool_id)
        .execution_options(populate_existing=True)
    ).one_or_none()
    if reservation is None:
        return False
    if reservation.reservation_id != reservation_id:
        raise PoolReservationRecoveryError("Reservation ID does not match the current pool reservation")

    benchmark_id = session.exec(
        select(ExecutorDispatch.benchmark_id).where(ExecutorDispatch.id == reservation.dispatch_id)
    ).one_or_none()
    if benchmark_id is None:
        raise PoolReservationRecoveryError("Reservation refers to a missing executor dispatch")
    session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()
    dispatch = session.exec(
        select(ExecutorDispatch)
        .where(ExecutorDispatch.id == reservation.dispatch_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()
    if dispatch is None or dispatch.status not in _TERMINAL_DISPATCH_STATUSES:
        raise PoolReservationRecoveryError("Executor dispatch must be terminal before reservation recovery")

    locked_reservation = session.exec(
        select(ExecutorPoolReservation)
        .where(ExecutorPoolReservation.pool_id == pool_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()
    if locked_reservation is None:
        return False
    if locked_reservation.reservation_id != reservation_id or locked_reservation.dispatch_id != dispatch.id:
        raise PoolReservationRecoveryError("Pool reservation changed while recovery was being prepared")

    session.delete(locked_reservation)
    session.flush()
    _logger.warning(
        "executor_pool_reservation_operator_recovery",
        extra={
            "event": "executor_pool_reservation_operator_recovery",
            "pool_id": pool_id,
            "reservation_id": str(reservation_id),
            "dispatch_id": str(dispatch.id),
            "provider_outcome": provider_outcome,
        },
    )

    return True


def _format_reservation(reservation: PoolReservationInfo) -> str:
    return (
        f"pool_id={reservation.pool_id} reservation_id={reservation.reservation_id} "
        f"dispatch_id={reservation.dispatch_id} dispatch_status={reservation.dispatch_status.value} "
        f"benchmark_id={reservation.benchmark_id} benchmark_status={reservation.benchmark_status.value} "
        f"task={reservation.task_name} task_id={reservation.task_id} started_at={reservation.started_at}"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("list", help="list outstanding pool reservations (the default)")
    release = subparsers.add_parser("release", help="release one verified, settled orphan reservation")
    release.add_argument("--pool-id", required=True)
    release.add_argument("--reservation-id", required=True, type=UUID)
    release.add_argument("--confirm-creator-stopped", action="store_true", required=True)
    release.add_argument("--confirm-provider-settled", action="store_true", required=True)
    release.add_argument("--provider-outcome", choices=("absent", "cleaned"), required=True)
    parser.set_defaults(command="list")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    with Session(engine, expire_on_commit=False) as session:
        if args.command == "release":
            released = release_settled_pool_reservation(
                session,
                pool_id=args.pool_id,
                reservation_id=args.reservation_id,
                creator_stopped=args.confirm_creator_stopped,
                provider_operation_settled=args.confirm_provider_settled,
                provider_outcome=args.provider_outcome,
            )
            session.commit()
            print("Reservation released." if released else "Reservation was already cleared.")
            return

        reservations = list_pool_reservations(session)
        if not reservations:
            print("No outstanding pool reservations.")
            return
        for reservation in reservations:
            print(_format_reservation(reservation))


if __name__ == "__main__":
    main()
