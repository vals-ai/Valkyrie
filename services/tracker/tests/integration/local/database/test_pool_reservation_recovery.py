"""Exercise operator pool-reservation recovery against PostgreSQL.

Run: uv run pytest tests/integration/local/database/test_pool_reservation_recovery.py
"""

from typing import Literal
from uuid import uuid4
import os
import subprocess
import sys

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorPoolReservation,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
    TaskStatus,
)
from tracker.executor.pool_reservation_recovery import (
    PoolReservationRecoveryError,
    list_pool_reservations,
    release_settled_pool_reservation,
)


@pytest.fixture
def pool_reservation(postgres_session: Session) -> ExecutorPoolReservation:
    org = Org(name=f"pool-recovery-{uuid4().hex}")
    release = ExecutorRelease(
        id=f"pool-recovery-{uuid4().hex}",
        artifact_uri="s3://artifacts/executor.pex",
        artifact_digest="a" * 64,
        protocol_version="4",
        status=ExecutorReleaseStatus.ACTIVE,
        readiness_verified=True,
    )
    postgres_session.add_all([org, release])
    postgres_session.flush()

    benchmark = make_benchmark(org_id=org.id, status=BenchmarkStatus.IN_PROGRESS)
    postgres_session.add(benchmark)
    postgres_session.flush()
    task = make_task(benchmark, "task-0", status=TaskStatus.PENDING)
    dispatch = ExecutorDispatch(
        benchmark_id=benchmark.id,
        kind=ExecutorDispatchKind.START,
        status=ExecutorDispatchStatus.FAILED,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
        assigned_task_ids=[task.task_id],
    )
    postgres_session.add_all([task, dispatch])
    postgres_session.flush()
    reservation = ExecutorPoolReservation(
        pool_id="pool_test",
        reservation_id=uuid4(),
        dispatch_id=dispatch.id,
        task_id=task.id,
        started_at=task.started_at,
    )
    postgres_session.add(reservation)
    postgres_session.commit()
    postgres_session.refresh(reservation)

    return reservation


def test_recovery_requires_both_provider_confirmations(
    postgres_session: Session, pool_reservation: ExecutorPoolReservation
) -> None:
    """List reservations read-only and require both provider confirmations to release."""
    listed = list_pool_reservations(postgres_session)
    assert len(listed) == 1
    assert listed[0].pool_id == pool_reservation.pool_id
    assert listed[0].reservation_id == pool_reservation.reservation_id

    with pytest.raises(PoolReservationRecoveryError, match="original executor creator"):
        release_settled_pool_reservation(
            postgres_session,
            pool_id=pool_reservation.pool_id,
            reservation_id=pool_reservation.reservation_id,
            creator_stopped=False,
            provider_operation_settled=True,
            provider_outcome="absent",
        )

    with pytest.raises(PoolReservationRecoveryError, match="no provider creation request"):
        release_settled_pool_reservation(
            postgres_session,
            pool_id=pool_reservation.pool_id,
            reservation_id=pool_reservation.reservation_id,
            creator_stopped=True,
            provider_operation_settled=False,
            provider_outcome="cleaned",
        )

    postgres_session.rollback()
    assert postgres_session.get(ExecutorPoolReservation, pool_reservation.pool_id) is not None


def test_recovery_refuses_a_nonterminal_dispatch(
    postgres_session: Session, pool_reservation: ExecutorPoolReservation
) -> None:
    """Refuse to clear a reservation while its dispatch can still be running."""
    dispatch = postgres_session.get(ExecutorDispatch, pool_reservation.dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.RUNNING
    postgres_session.add(dispatch)
    postgres_session.commit()

    with pytest.raises(PoolReservationRecoveryError, match="must be terminal"):
        release_settled_pool_reservation(
            postgres_session,
            pool_id=pool_reservation.pool_id,
            reservation_id=pool_reservation.reservation_id,
            creator_stopped=True,
            provider_operation_settled=True,
            provider_outcome="cleaned",
        )

    postgres_session.rollback()
    assert postgres_session.get(ExecutorPoolReservation, pool_reservation.pool_id) is not None


@pytest.mark.parametrize("provider_outcome", ["absent", "cleaned"])
def test_recovery_releases_only_the_matching_settled_reservation(
    postgres_session: Session,
    pool_reservation: ExecutorPoolReservation,
    provider_outcome: Literal["absent", "cleaned"],
) -> None:
    """Require the current reservation ID and a safe provider outcome."""
    with pytest.raises(PoolReservationRecoveryError, match="does not match"):
        release_settled_pool_reservation(
            postgres_session,
            pool_id=pool_reservation.pool_id,
            reservation_id=uuid4(),
            creator_stopped=True,
            provider_operation_settled=True,
            provider_outcome="cleaned",
        )
    postgres_session.rollback()

    released = release_settled_pool_reservation(
        postgres_session,
        pool_id=pool_reservation.pool_id,
        reservation_id=pool_reservation.reservation_id,
        creator_stopped=True,
        provider_operation_settled=True,
        provider_outcome=provider_outcome,
    )
    postgres_session.commit()

    assert released is True
    assert postgres_session.get(ExecutorPoolReservation, pool_reservation.pool_id) is None


def test_recovery_reports_an_already_cleared_reservation(postgres_session: Session) -> None:
    """Make repeat operator commands safe after the reservation is gone."""
    released = release_settled_pool_reservation(
        postgres_session,
        pool_id="pool_missing",
        reservation_id=uuid4(),
        creator_stopped=True,
        provider_operation_settled=True,
        provider_outcome="absent",
    )

    assert released is False


def test_recovery_cli_lists_and_releases_confirmed_reservation(
    postgres_engine: Engine, postgres_session: Session, pool_reservation: ExecutorPoolReservation
) -> None:
    """Run the operator commands against an isolated database without provider calls."""
    command = [sys.executable, "-m", "tracker.executor.pool_reservation_recovery"]
    environment = {**os.environ, "DATABASE_URL": postgres_engine.url.render_as_string(hide_password=False)}
    listed = subprocess.run([*command, "list"], env=environment, capture_output=True, text=True, check=True, timeout=30)
    assert str(pool_reservation.reservation_id) in listed.stdout
    released = subprocess.run(
        [
            *command,
            "release",
            "--pool-id",
            pool_reservation.pool_id,
            "--reservation-id",
            str(pool_reservation.reservation_id),
            "--confirm-creator-stopped",
            "--confirm-provider-settled",
            "--provider-outcome",
            "absent",
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert "Reservation released." in released.stdout
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorPoolReservation, "pool_test") is None
