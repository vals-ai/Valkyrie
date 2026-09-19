"""A deletion hold that never started purging must be correctable, and no other."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.aws.runtime import AWSResources
from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import (
    LifecycleConflict,
    OperationIdentity,
    RunScope,
    abandon_deletion_hold,
    acquire_hold,
    active_hold,
)
from tracker.run_purge import _selected_runs, _unstarted_purge_guard
from tracker.run_purge.contracts import ProviderLocator, PurgeCheckpoint, PurgePlan, PurgeRun
from tracker.run_purge.predecessor import _previous_identity, acquire_deletion_hold, capture_predecessor

_RESOURCES = AWSResources(region="us-west-2", s3_bucket="owner-data", log_group="runs", log_retention_days=7)
_PROVIDER = ProviderLocator(kind="daytona", secret_name="provider")
_ROW_TABLES = (
    "benchmark",
    "task",
    "evaluationresult",
    "errorresult",
    "finalevaluation",
    "executordispatch",
    "taskbreakdown",
)


def seeded_hold(session: Session) -> tuple[OperationIdentity, RunScope, Benchmark, PurgePlan]:
    run = make_benchmark(org_id=TEST_ORG_ID)
    run.arguments = run.arguments.model_copy(
        update={
            "properties": _RESOURCES,
            "sandbox_provider": _PROVIDER.kind,
            "sandbox_provider_secret_name": _PROVIDER.secret_name,
        }
    )
    session.add(run)
    session.commit()
    identity = make_identity(run.id)
    scope = RunScope(run_id=run.id, original_resources=_RESOURCES)
    acquire_hold(session, identity=identity, scope=scope, purpose="deletion")
    session.commit()
    plan = PurgePlan(identity=identity, runs=(PurgeRun(scope=scope, provider=_PROVIDER),))

    return identity, scope, run, plan


def make_identity(*run_ids: UUID) -> OperationIdentity:
    return OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=TEST_ORG_ID,
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-west-2",
        environment="test",
        database_target="tracker-test",
        run_ids=tuple(sorted(run_ids, key=str)),
    )


def make_checkpoint(phase: str, *, run_id: UUID, rows: bool, digest: str) -> PurgeCheckpoint:
    return PurgeCheckpoint.model_validate(
        {
            "child_plan_sha256": digest,
            "provider": _PROVIDER.model_dump(),
            "original_dispatches": [],
            "phase": phase,
            "rows": [{"table": table, "ids": [run_id] if table == "benchmark" else []} for table in _ROW_TABLES]
            if rows
            else [],
            "fence_policy_sha256": "c" * 64 if rows else None,
        }
    )


def store_checkpoint(session: Session, run_id: UUID, checkpoint: PurgeCheckpoint) -> RunLifecycle:
    record = session.get(RunLifecycle, run_id)
    assert record is not None
    record.checkpoint_json = checkpoint.model_dump_json()
    record.phase = checkpoint.phase
    session.add(record)
    session.commit()

    return record


@pytest.mark.parametrize("phase", ["held", "prepared"])
def test_abandon_releases_a_hold_before_any_purge_step(database_session: Session, phase: str) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    store_checkpoint(database_session, run.id, make_checkpoint(phase, run_id=run.id, rows=False, digest=plan.digest()))

    record = abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()

    assert record.phase == "abandoned"
    assert record.released_at is not None and record.released_at.tzinfo is not None
    assert active_hold(database_session, run.id) is None
    assert database_session.get(RunLifecycle, run.id) is not None


def test_abandon_releases_a_hold_that_has_no_checkpoint(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)

    abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()

    assert active_hold(database_session, run.id) is None


@pytest.mark.parametrize(
    ("phase", "rows"),
    [("prepared", True), ("objects_removed", True), ("logs_removed", True), ("rows_removed", True)],
)
def test_abandon_refuses_a_purge_that_has_started(database_session: Session, phase: str, rows: bool) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    store_checkpoint(database_session, run.id, make_checkpoint(phase, run_id=run.id, rows=rows, digest=plan.digest()))

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )
    database_session.rollback()

    assert active_hold(database_session, run.id) is not None


def test_abandon_refuses_a_checkpoint_whose_phase_was_edited(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    record = store_checkpoint(
        database_session, run.id, make_checkpoint("prepared", run_id=run.id, rows=False, digest=plan.digest())
    )
    record.phase = "held"
    database_session.add(record)
    database_session.commit()

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )
    database_session.rollback()

    assert active_hold(database_session, run.id) is not None


def test_abandon_refuses_a_checkpoint_from_another_child_plan(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    store_checkpoint(database_session, run.id, make_checkpoint("prepared", run_id=run.id, rows=False, digest="b" * 64))

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )
    database_session.rollback()

    assert active_hold(database_session, run.id) is not None


@pytest.mark.parametrize("phase", ["prepared", "objects_removed"])
def test_abandon_refuses_a_missing_checkpoint_after_the_held_phase(database_session: Session, phase: str) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    record = database_session.get(RunLifecycle, run.id)
    assert record is not None and record.checkpoint_json is None
    record.phase = phase
    database_session.add(record)
    database_session.commit()

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )
    database_session.rollback()

    assert active_hold(database_session, run.id) is not None


def test_abandon_refuses_another_operation_and_a_second_attempt(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    other = identity.model_copy(update={"operation_id": uuid4()})

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=other, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )
    database_session.rollback()

    abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )
    database_session.rollback()

    assert active_hold(database_session, run.id) is None


def test_abandon_refuses_an_absent_run(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    database_session.delete(database_session.get(Benchmark, run.id))
    database_session.commit()

    with pytest.raises(LifecycleConflict):
        abandon_deletion_hold(
            database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
        )


def test_a_corrected_operation_replaces_an_abandoned_deletion_hold(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()
    corrected = make_identity(run.id)
    relocation, abandoned = capture_predecessor(database_session, corrected, run.id)
    purge_run = PurgeRun(scope=scope, provider=_PROVIDER, abandoned_deletion=abandoned)

    assert relocation is None
    assert abandoned is not None and abandoned.operation_id == identity.operation_id
    record = acquire_deletion_hold(database_session, corrected, purge_run)
    database_session.commit()

    assert record.phase == "held" and record.released_at is None
    assert active_hold(database_session, run.id) is not None


def test_a_plan_built_before_a_replacement_hold_cannot_replace_it(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()
    stale = make_identity(run.id)
    stale_relocation, stale_abandoned = capture_predecessor(database_session, stale, run.id)
    replacement = make_identity(run.id)
    acquire_deletion_hold(
        database_session,
        replacement,
        PurgeRun(scope=scope, provider=_PROVIDER, abandoned_deletion=stale_abandoned),
    )
    abandon_deletion_hold(
        database_session,
        identity=replacement,
        scope=scope,
        verify_unstarted=_unstarted_purge_guard(
            PurgePlan(identity=replacement, runs=(PurgeRun(scope=scope, provider=_PROVIDER),))
        ),
    )
    database_session.commit()

    with pytest.raises(LifecycleConflict):
        acquire_deletion_hold(
            database_session,
            stale,
            PurgeRun(
                scope=scope,
                provider=_PROVIDER,
                released_relocation=stale_relocation,
                abandoned_deletion=stale_abandoned,
            ),
        )
    database_session.rollback()

    record = database_session.get(RunLifecycle, run.id)
    assert record is not None
    assert _previous_identity(record).operation_id == replacement.operation_id


def test_a_plan_with_no_predecessor_cannot_replace_an_abandoned_hold(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()

    with pytest.raises(LifecycleConflict):
        acquire_deletion_hold(database_session, make_identity(run.id), PurgeRun(scope=scope, provider=_PROVIDER))
    database_session.rollback()

    record = database_session.get(RunLifecycle, run.id)
    assert record is not None and record.phase == "abandoned"


def test_an_abandoned_hold_does_not_admit_an_unplanned_relocation_predecessor(database_session: Session) -> None:
    identity, scope, run, plan = seeded_hold(database_session)
    abandon_deletion_hold(
        database_session, identity=identity, scope=scope, verify_unstarted=_unstarted_purge_guard(plan)
    )
    database_session.commit()
    invented = PurgeRun(
        scope=scope,
        provider=_PROVIDER,
        released_relocation={
            "operation_id": uuid4(),
            "identity_sha256": "d" * 64,
            "scope_sha256": "e" * 64,
            "acquired_at": "2026-09-19T00:00:00Z",
            "released_at": "2026-09-19T01:00:00Z",
        },
    )

    with pytest.raises(LifecycleConflict):
        acquire_deletion_hold(database_session, make_identity(run.id), invented)


def test_a_deletion_hold_still_cannot_be_released_without_abandonment(database_session: Session) -> None:
    _, _, run, _ = seeded_hold(database_session)
    record = database_session.get(RunLifecycle, run.id)
    assert record is not None
    record.released_at = datetime.now(UTC)
    record.phase = "released"
    database_session.add(record)

    with pytest.raises(IntegrityError):
        database_session.commit()
    database_session.rollback()


def test_abandonment_requires_exact_planned_run_identifiers() -> None:
    first, second = sorted((uuid4(), uuid4()), key=str)
    identity = make_identity(first, second)
    plan = PurgePlan(
        identity=identity,
        runs=tuple(
            PurgeRun(scope=RunScope(run_id=run_id, original_resources=_RESOURCES), provider=_PROVIDER)
            for run_id in (first, second)
        ),
    )

    assert tuple(run.scope.run_id for run in _selected_runs(plan, (second,))) == (second,)

    for requested in ((), (uuid4(),), (first, uuid4())):
        with pytest.raises(LifecycleConflict):
            _selected_runs(plan, requested)
