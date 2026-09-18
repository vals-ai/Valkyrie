"""Transfer-only transition from a proved prior local relocation."""

import json
from datetime import UTC, datetime

from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity
from tracker.lifecycle_completion import (
    RelocationCheckpoint,
    RelocationPredecessor,
    acquire_successor_hold,
    capture_predecessor,
)
from tracker.run_transfer.contracts import TransferRequest, TransferRun
from tracker.run_transfer.rows import digest


def validate_predecessor(
    record: RunLifecycle | None, request: TransferRequest, run: TransferRun
) -> RelocationPredecessor | None:
    if record is None:
        if run.predecessor is not None:
            raise LifecycleConflict("Planned predecessor disappeared")
        return None
    identity = request.plan.source_identity
    old = OperationIdentity.model_validate_json(record.identity_json)
    if (
        old.destination_aws_account_id != identity.source_aws_account_id
        or any(
            getattr(old, field) != getattr(identity, field)
            for field in ("github_owner_id", "org_id", "database_target", "region", "environment")
        )
        or old.operation_id == identity.operation_id
    ):
        raise LifecycleConflict("Predecessor local authority differs")
    observed = capture_predecessor(record, old, run.source)
    # Released records also require their proved current location, not their old bucket.
    checkpoint = RelocationCheckpoint.model_validate_json(record.checkpoint_json or "null")
    if (
        checkpoint.phase != record.phase
        or checkpoint.destination_resources != run.source.original_resources
        or checkpoint.identity_sha256 != digest(old.model_dump(mode="json"))
        or checkpoint.scope_sha256 != digest(json.loads(record.scope_json))
    ):
        raise LifecycleConflict("Predecessor current saved location differs")
    observed = observed.model_copy(update={"completion_sha256": digest(checkpoint.model_dump(mode="json"))})
    if (request.action != "plan" or run.predecessor is not None) and observed != run.predecessor:
        raise LifecycleConflict("Exact reviewed predecessor changed")
    return observed


def acquire_source_hold(session: Session, request: TransferRequest, run: TransferRun) -> RunLifecycle:
    benchmark = session.exec(
        select(Benchmark)
        .where(col(Benchmark.id) == run.source.run_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()
    record = session.exec(
        select(RunLifecycle)
        .where(col(RunLifecycle.run_id) == run.source.run_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()
    if record is None:
        return acquire_successor_hold(
            session,
            identity=request.plan.source_identity,
            scope=run.source,
            purpose="relocation",
            predecessor=run.predecessor,
        )
    validate_predecessor(record, request, run)
    if (
        benchmark is None
        or benchmark.org_id != request.plan.source_identity.org_id
        or benchmark.arguments.properties != run.source.original_resources
    ):
        raise LifecycleConflict("Predecessor Benchmark scope differs")
    session.delete(record)
    session.flush()
    replacement = RunLifecycle(
        run_id=run.source.run_id,
        identity_json=request.plan.source_identity.model_dump_json(),
        scope_json=run.source.model_dump_json(),
        purpose="relocation",
        phase="held",
        acquired_at=datetime.now(UTC),
    )
    session.add(replacement)
    session.flush()
    return replacement
