"""The receipt frozen at release must not move the reviewed completed-history proof."""

from dataclasses import asdict
from datetime import UTC, datetime
from uuid import UUID, uuid4

from tracker.aws.runtime import AWSResources
from tracker.database.models import RunLifecycle
from tracker.lifecycle import OperationIdentity, RunScope
from tracker.lifecycle_completion import RelocationCheckpoint, canonical_digest, capture_predecessor
from tracker.storage_migration_exchange import RunObservation

_SOURCE = AWSResources("us-east-1", "legacy-shared-storage", "runs", 7)
_DESTINATION = AWSResources("us-east-1", "vs-dev-owner-42", "runs", 7)


def completed_history(run_id: UUID) -> tuple[RunLifecycle, OperationIdentity, RunScope, RunObservation]:
    identity = OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=uuid4(),
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-east-1",
        environment="dev",
        database_target="postgresql:localhost:5432/tracker",
        run_ids=(run_id,),
    )
    original = RunScope(run_id=run_id, original_resources=_SOURCE)
    current = RunScope(run_id=run_id, original_resources=_DESTINATION)
    checkpoint = RelocationCheckpoint(
        identity_sha256=canonical_digest(identity.model_dump(mode="json")),
        scope_sha256=canonical_digest(original.model_dump(mode="json")),
        child_plan_sha256="b" * 64,
        execution_arguments_sha256="c" * 64,
        execution_policy="history_only",
        destination_resources=_DESTINATION,
        dispatch_ids=(),
        copied_objects_sha256=canonical_digest([]),
        destination_versions_sha256=canonical_digest([]),
        parent_completion_sha256="d" * 64,
        phase="relocated_history_only",
    )
    record = RunLifecycle(
        run_id=run_id,
        identity_json=identity.model_dump_json(),
        scope_json=original.model_dump_json(),
        purpose="relocation",
        phase="relocated_history_only",
        acquired_at=datetime.now(UTC),
        checkpoint_json=checkpoint.model_dump_json(),
    )
    receipt = RunObservation.model_validate(
        {
            "run_id": run_id,
            "org_id": identity.org_id,
            "label": None,
            "resources": asdict(_DESTINATION),
            "status": "FINISHED",
            "hold_phase": "relocated_history_only",
            "observed_at": datetime.now(UTC),
        }
    )

    return record, identity, current, receipt


def test_a_frozen_receipt_leaves_the_completed_history_predecessor_digest_unchanged() -> None:
    record, identity, current, receipt = completed_history(uuid4())

    before = capture_predecessor(record, identity, current)
    stored = RelocationCheckpoint.model_validate_json(record.checkpoint_json or "null")
    record.checkpoint_json = stored.model_copy(update={"receipt": receipt}).model_dump_json()
    after = capture_predecessor(record, identity, current)

    assert before.completion_sha256 is not None
    assert before == after
