"""Focused tests for durable mutation operation receipts."""

from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import HTTPException
import pytest
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.api.mutation_operations import (
    ExecuteMutation,
    FailedMutation,
    JSONObject,
    ProcessingMutation,
    SucceededMutation,
    UncertainMutation,
    claim_mutation,
    complete_mutation,
    get_mutation_status,
    mark_mutation_failed,
    mark_mutation_uncertain,
    mutation_fingerprint,
)
from tracker.database.models import MutationOperation, MutationOperationKind, MutationOperationState, Org
from tracker.types import ERROR_EXCERPT_MAX_LENGTH


def test_fingerprint_is_canonical_and_includes_kind() -> None:
    left = mutation_fingerprint(
        MutationOperationKind.RETRY_OR_RESUME_BENCHMARK,
        {"task_ids": ["one"], "mode": "auto"},
    )
    reordered = mutation_fingerprint(
        MutationOperationKind.RETRY_OR_RESUME_BENCHMARK,
        {"mode": "auto", "task_ids": ["one"]},
    )

    assert left == reordered
    assert left != mutation_fingerprint(
        MutationOperationKind.STOP_BENCHMARK,
        {"mode": "auto", "task_ids": ["one"]},
    )


def test_duplicate_processing_claim_never_executes_twice(database_session: Session) -> None:
    org = _org()
    operation_id = uuid4()
    kind = MutationOperationKind.RETRY_OR_RESUME_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"task_ids": ["one"]})

    first = claim_mutation(database_session, org, operation_id, kind, fingerprint)
    duplicate = claim_mutation(database_session, org, operation_id, kind, fingerprint)

    assert first == ExecuteMutation()
    assert duplicate == ProcessingMutation(kind=kind)
    receipt = database_session.get(MutationOperation, (org.id, operation_id))
    assert receipt is not None


def test_succeeded_claim_replays_exact_response(database_session: Session) -> None:
    org = _org()
    operation_id = uuid4()
    kind = MutationOperationKind.START_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"benchmark": "swebench"})
    claim_mutation(database_session, org, operation_id, kind, fingerprint)

    completed = complete_mutation(database_session, org, operation_id, {"run_id": "run-1"})
    replay = claim_mutation(database_session, org, operation_id, kind, fingerprint)

    assert completed == SucceededMutation(kind=kind, response={"run_id": "run-1"})
    assert replay == completed


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (MutationOperationKind.STOP_BENCHMARK, {"run_id": "run-1"}),
        (MutationOperationKind.RETRY_OR_RESUME_BENCHMARK, {"task_ids": ["two"]}),
    ],
)
def test_claim_rejects_operation_id_mismatch(
    database_session: Session,
    kind: MutationOperationKind,
    payload: JSONObject,
) -> None:
    org = _org()
    operation_id = uuid4()
    original_kind = MutationOperationKind.RETRY_OR_RESUME_BENCHMARK
    original_fingerprint = mutation_fingerprint(original_kind, {"task_ids": ["one"]})
    claim_mutation(database_session, org, operation_id, original_kind, original_fingerprint)

    with pytest.raises(HTTPException) as error:
        claim_mutation(
            database_session,
            org,
            operation_id,
            kind,
            mutation_fingerprint(kind, payload),
        )

    assert error.value.status_code == 409


def test_uncertain_claim_never_executes_and_can_be_reconciled(database_session: Session) -> None:
    org = _org()
    operation_id = uuid4()
    kind = MutationOperationKind.RETRY_OR_RESUME_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"task_ids": ["one"]})
    claim_mutation(database_session, org, operation_id, kind, fingerprint)

    uncertain = mark_mutation_uncertain(database_session, org, operation_id)
    duplicate = claim_mutation(database_session, org, operation_id, kind, fingerprint)
    completed = complete_mutation(database_session, org, operation_id, {"run_id": "run-1"})

    assert uncertain == UncertainMutation(kind=kind)
    assert duplicate == UncertainMutation(kind=kind)
    assert completed == SucceededMutation(kind=kind, response={"run_id": "run-1"})


def test_failed_claim_replays_exact_error(database_session: Session) -> None:
    org = _org()
    operation_id = uuid4()
    kind = MutationOperationKind.START_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"benchmark": "missing"})
    claim_mutation(database_session, org, operation_id, kind, fingerprint)

    failed = mark_mutation_failed(database_session, org, operation_id, 404, "Agent is unavailable")
    replay = claim_mutation(database_session, org, operation_id, kind, fingerprint)

    assert failed == FailedMutation(
        kind=kind,
        status_code=404,
        detail="Agent is unavailable",
    )
    assert replay == failed


def test_failed_claim_bounds_stored_detail(database_session: Session) -> None:
    org = _org()
    operation_id = uuid4()
    kind = MutationOperationKind.START_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"benchmark": "missing"})
    claim_mutation(database_session, org, operation_id, kind, fingerprint)

    failed = mark_mutation_failed(
        database_session,
        org,
        operation_id,
        400,
        "x" * (ERROR_EXCERPT_MAX_LENGTH + 1),
    )

    assert failed.detail == "x" * ERROR_EXCERPT_MAX_LENGTH


def test_stale_processing_becomes_durably_uncertain(database_session: Session) -> None:
    org = _org()
    operation_id = uuid4()
    kind = MutationOperationKind.RETRY_OR_RESUME_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"task_ids": ["one"]})
    claim_mutation(database_session, org, operation_id, kind, fingerprint)
    receipt = database_session.get(MutationOperation, (org.id, operation_id))
    assert receipt is not None
    stale_at = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None) - timedelta(minutes=16)
    receipt.created_at = stale_at
    receipt.updated_at = stale_at
    database_session.add(receipt)
    database_session.commit()

    assert get_mutation_status(database_session, org, operation_id) == UncertainMutation(kind=kind)
    assert claim_mutation(database_session, org, operation_id, kind, fingerprint) == UncertainMutation(kind=kind)
    database_session.refresh(receipt)
    assert receipt.state == MutationOperationState.UNCERTAIN


def test_status_is_org_scoped(database_session: Session) -> None:
    owner = _org()
    other = Org(id=uuid4(), name="other")
    database_session.add(other)
    database_session.commit()
    operation_id = uuid4()
    kind = MutationOperationKind.STOP_BENCHMARK
    fingerprint = mutation_fingerprint(kind, {"run_id": "run-1"})
    claim_mutation(database_session, owner, operation_id, kind, fingerprint)

    assert get_mutation_status(database_session, other, operation_id) is None
    assert claim_mutation(database_session, other, operation_id, kind, fingerprint) == ExecuteMutation()


def _org() -> Org:
    return Org(id=TEST_ORG_ID, name="default")
