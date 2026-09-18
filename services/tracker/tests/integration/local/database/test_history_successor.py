"""Completed history controls admit only exact locked deletion successors."""

import asyncio
import json
from typing import Any
from uuid import uuid4

import pytest
from sqlmodel import Session

from tests.integration.local.database.test_run_purge import MemoryBoundary, contract
from tests.integration.local.database.test_run_relocation import execute, seed
from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, require_unheld
from tracker.run_purge import PurgeOperator, build_plan


def completed_history(session: Session) -> tuple[PurgeOperator, MemoryBoundary, dict[str, Any]]:
    _, _, request = seed(session)
    execute(session, request, "prepare")
    execute(session, request, "relocate")
    request["completion_sha256"] = "c" * 64
    execute(session, request, "release")
    identity = OperationIdentity.model_validate(request["plan"]["identity"]).model_copy(
        update={"operation_id": uuid4()}
    )
    boundary = MemoryBoundary()
    return PurgeOperator(session, build_plan(session, identity), boundary, host_contract=contract()), boundary, request


def test_history_inspection_then_exact_deletion_without_release(postgres_session: Session) -> None:
    operator, boundary, old_request = completed_history(postgres_session)
    run_id = operator.plan.identity.run_ids[0]
    predecessor = operator.plan.runs[0].completed_history
    assert predecessor is not None and predecessor.completion_sha256
    record = postgres_session.get(RunLifecycle, run_id)
    assert record is not None
    before = record.model_dump()
    observation = asyncio.run(operator.inspect(request_nonce=uuid4())).runs[0]
    assert observation.state == "present_history_held"
    assert observation.completed_history == predecessor
    postgres_session.refresh(record)
    assert record.model_dump() == before
    asyncio.run(operator.prepare())
    with pytest.raises(LifecycleConflict):
        require_unheld(postgres_session, run_id)
    with pytest.raises(LifecycleConflict):
        execute(postgres_session, old_request, "release")
    boundary.fenced = True
    asyncio.run(operator.purge())
    assert postgres_session.get(Benchmark, run_id) is None
    assert postgres_session.get_one(RunLifecycle, run_id).purpose == "deletion"


@pytest.mark.parametrize("change", ["checkpoint", "phase", "identity", "scope", "new_operation"])
def test_changed_history_predecessor_refuses_before_cleanup(postgres_session: Session, change: str) -> None:
    operator, boundary, _ = completed_history(postgres_session)
    run_id = operator.plan.identity.run_ids[0]
    record = postgres_session.get(RunLifecycle, run_id)
    assert record is not None
    if change == "phase":
        record.phase = "prepared"
    elif change == "checkpoint":
        payload = json.loads(record.checkpoint_json or "null")
        payload["parent_completion_sha256"] = "d" * 64
        record.checkpoint_json = json.dumps(payload)
    elif change in {"identity", "new_operation"}:
        payload = json.loads(record.identity_json)
        payload["github_owner_id" if change == "identity" else "operation_id"] = (
            99 if change == "identity" else str(uuid4())
        )
        record.identity_json = json.dumps(payload)
    else:
        payload = json.loads(record.scope_json)
        payload["original_resources"]["s3_bucket"] = "other-bucket"
        record.scope_json = json.dumps(payload)
    postgres_session.add(record)
    postgres_session.commit()
    saved = record.model_dump()
    with pytest.raises((LifecycleConflict, ValueError)):
        asyncio.run(operator.inspect(request_nonce=uuid4()))
    with pytest.raises((LifecycleConflict, ValueError)):
        asyncio.run(operator.prepare())
    assert "cleanup" not in boundary.calls
    postgres_session.refresh(record)
    assert record.model_dump() == saved


def test_cached_history_control_is_refreshed_before_successor(postgres_session: Session) -> None:
    operator, boundary, _ = completed_history(postgres_session)
    run_id = operator.plan.identity.run_ids[0]
    cached = postgres_session.get_one(RunLifecycle, run_id)
    postgres_session.rollback()
    with Session(postgres_session.get_bind()) as concurrent:
        record = concurrent.get_one(RunLifecycle, run_id)
        checkpoint = json.loads(record.checkpoint_json or "null")
        checkpoint["parent_completion_sha256"] = "f" * 64
        record.checkpoint_json = json.dumps(checkpoint)
        concurrent.add(record)
        concurrent.commit()
    with pytest.raises(LifecycleConflict, match="predecessor changed"):
        asyncio.run(operator.prepare())
    assert "cleanup" not in boundary.calls
    postgres_session.refresh(cached)
    assert cached.phase == "relocated_history_only"
