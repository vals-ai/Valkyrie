"""Read-only transfer inspection binds current and retained parent authorization."""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.integration.local.database.test_run_transfer import pair as pair
from tests.integration.local.database.test_run_transfer import seed_rows
from tests.transfer_support import OBSERVED_ACQUIRED_AT, OBSERVED_DECISION, FakeTransferBoundary, transfer_request
from tracker.database.models import BenchmarkStatus, RunLifecycle
from tracker.lifecycle import LifecycleConflict
from tracker.lifecycle_evidence import DispatchDrain
from tracker.run_transfer import TransferOperator
from tracker.run_transfer.contracts import TransferRequest, TransferResponse, TransferRun
from tracker.run_transfer.rows import digest
from tracker.runtime.log_history import ArchiveReport


class InterruptedCleanup(FakeTransferBoundary):
    stop_run_id: UUID | None = None
    inspection = False

    async def archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
    ) -> ArchiveReport:
        assert not self.inspection, "inspection must not upload archives"
        return await super().archive(request, run, dispatches=dispatches, acquired_at=acquired_at)

    async def cleanup_logs(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
        log_completeness_sha256: str | None = OBSERVED_DECISION,
    ) -> None:
        assert not self.inspection, "inspection must not remove source logs"
        if run.source.run_id == self.stop_run_id:
            raise RuntimeError("interrupted before second source cleanup")
        await super().cleanup_logs(
            request,
            run,
            archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )

    async def drain(
        self, request: TransferRequest, run: TransferRun, arguments: dict[str, Any], *, cleanup: bool = False
    ) -> None:
        assert not (self.inspection and cleanup), "inspection must not clean up providers"
        await super().drain(request, run, arguments, cleanup=cleanup)


def execute(operator: TransferOperator, payload: dict[str, Any], action: str) -> TransferResponse:
    payload["action"] = action
    return asyncio.run(operator.execute(TransferRequest.model_validate(payload)))


def imported_pair(
    pair: tuple[Session, Session], tmp_path: Path, *, multiple: bool = False
) -> tuple[TransferOperator, InterruptedCleanup, dict[str, Any]]:
    source, destination = pair
    org, run, _ = seed_rows(source, destination)
    payload = transfer_request(source, destination, org, run)
    if multiple:
        another = make_benchmark(org_id=org.id, status=BenchmarkStatus.FINISHED)
        source.add(another)
        source.commit()
        extra = transfer_request(source, destination, org, another)
        payload["plan"]["runs"] += extra["plan"]["runs"]
        runs: list[dict[str, Any]] = payload["plan"]["runs"]
        runs.sort(key=lambda item: item["source"]["run_id"])
        for side in ("source_identity", "destination_identity"):
            payload["plan"][side]["run_ids"] = [item["source"]["run_id"] for item in payload["plan"]["runs"]]
    boundary = InterruptedCleanup(tmp_path)
    operator = TransferOperator(source, destination, boundary)
    planned = execute(operator, payload, "plan")
    for item, observed in zip(payload["plan"]["runs"], planned.runs, strict=True):
        item["source_rows_sha256"] = observed.source_rows_sha256
    execute(operator, payload, "prepare")
    imported = execute(operator, payload, "import")
    plan = TransferRequest.model_validate(payload).plan
    payload["parent_completion"] = {
        "operation_id": str(plan.source_identity.operation_id),
        "parent_plan_sha256": plan.source_identity.parent_plan_sha256,
        "child_plan_sha256": plan.sha256,
        "valsmith_commit_sha256": "e" * 64,
        "object_completion_sha256": "f" * 64,
        "destination_rows_sha256": digest(
            [{"run_id": str(item.run_id), "sha256": item.destination_rows_sha256} for item in imported.runs]
        ),
        "archives_sha256": digest(
            [item.archive.model_dump(mode="json") for item in imported.runs if item.archive is not None]
        ),
    }
    return operator, boundary, payload


def snapshot(pair: tuple[Session, Session]) -> list[list[list[Any]]]:
    return [
        [
            list(
                session.connection()
                .execute(text(f'SELECT to_jsonb(record) FROM "{table}" AS record ORDER BY to_jsonb(record)::text'))
                .scalars()
            )
            for table in ("benchmark", "task", "runlifecycle")
        ]
        for session in pair
    ]


@pytest.mark.parametrize("phase", ["imported", "partial_cleanup", "cleaned", "finalized"])
@pytest.mark.parametrize("supplied", [False, True])
def test_inspection_binds_parent_evidence_without_mutation(
    pair: tuple[Session, Session], tmp_path: Path, phase: str, supplied: bool
) -> None:
    operator, boundary, payload = imported_pair(pair, tmp_path, multiple=True)
    if phase == "partial_cleanup":
        boundary.stop_run_id = TransferRequest.model_validate(payload).plan.runs[1].source.run_id
        with pytest.raises(RuntimeError, match="interrupted"):
            execute(operator, payload, "cleanup")
    elif phase in {"cleaned", "finalized"}:
        execute(operator, payload, "cleanup")
        if phase == "finalized":
            execute(operator, payload, "finalize")
    expected = digest(payload["parent_completion"]) if supplied else None
    if not supplied:
        payload["parent_completion"] = None
    before = snapshot(pair)
    boundary.inspection = True

    response = execute(operator, payload, "inspect")

    assert response.parent_completion_sha256 == expected
    assert snapshot(pair) == before
    assert len(response.runs) == 2
    if phase == "partial_cleanup":
        assert [run.source_phase for run in response.runs] == ["transferred_source_retired", "transferred"]
    if phase == "finalized":
        assert all(run.destination_phase == "transferred_history_only" for run in response.runs)


@pytest.mark.parametrize(
    "fault", ["source_hash", "destination_hash", "rows", "archives", "operation", "parent_plan", "child_plan"]
)
def test_inspection_rejects_changed_parent_authorization_without_mutation(
    pair: tuple[Session, Session], tmp_path: Path, fault: str
) -> None:
    operator, boundary, payload = imported_pair(pair, tmp_path)
    execute(operator, payload, "cleanup")
    execute(operator, payload, "finalize")
    run_id = TransferRequest.model_validate(payload).plan.runs[0].source.run_id
    if fault in {"source_hash", "destination_hash"}:
        session = pair[0] if fault == "source_hash" else pair[1]
        record = session.get_one(RunLifecycle, run_id)
        checkpoint = json.loads(record.checkpoint_json or "null")
        checkpoint["parent_completion_sha256"] = "b" * 64
        record.checkpoint_json = json.dumps(checkpoint)
        session.add(record)
        session.commit()
    else:
        field = {
            "rows": "destination_rows_sha256",
            "archives": "archives_sha256",
            "operation": "operation_id",
            "parent_plan": "parent_plan_sha256",
            "child_plan": "child_plan_sha256",
        }[fault]
        payload["parent_completion"][field] = str(UUID(int=999)) if fault == "operation" else "b" * 64
    before = snapshot(pair)
    boundary.inspection = True

    with pytest.raises(LifecycleConflict, match="completion|Parent"):
        execute(operator, payload, "inspect")

    assert snapshot(pair) == before
