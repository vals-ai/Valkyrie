"""Observed-event integrity never authorizes production history completion."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.unit.test_transfer_archive import archive_boundary
from tracker.aws.log_history_archive import archive_logs, read_events, read_manifest
from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer.providers import TransferAWSBoundary
from tracker.run_transfer.rows import digest
from tracker.runtime.log_history import FrozenLogScope


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["matching", "empty", "absent"])
@pytest.mark.parametrize("operation", ["archive", "verify_archive", "verify_objects", "cleanup_logs"])
async def test_production_refuses_unproved_completeness_even_with_verified_observed_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str, operation: str
) -> None:
    request, transport, logs, storage = archive_boundary(tmp_path)
    if state == "empty":
        logs.events = []
    elif state == "absent":
        logs.absent = True
    deleted: list[str] = []

    def delete_log_group(**arguments: Any) -> None:
        deleted.append(arguments["logGroupName"])
        logs.absent = True

    monkeypatch.setattr(logs, "delete_log_group", delete_log_group, raising=False)
    boundary = TransferAWSBoundary(
        transport.source,
        transport.destination,
        tmp_path / "production-journal",
        source_session=transport.source_session,
        destination_session=transport.destination_session,
    )
    run = request.plan.runs[0]
    scope = FrozenLogScope(
        source_identity=request.plan.source_identity,
        destination_identity=request.plan.destination_identity,
        source=run.source,
        destination=run.destination,
        freeze_evidence_sha256=digest(
            {"child_plan_sha256": request.plan.sha256, "source_rows_sha256": run.source_rows_sha256}
        ),
        unmasked_read_authorized=True,
    )
    archive = archive_logs(
        scope,
        source_session=transport.source_session,
        destination_session=transport.destination_session,
        journal_directory=tmp_path / "observed-journal",
    )
    manifest = read_manifest(archive.reference, scope, transport.destination_session)
    assert len(list(read_events(manifest, scope, transport.destination_session))) == (2 if state == "matching" else 0)
    saved = dict(storage.objects)
    # Object storage is independent of the real archive-acceptance boundary under test.
    monkeypatch.setattr("tracker.run_transfer.providers.RelocationAWSBoundary.verify_objects", AsyncMock())

    with pytest.raises(LifecycleConflict, match="completeness"):
        if operation == "archive":
            await boundary.archive(request, run)
        elif operation == "verify_archive":
            await boundary.verify_archive(request, run, archive)
        elif operation == "verify_objects":
            await boundary.verify_objects(request, run, archive=archive)
        else:
            await boundary.cleanup_logs(request, run, archive)

    assert not deleted
    assert storage.objects == saved
    assert logs.absent == (state == "absent")
    assert not (tmp_path / "production-journal").exists()
