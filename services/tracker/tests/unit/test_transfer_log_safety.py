"""Quiet-interval completeness policy: every clause must keep the operation pending."""

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from tests.unit.test_transfer_archive import archive_boundary
from tracker.aws.log_history_archive import archive_logs, read_manifest
from tracker.lifecycle import LifecycleConflict
from tracker.lifecycle_evidence import DispatchDrain
from tracker.run_transfer import settings
from tracker.run_transfer.contracts import TransferRequest, TransferRun
from tracker.run_transfer.providers import TransferAWSBoundary
from tracker.run_transfer.rows import digest
from tracker.run_transfer.settings import LOG_QUIET_INTERVAL_FLOOR_HOURS, load_settings
from tracker.runtime.log_history import ArchiveReport, FrozenLogScope, LogHistoryManifest

DISPATCH_ID = UUID("00000000-0000-0000-0000-0000000000dd")
QUIET_EXIT = timedelta(days=2)


class QuietRun:
    """A legacy run whose every quiet-interval clause passes, with its archive published."""

    def __init__(self, request: TransferRequest, boundary: TransferAWSBoundary, logs: Any, storage: Any) -> None:
        self.request = request
        self.boundary = boundary
        self.logs = logs
        self.storage = storage
        self.deleted: list[str] = []
        self.archive: ArchiveReport
        self.dispatches = drained(now() - QUIET_EXIT)
        self.acquired_at = now() - QUIET_EXIT

    @property
    def run(self) -> TransferRun:
        return self.request.plan.runs[0]


def now() -> datetime:
    return datetime.now(UTC)


@pytest.fixture
def lower_quiet_interval(monkeypatch: pytest.MonkeyPatch) -> Callable[[int], None]:
    """Replace the frozen settings object; the deployed one is never mutated."""

    def lower(hours: int) -> None:
        monkeypatch.setattr(settings, "SETTINGS", replace(settings.SETTINGS, log_quiet_interval_hours=hours))

    return lower


def drained(exit_at: datetime) -> tuple[DispatchDrain, ...]:
    return (DispatchDrain(dispatch_id=DISPATCH_ID, provenance="host_process_exit", observed_exit_at=exit_at),)


def quiet_request(
    request: TransferRequest, *, observation: datetime, acknowledged: timedelta = timedelta(days=7)
) -> TransferRequest:
    payload = request.model_dump(mode="json")
    payload["source_host_contract"] = {
        "contract": "stable-host-lifecycle-v1",
        "deployment_sha256": "b" * 64,
        "host_inventory": ["host-1"],
        "observed_at": observation.isoformat(),
        "acknowledgement_required_since": (observation - acknowledged).isoformat(),
        "verifier": "local-test",
    }
    return TransferRequest.model_validate(payload)


def quiet_scope(request: TransferRequest, run: TransferRun) -> FrozenLogScope:
    return FrozenLogScope(
        source_identity=request.plan.source_identity,
        destination_identity=request.plan.destination_identity,
        source=run.source,
        destination=run.destination,
        freeze_evidence_sha256=digest(
            {"child_plan_sha256": request.plan.sha256, "source_rows_sha256": run.source_rows_sha256}
        ),
        unmasked_read_authorized=True,
    )


def quiet_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[dict[str, Any]] | None = None,
    publish: bool = True,
) -> QuietRun:
    request, transport, logs, storage = archive_boundary(tmp_path)
    if events is not None:
        logs.events = events
    request = quiet_request(request, observation=now())
    boundary = TransferAWSBoundary(
        transport.source,
        transport.destination,
        tmp_path / "production-journal",
        source_session=transport.source_session,
        destination_session=transport.destination_session,
    )
    state = QuietRun(request, boundary, logs, storage)

    def delete_log_group(**arguments: Any) -> None:
        state.deleted.append(arguments["logGroupName"])
        logs.absent = True

    monkeypatch.setattr(logs, "delete_log_group", delete_log_group, raising=False)
    if publish:
        state.archive = archive_logs(
            quiet_scope(request, state.run),
            source_session=transport.source_session,
            destination_session=transport.destination_session,
            journal_directory=tmp_path / "published-journal",
        )
    # The paired object verifier has its own tests; only the completeness policy is under test here.
    monkeypatch.setattr("tracker.run_transfer.providers.RelocationAWSBoundary.verify_objects", AsyncMock())
    return state


@pytest.mark.asyncio
async def test_a_quiet_legacy_run_archives_verifies_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = quiet_run(tmp_path, monkeypatch, publish=False)
    archive, decision = await state.boundary.archive(
        state.request, state.run, dispatches=state.dispatches, acquired_at=state.acquired_at
    )
    await state.boundary.verify_archive(
        state.request,
        state.run,
        archive,
        dispatches=state.dispatches,
        acquired_at=state.acquired_at,
        log_completeness_sha256=decision,
    )
    await state.boundary.verify_objects(
        state.request,
        state.run,
        archive=archive,
        dispatches=state.dispatches,
        acquired_at=state.acquired_at,
        log_completeness_sha256=decision,
    )
    await state.boundary.cleanup_logs(
        state.request,
        state.run,
        archive,
        dispatches=state.dispatches,
        acquired_at=state.acquired_at,
        log_completeness_sha256=decision,
    )

    assert len(decision) == 64
    assert state.deleted == [state.run.source.log_group]
    assert state.logs.absent
    assert state.storage.objects


@pytest.mark.asyncio
@pytest.mark.parametrize("state_name", ["empty", "absent"])
async def test_a_quiet_group_without_events_satisfies_the_scan_clause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state_name: str
) -> None:
    state = quiet_run(tmp_path, monkeypatch, events=[], publish=False)
    if state_name == "absent":
        state.logs.absent = True
    _, decision = await state.boundary.archive(
        state.request, state.run, dispatches=state.dispatches, acquired_at=state.acquired_at
    )

    assert len(decision) == 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("clause", "fault"),
    [
        ("host_observation", "missing_host"),
        ("host_observation", "expired_host"),
        ("dispatch_drain", "pending_drain"),
        ("dispatch_drain", "recent_exit"),
        ("dispatch_drain", "recent_contract"),
        ("dispatch_drain", "no_drain_time"),
        ("hold_quiet_interval", "recent_hold"),
    ],
)
@pytest.mark.parametrize("operation", ["archive", "verify_archive", "verify_objects", "cleanup_logs"])
async def test_a_failed_quiet_input_clause_keeps_every_operation_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clause: str, fault: str, operation: str
) -> None:
    state = quiet_run(tmp_path, monkeypatch)
    request, dispatches, acquired_at = state.request, state.dispatches, state.acquired_at
    if fault == "missing_host":
        request = request.model_copy(update={"source_host_contract": None})
    elif fault == "expired_host":
        request = quiet_request(request, observation=now() - timedelta(hours=1))
    elif fault == "pending_drain":
        dispatches = (DispatchDrain(dispatch_id=DISPATCH_ID, provenance="pending"),)
    elif fault == "recent_exit":
        dispatches = drained(now() - timedelta(hours=1))
    elif fault == "recent_contract":
        request = quiet_request(request, observation=now(), acknowledged=timedelta(hours=1))
        dispatches = (DispatchDrain(dispatch_id=DISPATCH_ID, provenance="held_unclaimed"),)
    elif fault == "no_drain_time":
        dispatches = (DispatchDrain(dispatch_id=DISPATCH_ID, provenance="externally_confirmed_host_drain"),)
    else:
        acquired_at = now() - timedelta(hours=1)
    saved = dict(state.storage.objects)

    with pytest.raises(LifecycleConflict, match=f"clause {clause} failed"):
        await run_operation(state, operation, request=request, dispatches=dispatches, acquired_at=acquired_at)

    assert not state.deleted
    assert state.storage.objects == saved
    assert not state.logs.absent


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["recent_hold", "recent_events"])
async def test_a_refused_archive_writes_no_chunk_and_no_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    recent = int((now() - timedelta(minutes=5)).timestamp() * 1000)
    events = [{"timestamp": recent, "ingestionTime": recent, "message": "late", "eventId": "x", "logStreamName": "old"}]
    state = quiet_run(tmp_path, monkeypatch, events=events if fault == "recent_events" else None, publish=False)
    acquired_at = now() - timedelta(hours=1) if fault == "recent_hold" else state.acquired_at
    clause = "hold_quiet_interval" if fault == "recent_hold" else "scan_quiet_interval"

    with pytest.raises(LifecycleConflict, match=f"clause {clause} failed"):
        await state.boundary.archive(state.request, state.run, dispatches=state.dispatches, acquired_at=acquired_at)

    assert state.storage.objects == {}
    assert not (tmp_path / "production-journal").exists()
    assert state.logs.scan == (0 if fault == "recent_hold" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("provenance", ["held_unclaimed", "verified_finished_contract"])
async def test_a_contract_drain_older_than_the_interval_satisfies_the_drain_clause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provenance: str
) -> None:
    state = quiet_run(tmp_path, monkeypatch, publish=False)
    dispatches = (DispatchDrain.model_validate({"dispatch_id": DISPATCH_ID, "provenance": provenance}),)
    _, decision = await state.boundary.archive(
        state.request, state.run, dispatches=dispatches, acquired_at=state.acquired_at
    )

    assert len(decision) == 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("clause", "fault"),
    [
        ("scan_quiet_interval", "recent_events"),
        ("matching_scans", "changed_second_scan"),
        ("persisted_decision", "wrong_decision"),
    ],
)
@pytest.mark.parametrize("operation", ["verify_archive", "verify_objects", "cleanup_logs"])
async def test_a_failed_manifest_clause_keeps_every_acceptance_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clause: str, fault: str, operation: str
) -> None:
    recent = int((now() - timedelta(minutes=5)).timestamp() * 1000)
    events = (
        [{"timestamp": recent, "ingestionTime": recent, "message": "late", "eventId": "x", "logStreamName": "old"}]
        if fault == "recent_events"
        else None
    )
    state = quiet_run(tmp_path, monkeypatch, events=events)
    if fault == "changed_second_scan":
        manifest = read_manifest(
            state.archive.reference, quiet_scope(state.request, state.run), state.boundary.destination_session
        )
        changed = manifest.model_copy(
            update={"second_scan": manifest.second_scan.model_copy(update={"newest_ingestion_ms": 99})}
        )

        def read_changed_manifest(*_arguments: Any, **_options: Any) -> LogHistoryManifest:
            return changed

        monkeypatch.setattr("tracker.run_transfer.providers.read_manifest", read_changed_manifest)
    saved = dict(state.storage.objects)

    with pytest.raises(LifecycleConflict, match=f"clause {clause} failed"):
        await run_operation(state, operation)

    assert not state.deleted
    assert state.storage.objects == saved
    assert not state.logs.absent


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["verify_archive", "verify_objects", "cleanup_logs"])
async def test_a_checkpoint_without_the_persisted_decision_keeps_the_operation_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    state = quiet_run(tmp_path, monkeypatch)
    saved = dict(state.storage.objects)

    with pytest.raises(LifecycleConflict, match="clause persisted_decision failed"):
        await run_operation(state, operation, log_completeness_sha256=None)

    assert not state.deleted
    assert state.storage.objects == saved
    assert not state.logs.absent


@pytest.mark.asyncio
async def test_resume_with_a_fresh_host_observation_reuses_the_persisted_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = quiet_run(tmp_path, monkeypatch, publish=False)
    state.archive, decision = await state.boundary.archive(
        state.request, state.run, dispatches=state.dispatches, acquired_at=state.acquired_at
    )
    resumed = quiet_request(state.request, observation=now())

    await run_operation(state, "verify_archive", request=resumed, log_completeness_sha256=decision)


@pytest.mark.asyncio
async def test_a_lowered_quiet_interval_invalidates_the_persisted_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lower_quiet_interval: Callable[[int], None]
) -> None:
    state = quiet_run(tmp_path, monkeypatch, publish=False)
    state.archive, decision = await state.boundary.archive(
        state.request, state.run, dispatches=state.dispatches, acquired_at=state.acquired_at
    )
    lower_quiet_interval(1)

    with pytest.raises(LifecycleConflict, match="clause persisted_decision failed"):
        await run_operation(state, "verify_archive", log_completeness_sha256=decision)


def test_the_deployed_quiet_interval_cannot_go_below_the_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRANSFER_LOG_QUIET_INTERVAL_HOURS", raising=False)

    assert load_settings().log_quiet_interval_hours == LOG_QUIET_INTERVAL_FLOOR_HOURS

    monkeypatch.setenv("TRANSFER_LOG_QUIET_INTERVAL_HOURS", str(LOG_QUIET_INTERVAL_FLOOR_HOURS - 1))
    with pytest.raises(ValueError, match="at least 24"):
        load_settings()

    monkeypatch.setenv("TRANSFER_LOG_QUIET_INTERVAL_HOURS", "48")

    assert load_settings().log_quiet_interval == timedelta(hours=48)


async def run_operation(
    state: QuietRun,
    operation: str,
    *,
    request: TransferRequest | None = None,
    dispatches: tuple[DispatchDrain, ...] | None = None,
    acquired_at: datetime | None = None,
    log_completeness_sha256: str | None = "f" * 64,
) -> str:
    request = state.request if request is None else request
    dispatches = state.dispatches if dispatches is None else dispatches
    acquired_at = state.acquired_at if acquired_at is None else acquired_at
    if operation == "archive":
        _ = await state.boundary.archive(request, state.run, dispatches=dispatches, acquired_at=acquired_at)
        return ""

    if operation == "verify_archive":
        await state.boundary.verify_archive(
            request,
            state.run,
            state.archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )
        return ""

    if operation == "verify_objects":
        await state.boundary.verify_objects(
            request,
            state.run,
            archive=state.archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )
        return ""

    await state.boundary.cleanup_logs(
        request,
        state.run,
        state.archive,
        dispatches=dispatches,
        acquired_at=acquired_at,
        log_completeness_sha256=log_completeness_sha256,
    )
    return ""
