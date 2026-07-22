"""Safe machine-readable snapshots for benchmark runs."""

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from math import isfinite
from typing import Literal
from uuid import UUID

from tracker.database.models import BenchmarkStatus, TaskStatus
from tracker.types import (
    BenchmarkStatusEntry,
    BenchmarkTableRow,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
)

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService

RunEvent = Literal["snapshot", "update", "complete", "error", "stopped", "disconnect", "interrupted"]


def fetch_run_metadata(tracker: TrackerService, run_id: UUID) -> FetchBenchmarkMetadataResponse | None:
    """Fetch optional identity metadata without making status monitoring fail."""
    try:
        return tracker.fetch_benchmark_metadata(run_id)
    except TrackerServiceError:
        return None


def _utc_isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite_score_or_none(value: float | None) -> float | None:
    return value if value is None or isfinite(value) else None


def _format_json(payload: Mapping[str, object]) -> str:
    """Serialize strict compact JSON; NaN and Infinity are never valid machine output."""
    return json.dumps(payload, allow_nan=False, sort_keys=True, separators=(",", ":"))


def build_run_snapshot(
    response: FetchBenchmarkResponse,
    metadata: FetchBenchmarkMetadataResponse | None,
    *,
    event: RunEvent,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Build a versioned allowlisted view that excludes stored contract secrets and kwargs."""
    details = response.details
    arguments = metadata.benchmark_arguments if metadata is not None else None
    contract = arguments.contract if arguments is not None else None
    progress_percent = (details.finished_tasks / details.total_tasks * 100) if details.total_tasks else 0.0

    return {
        "schema_version": 1,
        "event": event,
        "observed_at": _utc_isoformat(observed_at or datetime.now(timezone.utc)),
        "run_id": str(response.benchmark_id),
        "benchmark_name": response.benchmark_name,
        "agent_name": contract.name if contract is not None else None,
        "model": contract.model if contract is not None else None,
        "dataset": (arguments.dataset or "default") if arguments is not None else None,
        "label": response.label,
        "started_by_email": metadata.started_by_email if metadata is not None else None,
        "started_at": _utc_isoformat(details.started_at),
        "status": details.status.value,
        "total_tasks": details.total_tasks,
        "finished_tasks": details.finished_tasks,
        "task_state_counts": {
            task_status.value: details.task_breakdown.get(task_status, 0) for task_status in TaskStatus
        },
        "progress_percent": round(progress_percent, 4),
        "max_concurrency": arguments.concurrency if arguments is not None else None,
        "final_score": _finite_score_or_none(response.final_score),
        "error_message": response.error_message if details.status == BenchmarkStatus.ERROR else None,
        "s3_bucket_url": response.s3_bucket_url,
        "docent_reading_status": details.docent_reading_status.value,
        "docent_reading_url": details.docent_reading_url,
        "metadata_available": metadata is not None,
    }


def format_run_snapshot_json(
    response: FetchBenchmarkResponse,
    metadata: FetchBenchmarkMetadataResponse | None,
    *,
    event: RunEvent,
) -> str:
    """Serialize one compact JSON or JSONL record."""
    return _format_json(build_run_snapshot(response, metadata, event=event))


def build_run_summary(run: BenchmarkTableRow) -> dict[str, object]:
    """Build a stable allowlisted run summary for list output."""
    progress_percent = (run.finished_tasks / run.total_tasks * 100) if run.total_tasks else 0.0
    return {
        "run_id": str(run.id),
        "benchmark_name": run.name,
        "agent_name": run.agent_name,
        "model": run.model,
        "dataset": run.dataset or "default",
        "label": run.label,
        "started_by_email": run.started_by_email,
        "started_at": _utc_isoformat(run.started_at),
        "finished_at": _utc_isoformat(run.finished_at) if run.finished_at is not None else None,
        "status": run.status.value,
        "total_tasks": run.total_tasks,
        "finished_tasks": run.finished_tasks,
        "task_state_counts": {
            task_status.value: run.task_state_counts.get(task_status.value, 0) for task_status in TaskStatus
        },
        "progress_percent": round(progress_percent, 4),
        "final_score": _finite_score_or_none(run.final_score),
    }


def format_run_list_json(runs: list[BenchmarkTableRow], *, observed_at: datetime | None = None) -> str:
    """Serialize a complete set of matching runs as one compact JSON document."""
    payload = {
        "schema_version": 1,
        "kind": "run_list",
        "observed_at": _utc_isoformat(observed_at or datetime.now(timezone.utc)),
        "returned_count": len(runs),
        "runs": [build_run_summary(run) for run in runs],
    }
    return _format_json(payload)


def build_run_status(entry: BenchmarkStatusEntry) -> dict[str, object]:
    """Build a stable allowlisted batch-status record."""
    progress_percent = (entry.finished_tasks / entry.total_tasks * 100) if entry.total_tasks else 0.0
    return {
        "run_id": str(entry.id),
        "status": entry.status.value,
        "finished_at": _utc_isoformat(entry.finished_at) if entry.finished_at is not None else None,
        "total_tasks": entry.total_tasks,
        "finished_tasks": entry.finished_tasks,
        "task_state_counts": {
            task_status.value: entry.task_state_counts.get(task_status.value, 0) for task_status in TaskStatus
        },
        "progress_percent": round(progress_percent, 4),
    }


def format_run_status_json(
    entries: list[BenchmarkStatusEntry],
    missing_run_ids: list[UUID],
    *,
    requested_count: int,
    observed_at: datetime | None = None,
) -> str:
    """Serialize deterministic multi-run status output as one compact JSON document."""
    payload = {
        "schema_version": 1,
        "kind": "run_status",
        "observed_at": _utc_isoformat(observed_at or datetime.now(timezone.utc)),
        "requested_count": requested_count,
        "returned_count": len(entries),
        "missing_run_ids": [str(run_id) for run_id in missing_run_ids],
        "runs": [build_run_status(entry) for entry in entries],
    }
    return _format_json(payload)
