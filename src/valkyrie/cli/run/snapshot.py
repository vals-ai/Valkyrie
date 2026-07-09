"""Safe machine-readable snapshots for benchmark runs."""

import json
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from tracker.database.models import TaskStatus
from tracker.types import BenchmarkTableRow, FetchBenchmarkMetadataResponse, FetchBenchmarkResponse

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
        "final_score": response.final_score,
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
    return json.dumps(build_run_snapshot(response, metadata, event=event), sort_keys=True, separators=(",", ":"))


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
        "final_score": run.final_score,
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
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
