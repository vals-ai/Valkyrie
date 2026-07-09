"""Safe machine-readable snapshots for benchmark runs."""

import json
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from tracker.database.models import TaskStatus
from tracker.types import FetchBenchmarkMetadataResponse, FetchBenchmarkResponse

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
