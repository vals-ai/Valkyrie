"""Safe machine-readable snapshots for benchmark runs."""

import json
from collections.abc import Mapping
from datetime import datetime
from math import isfinite
from typing import Any, Literal
from uuid import UUID

from tracker.database.models import BenchmarkStatus, TaskStatus
from tracker.types import (
    BenchmarkStatusEntry,
    BenchmarkTableRow,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
)

from valkyrie.cli.display import progress_percent, utc_isoformat
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService

RunEvent = Literal["snapshot", "update", "complete", "error", "stopped", "disconnect", "interrupted"]


def fetch_run_metadata(tracker: TrackerService, run_id: UUID) -> FetchBenchmarkMetadataResponse | None:
    """Fetch optional identity metadata without making status monitoring fail."""
    try:
        return tracker.fetch_benchmark_metadata(run_id)
    except TrackerServiceError:
        return None


def _finite_score_or_none(value: float | None) -> float | None:
    return value if value is None or isfinite(value) else None


def _progress_fields(finished_tasks: int, total_tasks: int, task_counts: Mapping[Any, int]) -> dict[str, object]:
    return {
        "total_tasks": total_tasks,
        "finished_tasks": finished_tasks,
        "task_state_counts": {status.value: task_counts.get(status.value, 0) for status in TaskStatus},
        "progress_percent": round(progress_percent(finished_tasks, total_tasks), 4),
    }


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
    dataset_version = arguments.dataset_version if arguments is not None else None

    return {
        "schema_version": 1,
        "event": event,
        "observed_at": utc_isoformat(observed_at),
        "run_id": str(response.benchmark_id),
        "benchmark_name": response.benchmark_name,
        "agent_name": contract.name if contract is not None else None,
        "model": contract.model if contract is not None else None,
        "dataset": (arguments.dataset or "default") if arguments is not None else None,
        "dataset_version": dataset_version.model_dump() if dataset_version is not None else None,
        "label": response.label,
        "started_by_email": metadata.started_by_email if metadata is not None else None,
        "started_at": utc_isoformat(details.started_at),
        "status": details.status.value,
        **_progress_fields(details.finished_tasks, details.total_tasks, details.task_breakdown),
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


def format_run_list_json(runs: list[BenchmarkTableRow], *, observed_at: datetime | None = None) -> str:
    """Serialize a complete set of matching runs as one compact JSON document."""
    return _format_json(
        {
            "schema_version": 1,
            "kind": "run_list",
            "observed_at": utc_isoformat(observed_at),
            "returned_count": len(runs),
            "runs": [
                {
                    "run_id": str(run.id),
                    "benchmark_name": run.name,
                    "agent_name": run.agent_name,
                    "model": run.model,
                    "dataset": run.dataset or "default",
                    "label": run.label,
                    "started_by_email": run.started_by_email,
                    "started_at": utc_isoformat(run.started_at),
                    "finished_at": run.finished_at and utc_isoformat(run.finished_at),
                    "status": run.status.value,
                    "final_score": _finite_score_or_none(run.final_score),
                    **_progress_fields(run.finished_tasks, run.total_tasks, run.task_state_counts),
                }
                for run in runs
            ],
        }
    )


def format_run_status_json(
    entries: list[BenchmarkStatusEntry],
    missing_run_ids: list[UUID],
    *,
    requested_count: int,
    observed_at: datetime | None = None,
) -> str:
    """Serialize deterministic multi-run status output as one compact JSON document."""
    return _format_json(
        {
            "schema_version": 1,
            "kind": "run_status",
            "observed_at": utc_isoformat(observed_at),
            "requested_count": requested_count,
            "returned_count": len(entries),
            "missing_run_ids": [str(run_id) for run_id in missing_run_ids],
            "runs": [
                {
                    "run_id": str(entry.id),
                    "status": entry.status.value,
                    "finished_at": entry.finished_at and utc_isoformat(entry.finished_at),
                    **_progress_fields(entry.finished_tasks, entry.total_tasks, entry.task_state_counts),
                }
                for entry in entries
            ],
        }
    )
