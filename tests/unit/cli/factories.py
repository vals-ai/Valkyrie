"""Factories for Tracker responses with deterministic defaults."""

import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
from pydantic import BaseModel
from valkyrie.sdk.models import (
    AgentContractRequest,
    AWSBenchmarkArguments,
    BenchmarkDetails,
    BenchmarkStatus,
    DocentReadingStatus,
    FailureCategory,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FinalViewResponse,
    TaskStatus,
)

from tests.unit.cli.conftest import MockTracker


def make_fetch_response(
    run_id: UUID,
    *,
    status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS,
    total_tasks: int = 4,
    finished_tasks: int = 1,
    task_breakdown: dict[TaskStatus, int] | None = None,
    final_score: float | None = None,
    error_message: str | None = None,
) -> FetchBenchmarkResponse:
    """Build a run response with configurable progress and terminal state."""
    return FetchBenchmarkResponse(
        benchmark_name="swebench",
        benchmark_id=run_id,
        details=BenchmarkDetails(
            status=status,
            started_at=datetime(2026, 7, 9, 12, 30, tzinfo=timezone.utc),
            total_tasks=total_tasks,
            finished_tasks=finished_tasks,
            task_breakdown=task_breakdown
            if task_breakdown is not None
            else {
                TaskStatus.FINISHED: finished_tasks,
                TaskStatus.IN_PROGRESS: total_tasks - finished_tasks,
            },
            docent_reading_status=DocentReadingStatus.IDLE,
        ),
        s3_bucket_url="s3://example/run",
        label="release-candidate",
        final_score=final_score,
        error_message=error_message,
    )


def make_fetch_metadata(run_id: UUID) -> FetchBenchmarkMetadataResponse:
    """Build run metadata with private contract values used by redaction tests."""
    return FetchBenchmarkMetadataResponse(
        benchmark_id=run_id,
        benchmark_name="swebench",
        benchmark_arguments=AWSBenchmarkArguments(
            contract=AgentContractRequest(
                name="mini_sweagent",
                model="openai/gpt-5",
                secrets={"MODEL_API_KEY": "classified-secret-name"},
                kwargs={"temperature": "0"},
            ),
            concurrency=20,
            dataset="verified",
        ),
        started_by_email="runner@vals.ai",
    )


def make_final_view(
    run_id: UUID,
    *,
    status: BenchmarkStatus = BenchmarkStatus.ERROR,
    error_message: str | None = "No tasks were completed successfully",
    task_errors: dict[str, str] | None = None,
    task_failure_categories: dict[str, FailureCategory] | None = None,
    evaluation_results: dict[str, dict[str, object]] | None = None,
) -> FinalViewResponse:
    """Build a complete final-view response for CLI behavior tests."""
    return FinalViewResponse(
        benchmark_id=run_id,
        benchmark_name="demo-bench",
        started_at=datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 10, 12, 5, tzinfo=timezone.utc),
        status=status,
        error_message=error_message,
        benchmark_arguments=AWSBenchmarkArguments(
            contract=AgentContractRequest(
                name="demo-agent",
                secrets={"SYNTHETIC_KEY": "excluded-secret-name"},
                kwargs={"private-option": "excluded-kwarg-value"},
            ),
            concurrency=10,
        ),
        tasks_stopped=None,
        final_evaluation=None,
        average_task_breakdown=None,
        evaluation_results=evaluation_results or {"successful-task": {"private": "excluded-evaluation-value"}},
        task_errors=task_errors,
        task_failure_categories=task_failure_categories,
    )


def ok(model: BaseModel) -> httpx.Response:
    """A 200 response carrying `model` as JSON; non-finite floats go out as NaN/Infinity."""
    return httpx.Response(
        200, content=json.dumps(model.model_dump(mode="json")), headers={"content-type": "application/json"}
    )


def run_stream(*updates: FetchBenchmarkResponse, end: str | None = "complete") -> httpx.Response:
    """A run update stream: one `data:` event per update, then `event: <end>` unless `end` is None."""
    body = "".join(f"data: {json.dumps(update.model_dump(mode='json'))}\n\n" for update in updates)
    if end is not None:
        body += f"event: {end}\n\n"

    return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})


def serve_run(
    tracker: MockTracker,
    run: FetchBenchmarkResponse,
    *,
    stream: httpx.Response | BaseException | None = None,
    metadata: FetchBenchmarkMetadataResponse | None = None,
) -> None:
    """Answer fetches of `run`, its update stream, and its metadata (404 when `metadata` is None)."""

    def fetch(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("connect") != "true":
            return ok(run)
        if isinstance(stream, BaseException):
            raise stream

        return stream or run_stream()

    tracker.reply("GET", "/fetch-benchmark", fetch)
    if metadata is not None:
        tracker.reply("GET", f"/fetch-benchmark-metadata/{run.benchmark_id}", ok(metadata))
