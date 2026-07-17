"""Factories for CLI response objects with deterministic defaults."""

from datetime import datetime, timezone
from uuid import UUID

from tracker.database.models import (
    AgentContractRequest,
    BenchmarkArguments,
    BenchmarkStatus,
    DocentReadingStatus,
    TaskStatus,
)
from tracker.types import BenchmarkDetails, FetchBenchmarkMetadataResponse, FetchBenchmarkResponse, FinalViewResponse


def make_fetch_response(
    run_id: UUID,
    *,
    status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS,
    finished_tasks: int = 1,
    final_score: float | None = None,
) -> FetchBenchmarkResponse:
    """Build a run response with configurable progress and terminal state."""
    return FetchBenchmarkResponse(
        benchmark_name="swebench",
        benchmark_id=run_id,
        details=BenchmarkDetails(
            status=status,
            started_at=datetime(2026, 7, 9, 12, 30, tzinfo=timezone.utc),
            total_tasks=4,
            finished_tasks=finished_tasks,
            task_breakdown={
                TaskStatus.FINISHED: finished_tasks,
                TaskStatus.IN_PROGRESS: 4 - finished_tasks,
            },
            docent_reading_status=DocentReadingStatus.IDLE,
        ),
        s3_bucket_url="s3://example/run",
        label="release-candidate",
        final_score=final_score,
    )


def make_fetch_metadata(run_id: UUID) -> FetchBenchmarkMetadataResponse:
    """Build run metadata with private contract values used by redaction tests."""
    return FetchBenchmarkMetadataResponse(
        benchmark_id=run_id,
        benchmark_name="swebench",
        benchmark_arguments=BenchmarkArguments(
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
        benchmark_arguments=BenchmarkArguments(
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
    )
