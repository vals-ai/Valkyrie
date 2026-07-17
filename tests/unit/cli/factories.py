"""Factories for CLI response objects with deterministic defaults."""

from datetime import datetime, timezone
from uuid import UUID

from tracker.database.models import AgentContractRequest, BenchmarkArguments, BenchmarkStatus
from tracker.types import FinalViewResponse


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
