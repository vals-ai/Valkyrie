"""Run with `uv run pytest tests/integration/local/api/test_single_task.py`.

Exercise single-task routes through the real app and local database.
"""

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Task,
    TaskStatus,
)

_UTC = ZoneInfo("UTC")


def _persist_benchmark_with_task(
    database_session: Session,
    task_id: str = "astropy__12907",
) -> tuple[Benchmark, Task]:
    """Persist a benchmark and finished task for one route scenario."""
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    database_session.add(benchmark)
    database_session.commit()
    task = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id=task_id,
        status=TaskStatus.FINISHED,
    )
    database_session.add(task)
    database_session.commit()

    return benchmark, task


def _created_at(hour: int) -> datetime:
    """Return a stable timestamp for ordering result history."""
    return datetime(2026, 6, 24, hour, tzinfo=_UTC)


def _evaluation_result(
    task: Task,
    instance_id: str,
    result: dict[str, Any],
    created_at: datetime,
) -> EvaluationResult:
    """Build one evaluation attempt for a task."""
    return EvaluationResult(
        org_id=task.org_id, task=task.id, instance_id=instance_id, result=result, created_at=created_at
    )


def _error_result(task: Task, error_message: str, created_at: datetime) -> ErrorResult:
    """Build one error attempt for a task."""
    return ErrorResult(org_id=task.org_id, task=task.id, error_message=error_message, created_at=created_at)


def test_get_single_task_returns_current_result_or_error(client: TestClient, database_session: Session) -> None:
    """The single task endpoint should expose only the current task outcome.

    Test cases:
    - A finished task returns its latest evaluation result and no stale error message.
    - An errored task returns its latest error message and no stale evaluation result.
    """
    benchmark, finished_task = _persist_benchmark_with_task(database_session, task_id="finished")
    error_task = Task(org_id=benchmark.org_id, benchmark=benchmark.id, task_id="errored", status=TaskStatus.ERROR)
    database_session.add(error_task)
    database_session.flush()

    old_created_at = _created_at(12)
    new_created_at = _created_at(13)
    for result_row in (
        _error_result(finished_task, "old finished task error", old_created_at),
        _evaluation_result(finished_task, "finished-old", {"attempt": "old"}, old_created_at),
        _evaluation_result(finished_task, "finished-new", {"attempt": "new"}, new_created_at),
        _evaluation_result(error_task, "errored-old", {"attempt": "old success"}, old_created_at),
        _error_result(error_task, "old error", old_created_at),
        _error_result(error_task, "new error", new_created_at),
    ):
        database_session.add(result_row)
    database_session.commit()

    finished_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert finished_response.status_code == 200, finished_response.text
    finished_data: dict[str, Any] = finished_response.json()
    assert finished_data["task_id"] == finished_task.task_id
    assert finished_data["status"] == "FINISHED"
    assert finished_data["error_message"] is None
    assert finished_data["evaluation_result"] == {"attempt": "new"}

    error_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert error_response.status_code == 200, error_response.text
    error_data: dict[str, Any] = error_response.json()
    assert error_data["task_id"] == error_task.task_id
    assert error_data["status"] == "ERROR"
    assert error_data["error_message"] == "new error"
    assert error_data["evaluation_result"] is None


def test_get_single_task_404_unknown(client: TestClient, database_session: Session) -> None:
    """Unknown tasks must return not found inside an existing benchmark.

    Test cases:
    - A missing task ID receives 404 without altering the benchmark.
    """
    benchmark, _task = _persist_benchmark_with_task(database_session)

    response = client.get(
        f"/benchmarks/{benchmark.id}/tasks/nonexistent",
        headers={"Authorization": "Bearer fake"},
    )

    assert response.status_code == 404


def test_unauthenticated_returns_401(client: TestClient, database_session: Session) -> None:
    """Single-task detail must require authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    benchmark, task = _persist_benchmark_with_task(database_session)

    assert client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}").status_code == 401
