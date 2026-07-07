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

UTC = ZoneInfo("UTC")


def _make_bench_with_task(session: Session, task_id: str = "astropy__12907") -> tuple[Benchmark, Task]:
    b = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(b)
    session.commit()
    t = Task(org_id=b.org_id, benchmark=b.id, task_id=task_id, status=TaskStatus.FINISHED)
    session.add(t)
    session.commit()
    return b, t


def _created_at(hour: int) -> datetime:
    return datetime(2026, 6, 24, hour, tzinfo=UTC)


def _evaluation_result(
    task: Task,
    instance_id: str,
    result: dict[str, Any],
    created_at: datetime,
) -> EvaluationResult:
    return EvaluationResult(
        org_id=task.org_id, task=task.id, instance_id=instance_id, result=result, created_at=created_at
    )


def _error_result(task: Task, error_message: str, created_at: datetime) -> ErrorResult:
    return ErrorResult(org_id=task.org_id, task=task.id, error_message=error_message, created_at=created_at)


def test_get_single_task_returns_current_result_or_error(client: TestClient, database_session: Session) -> None:
    """The single task endpoint should expose only the current task outcome.

    Test cases:
    - A finished task returns its latest evaluation result and no stale error message.
    - An errored task returns its latest error message and no stale evaluation result.
    """
    benchmark, finished_task = _make_bench_with_task(database_session, task_id="finished")
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
    b, _ = _make_bench_with_task(database_session)
    resp = client.get(
        f"/benchmarks/{b.id}/tasks/nonexistent",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 404


def test_unauthenticated_returns_401(client: TestClient, database_session: Session) -> None:
    b, t = _make_bench_with_task(database_session)
    assert client.get(f"/benchmarks/{b.id}/tasks/{t.task_id}").status_code == 401
