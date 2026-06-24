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


def test_get_single_task_returns_payload(client: TestClient, database_session: Session) -> None:
    b, t = _make_bench_with_task(database_session)
    database_session.add(ErrorResult(org_id=t.org_id, task=t.id, error_message="old boom"))
    database_session.add(
        EvaluationResult(
            org_id=t.org_id,
            task=t.id,
            instance_id=f"{b.id}/{t.task_id}",
            result={"resolved": True, "f2p_score": 0.9},
        )
    )
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data: dict[str, Any] = resp.json()
    assert data["task_id"] == t.task_id
    assert data["status"] == "FINISHED"
    assert data["error_message"] is None
    assert data["evaluation_result"]["resolved"] is True


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

    old_created_at = datetime(2026, 6, 24, 12, tzinfo=ZoneInfo("UTC"))
    new_created_at = datetime(2026, 6, 24, 13, tzinfo=ZoneInfo("UTC"))
    database_session.add_all(
        [
            ErrorResult(
                org_id=finished_task.org_id,
                task=finished_task.id,
                error_message="old finished task error",
                created_at=old_created_at,
            ),
            EvaluationResult(
                org_id=finished_task.org_id,
                task=finished_task.id,
                instance_id="finished-old",
                result={"attempt": "old"},
                created_at=old_created_at,
            ),
            EvaluationResult(
                org_id=finished_task.org_id,
                task=finished_task.id,
                instance_id="finished-new",
                result={"attempt": "new"},
                created_at=new_created_at,
            ),
            EvaluationResult(
                org_id=error_task.org_id,
                task=error_task.id,
                instance_id="errored-old",
                result={"attempt": "old success"},
                created_at=old_created_at,
            ),
            ErrorResult(
                org_id=error_task.org_id,
                task=error_task.id,
                error_message="old error",
                created_at=old_created_at,
            ),
            ErrorResult(
                org_id=error_task.org_id,
                task=error_task.id,
                error_message="new error",
                created_at=new_created_at,
            ),
        ]
    )
    database_session.commit()

    finished_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert finished_response.status_code == 200, finished_response.text
    finished_data: dict[str, Any] = finished_response.json()
    assert finished_data["error_message"] is None
    assert finished_data["evaluation_result"] == {"attempt": "new"}

    error_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert error_response.status_code == 200, error_response.text
    error_data: dict[str, Any] = error_response.json()
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
