"""Stored categories follow the same result and task selection as error messages."""

from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.database.models import Benchmark, ErrorResult, EvaluationResult, FailureCategory, Task, TaskStatus


@pytest.mark.usefixtures("process_benchmark_env")
def test_results_keep_categories_with_selected_errors_and_history(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
) -> None:
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    tasks = [
        Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id=name, status=TaskStatus.ERROR)
        for name in ("legacy", "unknown", "infrastructure", "recovered")
    ]
    database_session.add_all(tasks)
    database_session.flush()
    categories = [None, FailureCategory.UNKNOWN, FailureCategory.INFRASTRUCTURE, FailureCategory.AGENT]
    recorded_at = datetime(2026, 9, 21, 12)
    for task, category in zip(tasks, categories, strict=True):
        database_session.add(
            ErrorResult(
                task=task.id,
                org_id=TEST_ORG_ID,
                error_message="same message",
                category=category,
                created_at=recorded_at,
            )
        )
    # A later scheduled retry error must not replace the terminal message/category.
    database_session.add(
        ErrorResult(
            task=tasks[2].id,
            org_id=TEST_ORG_ID,
            error_message="retry only",
            category=FailureCategory.AGENT,
            retry_scheduled=True,
            created_at=recorded_at + timedelta(seconds=1),
        )
    )
    tasks[3].status = TaskStatus.FINISHED
    database_session.add(
        EvaluationResult(
            task=tasks[3].id,
            org_id=TEST_ORG_ID,
            instance_id="test",
            result={"score": 1.0},
            created_at=recorded_at + timedelta(seconds=2),
        )
    )
    database_session.commit()
    client = TestClient(app)
    response = client.get("/retrieve-results", params={"benchmark_id": str(benchmark.id)}, headers=harness_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["task_errors"] == {name: "same message" for name in ("legacy", "unknown", "infrastructure")}
    assert body["task_failure_categories"] == {"unknown": "unknown", "infrastructure": "infrastructure"}
    assert body["evaluation_results"]["recovered"]["history"][0]["failure_category"] == "agent"
    assert body["evaluation_results"]["recovered"]["history"][0]["error_message"] == "same message"
    for task_id, expected in [("unknown", {"unknown": "unknown"}), ("legacy", None)]:
        selected = client.get(
            "/retrieve-results",
            params={"benchmark_id": str(benchmark.id), "task_ids": task_id},
            headers=harness_headers,
        )
        assert selected.status_code == 200
        assert selected.json()["task_errors"] == {task_id: "same message"}
        assert selected.json()["task_failure_categories"] == expected
