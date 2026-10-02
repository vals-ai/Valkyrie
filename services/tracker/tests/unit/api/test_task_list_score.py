"""Run: uv run pytest tests/unit/api/test_task_list_score.py -q.

Task list scores come from stored evaluation results in one list query.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel
from sqlalchemy import event
from sqlalchemy.dialects import postgresql
from sqlmodel import Session

from main import app
from tests.factories import make_evaluation_result, make_task
from tracker.database.models import Benchmark, EvaluationResult, Org, TaskStatus


class OldTaskClient(BaseModel):
    task_id: str
    status: TaskStatus


def test_task_list_returns_latest_score_and_null_for_missing_scores(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    tasks = [
        make_task(benchmark, "scored", status=TaskStatus.FINISHED),
        make_task(benchmark, "zero", status=TaskStatus.FINISHED),
        make_task(benchmark, "missing-score", status=TaskStatus.FINISHED),
        make_task(benchmark, "missing-result", status=TaskStatus.FINISHED),
        make_task(benchmark, "pending"),
        make_task(benchmark, "error", status=TaskStatus.ERROR),
        make_task(benchmark, "null-score", status=TaskStatus.FINISHED),
        make_task(benchmark, "string-score", status=TaskStatus.FINISHED),
        make_task(benchmark, "bool-score", status=TaskStatus.FINISHED),
        make_task(benchmark, "object-score", status=TaskStatus.FINISHED),
    ]
    database_session.add_all(tasks)
    database_session.flush()
    now = benchmark.started_at
    other_org = Org(id=uuid4(), name="other-org")
    database_session.add(other_org)
    database_session.flush()
    old_result = make_evaluation_result(tasks[0], "old", {"score": 0.25}, now)
    old_result.id = UUID(int=1)
    latest_result = make_evaluation_result(tasks[0], "latest", {"score": 0.75}, now)
    latest_result.id = UUID(int=2)
    database_session.add_all(
        [
            old_result,
            latest_result,
            make_evaluation_result(tasks[1], "zero", {"score": 0.0}, now),
            make_evaluation_result(tasks[2], "no-score", {"correct": True}, now),
            make_evaluation_result(tasks[4], "pending", {"score": 1.0}, now),
            make_evaluation_result(tasks[5], "error", {"score": 1.0}, now),
            make_evaluation_result(tasks[6], "null-score", {"score": None}, now),
            make_evaluation_result(tasks[7], "string-score", {"score": "passed"}, now),
            make_evaluation_result(tasks[8], "bool-score", {"score": True}, now),
            make_evaluation_result(tasks[9], "object-score", {"score": {"value": 0.5}}, now),
            EvaluationResult(
                org_id=other_org.id,
                task=tasks[0].id,
                result={"score": 1.0},
                created_at=now + timedelta(minutes=1),
            ),
        ]
    )
    database_session.commit()

    response = TestClient(app).get(f"/benchmarks/{benchmark.id}/tasks")

    assert response.status_code == 200, response.text

    rows = response.json()["tasks"]
    assert {row["task_id"]: row["score"] for row in rows} == {
        "scored": 0.75,
        "zero": 0.0,
        "missing-score": None,
        "missing-result": None,
        "pending": None,
        "error": None,
        "null-score": None,
        "string-score": None,
        "bool-score": None,
        "object-score": None,
    }

    assert [OldTaskClient.model_validate(row).task_id for row in rows] == [row["task_id"] for row in rows]


@pytest.mark.parametrize("task_count", [1, 25])
def test_task_list_score_does_not_add_queries(
    database_session: Session,
    example_benchmark_object: Benchmark,
    task_count: int,
) -> None:
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    tasks = [make_task(benchmark, f"task-{index}", status=TaskStatus.FINISHED) for index in range(task_count)]
    database_session.add_all(tasks)
    database_session.flush()
    database_session.add_all(
        make_evaluation_result(task, task.task_id, {"score": 0.5}, benchmark.started_at) for task in tasks
    )
    database_session.commit()
    statements: list[str] = []
    postgres_statements: list[str] = []

    def record_query(_conn: Any, _cursor: Any, statement: str, _params: Any, context: Any, _many: bool) -> None:
        statements.append(statement)
        assert context.compiled is not None
        postgres_statements.append(str(context.compiled.statement.compile(dialect=postgresql.dialect())))

    engine = database_session.get_bind()
    event.listen(engine, "before_cursor_execute", record_query)
    try:
        response = TestClient(app).get(f"/benchmarks/{benchmark.id}/tasks")
    finally:
        event.remove(engine, "before_cursor_execute", record_query)

    assert response.status_code == 200, response.text
    assert len(response.json()["tasks"]) == task_count
    assert all(task["score"] == 0.5 for task in response.json()["tasks"])
    assert len(statements) == 3
    assert sum("evaluationresult" in statement for statement in statements) == 1
    evaluation_query = next(statement for statement in postgres_statements if "evaluationresult" in statement)
    assert "SELECT evaluationresult.result ->" in evaluation_query
