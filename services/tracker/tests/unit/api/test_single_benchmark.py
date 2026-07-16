"""Run with `uv run pytest tests/unit/api/test_single_benchmark.py`.

Cover single-benchmark details and task listing behavior.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.factories import make_error_result, make_task
from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    Benchmark,
    FinalEvaluation,
    Org,
    TaskStatus,
)

_client = TestClient(app)


def test_single_benchmark_reports_terminal_progress_and_enforces_org_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Run detail must count terminal outcomes and hide another organization's run.

    Test cases:
    - Finished, error, and stopped tasks all count toward completed progress.
    - Final score and run links are returned with the persisted metadata.
    - A benchmark from another organization returns 404.
    """
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    database_session.add_all(
        [
            make_task(
                benchmark,
                "finished",
                status=TaskStatus.FINISHED,
                finished_at=benchmark.started_at,
            ),
            make_task(
                benchmark,
                "error",
                status=TaskStatus.ERROR,
                finished_at=benchmark.started_at,
            ),
            make_task(benchmark, "stopped", status=TaskStatus.STOPPED),
            make_task(benchmark, "pending"),
            FinalEvaluation(org_id=TEST_ORG_ID, benchmark=benchmark.id, final_score=0.75),
        ]
    )
    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(org_id=other_org.id, name=benchmark.name, arguments=benchmark.arguments)
    database_session.add_all([other_org, other_benchmark])
    database_session.commit()

    response = _client.get(
        f"/benchmarks/{benchmark.id}",
        headers={
            "x-harness-aws-access-key-id": "test-key",
            "x-harness-aws-secret-access-key": "test-secret",
            "x-harness-aws-default-region": "us-east-1",
            "x-harness-s3-bucket": "test-bucket",
            "x-harness-log-group": "test-log-group",
        },
    )
    other_org_response = _client.get(f"/benchmarks/{other_benchmark.id}")

    response_body = response.json()
    assert response.status_code == 200
    assert response_body["total_tasks"] == 4
    assert response_body["finished_tasks"] == 3
    assert response_body["task_state_counts"] == {
        "ERROR": 1,
        "FINISHED": 1,
        "PENDING": 1,
        "STOPPED": 1,
    }
    assert response_body["final_score"] == 0.75
    assert str(benchmark.id) in response_body["cloudwatch_url"]
    assert str(benchmark.id) in response_body["s3_bucket_url"]
    assert other_org_response.status_code == 404


def test_benchmark_tasks_filter_literal_search_and_latest_error(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Task listing must preserve attention order, literal search, and retry history.

    Test cases:
    - Status sorting places errors before finished tasks.
    - Percent and underscore search characters are treated literally.
    - The newest error from retry history is returned.
    """
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    literal_task = make_task(
        benchmark,
        "literal_%_match",
        status=TaskStatus.ERROR,
        started_at=now,
        finished_at=now,
    )
    other_error = make_task(
        benchmark,
        "ordinary-error",
        status=TaskStatus.ERROR,
        started_at=now - timedelta(minutes=1),
        finished_at=now,
    )
    finished_task = make_task(
        benchmark,
        "finished",
        status=TaskStatus.FINISHED,
        started_at=now,
        finished_at=now,
    )
    database_session.add_all([literal_task, other_error, finished_task])
    database_session.flush()
    database_session.add_all(
        [
            make_error_result(literal_task, "old failure", now - timedelta(minutes=1)),
            make_error_result(literal_task, "latest failure", now),
            make_error_result(other_error, "other failure", now),
        ]
    )
    database_session.commit()

    sorted_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks",
        params={"status": "ERROR,FINISHED", "sort": "status", "sort_dir": "desc"},
    )
    literal_search_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks",
        params={"task_id_search": "_%"},
    )

    sorted_body = sorted_response.json()
    assert sorted_response.status_code == 200
    assert sorted_body["total_count"] == 3
    assert [task["status"] for task in sorted_body["tasks"]] == ["ERROR", "ERROR", "FINISHED"]
    literal_row = next(task for task in sorted_body["tasks"] if task["task_id"] == literal_task.task_id)
    assert literal_row["error_message"] == "latest failure"
    assert literal_search_response.status_code == 200
    assert literal_search_response.json()["total_count"] == 1
    assert literal_search_response.json()["tasks"][0]["task_id"] == "literal_%_match"
