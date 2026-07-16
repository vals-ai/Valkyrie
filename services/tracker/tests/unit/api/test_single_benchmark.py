"""Behavior tests for the single-benchmark API."""

from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)

client = TestClient(app)


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
            Task(
                org_id=TEST_ORG_ID,
                benchmark=benchmark.id,
                task_id="finished",
                status=TaskStatus.FINISHED,
                finished_at=benchmark.started_at,
            ),
            Task(
                org_id=TEST_ORG_ID,
                benchmark=benchmark.id,
                task_id="error",
                status=TaskStatus.ERROR,
                finished_at=benchmark.started_at,
            ),
            Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="stopped", status=TaskStatus.STOPPED),
            Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="pending"),
            FinalEvaluation(org_id=TEST_ORG_ID, benchmark=benchmark.id, final_score=0.75),
        ]
    )
    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(org_id=other_org.id, name=benchmark.name, arguments=benchmark.arguments)
    database_session.add_all([other_org, other_benchmark])
    database_session.commit()

    response = client.get(
        f"/benchmarks/{benchmark.id}",
        headers={
            "x-harness-aws-access-key-id": "test-key",
            "x-harness-aws-secret-access-key": "test-secret",
            "x-harness-aws-default-region": "us-east-1",
            "x-harness-s3-bucket": "test-bucket",
            "x-harness-log-group": "test-log-group",
        },
    )
    other_org_response = client.get(f"/benchmarks/{other_benchmark.id}")

    body = response.json()
    assert response.status_code == 200
    assert body["total_tasks"] == 4
    assert body["finished_tasks"] == 3
    assert body["task_state_counts"] == {
        "ERROR": 1,
        "FINISHED": 1,
        "PENDING": 1,
        "STOPPED": 1,
    }
    assert body["final_score"] == 0.75
    assert str(benchmark.id) in body["cloudwatch_url"]
    assert str(benchmark.id) in body["s3_bucket_url"]
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
    literal_task = Task(
        org_id=TEST_ORG_ID,
        benchmark=benchmark.id,
        task_id="literal_%_match",
        status=TaskStatus.ERROR,
        started_at=now,
        finished_at=now,
    )
    other_error = Task(
        org_id=TEST_ORG_ID,
        benchmark=benchmark.id,
        task_id="ordinary-error",
        status=TaskStatus.ERROR,
        started_at=now - timedelta(minutes=1),
        finished_at=now,
    )
    finished = Task(
        org_id=TEST_ORG_ID,
        benchmark=benchmark.id,
        task_id="finished",
        status=TaskStatus.FINISHED,
        started_at=now,
        finished_at=now,
    )
    database_session.add_all([literal_task, other_error, finished])
    database_session.flush()
    database_session.add_all(
        [
            ErrorResult(
                org_id=TEST_ORG_ID,
                task=literal_task.id,
                error_message="old failure",
                created_at=now - timedelta(minutes=1),
            ),
            ErrorResult(
                org_id=TEST_ORG_ID,
                task=literal_task.id,
                error_message="latest failure",
                created_at=now,
            ),
            ErrorResult(org_id=TEST_ORG_ID, task=other_error.id, error_message="other failure", created_at=now),
        ]
    )
    database_session.commit()

    sorted_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks",
        params={"status": "ERROR,FINISHED", "sort": "status", "sort_dir": "desc"},
    )
    literal_search = client.get(
        f"/benchmarks/{benchmark.id}/tasks",
        params={"task_id_search": "_%"},
    )

    sorted_body = sorted_response.json()
    assert sorted_response.status_code == 200
    assert sorted_body["total_count"] == 3
    assert [task["status"] for task in sorted_body["tasks"]] == ["ERROR", "ERROR", "FINISHED"]
    literal_row = next(task for task in sorted_body["tasks"] if task["task_id"] == literal_task.task_id)
    assert literal_row["error_message"] == "latest failure"
    assert literal_search.status_code == 200
    assert literal_search.json()["total_count"] == 1
    assert literal_search.json()["tasks"][0]["task_id"] == "literal_%_match"
