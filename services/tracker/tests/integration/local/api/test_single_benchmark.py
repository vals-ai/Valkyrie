"""Run with `uv run pytest tests/integration/local/api/test_single_benchmark.py`.

Exercise single-benchmark routes through the real app and local database.
"""

from uuid import uuid4

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ErrorResult,
    FinalEvaluation,
    Task,
    TaskStatus,
)


def _persist_benchmark(
    database_session: Session,
    name: str = "bench-1",
    status: BenchmarkStatus = BenchmarkStatus.FINISHED,
) -> Benchmark:
    """Create and persist a benchmark for one route scenario."""
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=status,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    database_session.add(benchmark)
    database_session.commit()

    return benchmark


def _make_task(benchmark: Benchmark, task_id: str, status: TaskStatus = TaskStatus.PENDING) -> Task:
    """Build a task with the scenario-defining status visible at the call site."""
    return Task(org_id=benchmark.org_id, benchmark=benchmark.id, task_id=task_id, status=status)


def test_get_single_benchmark_returns_payload(client: TestClient, database_session: Session) -> None:
    """Run detail must combine persisted benchmark metadata and task progress.

    Test cases:
    - An authenticated request returns the expected run payload and counts.
    """
    benchmark = _persist_benchmark(database_session)

    response = client.get(f"/benchmarks/{benchmark.id}", headers={"Authorization": "Bearer fake"})

    assert response.status_code == 200, response.text
    response_body = response.json()
    assert response_body["id"] == str(benchmark.id)
    assert response_body["name"] == "bench-1"
    assert response_body["status"] == "FINISHED"
    assert response_body["final_score"] is None
    assert response_body["total_tasks"] == 0


def test_get_single_benchmark_includes_final_score(client: TestClient, database_session: Session) -> None:
    """Completed run detail must include its persisted final score.

    Test cases:
    - A final-evaluation row appears in the API response.
    """
    benchmark = _persist_benchmark(database_session)
    database_session.add(FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=0.42))
    database_session.commit()

    response = client.get(f"/benchmarks/{benchmark.id}", headers={"Authorization": "Bearer fake"})

    assert response.json()["final_score"] == 0.42


def test_get_single_benchmark_unknown_returns_404(client: TestClient) -> None:
    """Unknown run IDs must return a stable not-found response.

    Test cases:
    - A missing benchmark UUID receives 404.
    """
    unknown_benchmark_id = uuid4()

    response = client.get(f"/benchmarks/{unknown_benchmark_id}", headers={"Authorization": "Bearer fake"})

    assert response.status_code == 404


def test_get_benchmark_tasks_paginates(client: TestClient, database_session: Session) -> None:
    """Task listing must honor limit and offset while reporting the full count.

    Test cases:
    - A page contains the requested rows and the unpaginated total.
    """
    benchmark = _persist_benchmark(database_session)
    for task_index in range(3):
        database_session.add(_make_task(benchmark, f"task-{task_index}"))
    database_session.commit()

    response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?limit=2&offset=0",
        headers={"Authorization": "Bearer fake"},
    )

    assert response.status_code == 200
    response_body = response.json()
    assert len(response_body["tasks"]) == 2
    assert response_body["total_count"] == 3


def test_get_benchmark_tasks_filters_by_status(client: TestClient, database_session: Session) -> None:
    """Task listing must apply comma-separated status filters.

    Test cases:
    - Only tasks in the requested statuses are returned and counted.
    """
    benchmark = _persist_benchmark(database_session)
    finished_task = _make_task(benchmark, "ok", TaskStatus.FINISHED)
    error_task = _make_task(benchmark, "err", TaskStatus.ERROR)
    database_session.add_all([finished_task, error_task])
    database_session.flush()
    database_session.add(ErrorResult(org_id=benchmark.org_id, task=finished_task.id, error_message="old boom"))
    database_session.add(ErrorResult(org_id=benchmark.org_id, task=error_task.id, error_message="boom"))
    database_session.commit()

    error_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?status=ERROR",
        headers={"Authorization": "Bearer fake"},
    )
    error_response_body = error_response.json()

    finished_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?status=FINISHED",
        headers={"Authorization": "Bearer fake"},
    )
    finished_response_body = finished_response.json()

    assert len(error_response_body["tasks"]) == 1
    assert error_response_body["tasks"][0]["task_id"] == "err"
    assert error_response_body["tasks"][0]["error_message"] == "boom"
    assert len(finished_response_body["tasks"]) == 1
    assert finished_response_body["tasks"][0]["task_id"] == "ok"
    assert finished_response_body["tasks"][0]["error_message"] is None


def test_get_benchmark_tasks_sort_by_status_surfaces_errors_first(
    client: TestClient,
    database_session: Session,
) -> None:
    """Attention sorting must place task errors ahead of less urgent states.

    Test cases:
    - Descending status order returns errors before terminal and active tasks.
    """
    benchmark = _persist_benchmark(database_session)

    # Seed tasks out of priority order to prove the route performs the sort.
    for task_id, status in [
        ("a-pending", TaskStatus.PENDING),
        ("b-finished", TaskStatus.FINISHED),
        ("c-error", TaskStatus.ERROR),
        ("d-in-progress", TaskStatus.IN_PROGRESS),
    ]:
        database_session.add(_make_task(benchmark, task_id, status))
    database_session.commit()

    response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?sort=status",
        headers={"Authorization": "Bearer fake"},
    )

    statuses = [task["status"] for task in response.json()["tasks"]]
    assert statuses == ["ERROR", "FINISHED", "IN_PROGRESS", "PENDING"]


_HARNESS_HEADERS: dict[str, str] = {
    "Authorization": "Bearer fake",
    "X-Harness-AWS-Access-Key-Id": "AKIA",
    "X-Harness-AWS-Secret-Access-Key": "secret",
    "X-Harness-AWS-Default-Region": "us-east-1",
    "X-Harness-S3-Bucket": "agentic-harness",
    "X-Harness-Log-Group": "benchmarks",
    "X-Harness-Log-Retention-Policy": "30",
}


def test_get_single_benchmark_builds_run_console_urls_from_harness_headers(
    client: TestClient,
    database_session: Session,
) -> None:
    """Run detail must build CloudWatch and S3 links from valid harness headers.

    Test cases:
    - A complete header set returns both console URLs for the benchmark.
    """
    benchmark = _persist_benchmark(database_session)

    response_body = client.get(f"/benchmarks/{benchmark.id}", headers=_HARNESS_HEADERS).json()

    assert "cloudwatch/home" in response_body["cloudwatch_url"]
    assert str(benchmark.id) in response_body["cloudwatch_url"]
    assert "s3/buckets/agentic-harness" in response_body["s3_bucket_url"]
    assert str(benchmark.id) in response_body["s3_bucket_url"]


def test_get_single_benchmark_omits_console_urls_without_harness_headers(
    client: TestClient,
    database_session: Session,
) -> None:
    """Run detail must remain usable when optional harness headers are absent.

    Test cases:
    - Missing headers return null CloudWatch and S3 links instead of an error.
    """
    benchmark = _persist_benchmark(database_session)

    response_body = client.get(
        f"/benchmarks/{benchmark.id}",
        headers={"Authorization": "Bearer fake"},
    ).json()

    assert response_body["cloudwatch_url"] is None
    assert response_body["s3_bucket_url"] is None


def test_get_single_benchmark_s3_url_without_log_group_skips_cloudwatch(
    client: TestClient,
    database_session: Session,
) -> None:
    """S3 navigation must not depend on an optional CloudWatch log group.

    Test cases:
    - Complete AWS headers without a log group return only the S3 link.
    """
    benchmark = _persist_benchmark(database_session)

    headers = {header: value for header, value in _HARNESS_HEADERS.items() if header != "X-Harness-Log-Group"}
    response_body = client.get(f"/benchmarks/{benchmark.id}", headers=headers).json()

    assert response_body["cloudwatch_url"] is None
    assert "s3/buckets/agentic-harness" in response_body["s3_bucket_url"]


def test_get_benchmark_tasks_sort_by_task_id_ascending(client: TestClient, database_session: Session) -> None:
    """Task listing must support stable ascending task-ID order.

    Test cases:
    - Ascending task sort returns IDs in lexical order.
    """
    benchmark = _persist_benchmark(database_session)
    for task_id in ["t-c", "t-a", "t-b"]:
        database_session.add(_make_task(benchmark, task_id))
    database_session.commit()

    response_body = client.get(
        f"/benchmarks/{benchmark.id}/tasks?sort=task_id&sort_dir=asc",
        headers={"Authorization": "Bearer fake"},
    ).json()

    assert [task["task_id"] for task in response_body["tasks"]] == ["t-a", "t-b", "t-c"]


def test_unauth_returns_401(client: TestClient) -> None:
    """Single-run routes must require authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    unknown_benchmark_id = uuid4()

    assert client.get(f"/benchmarks/{unknown_benchmark_id}").status_code == 401
    assert client.get(f"/benchmarks/{unknown_benchmark_id}/tasks").status_code == 401


def test_get_benchmark_tasks_filters_by_task_id_search(client: TestClient, database_session: Session) -> None:
    """Task-ID search must narrow a run without changing the original rows.

    Test cases:
    - A substring query returns only matching task IDs.
    """
    benchmark = _persist_benchmark(database_session)
    database_session.add_all(
        [
            _make_task(benchmark, "astropy__astropy-12907", TaskStatus.FINISHED),
            _make_task(benchmark, "django__django-11400", TaskStatus.FINISHED),
            _make_task(benchmark, "astropy__astropy-13033", TaskStatus.ERROR),
        ]
    )
    database_session.commit()

    response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?task_id_search=astropy",
        headers={"Authorization": "Bearer fake"},
    )

    assert response.status_code == 200, response.text
    response_body = response.json()
    assert response_body["total_count"] == 2
    task_ids = sorted(task["task_id"] for task in response_body["tasks"])
    assert task_ids == ["astropy__astropy-12907", "astropy__astropy-13033"]


def test_get_benchmark_tasks_search_treats_like_wildcards_literally(
    client: TestClient,
    database_session: Session,
) -> None:
    """Task-ID search must treat SQL wildcard characters as user text.

    Test cases:
    - Percent and underscore characters match only literal task IDs.
    """
    benchmark = _persist_benchmark(database_session)
    database_session.add_all(
        [
            _make_task(benchmark, "task_1"),
            _make_task(benchmark, "taskA1"),
            _make_task(benchmark, "task%1"),
        ]
    )
    database_session.commit()

    underscore_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?task_id_search=task_1",
        headers={"Authorization": "Bearer fake"},
    )

    percent_response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?task_id_search=task%251",
        headers={"Authorization": "Bearer fake"},
    )

    assert underscore_response.status_code == 200, underscore_response.text
    assert underscore_response.json()["total_count"] == 1
    assert underscore_response.json()["tasks"][0]["task_id"] == "task_1"
    assert percent_response.status_code == 200, percent_response.text
    assert percent_response.json()["total_count"] == 1
    assert percent_response.json()["tasks"][0]["task_id"] == "task%1"


def test_get_benchmark_tasks_search_case_insensitive(client: TestClient, database_session: Session) -> None:
    """Task-ID search must be case-insensitive for CLI and UI parity.

    Test cases:
    - A differently cased query still finds the persisted task ID.
    """
    benchmark = _persist_benchmark(database_session)
    database_session.add_all(
        [
            _make_task(benchmark, "DJANGO__django-1"),
            _make_task(benchmark, "astropy__1"),
        ]
    )
    database_session.commit()

    response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?task_id_search=django",
        headers={"Authorization": "Bearer fake"},
    )

    response_body = response.json()
    assert response_body["total_count"] == 1
    assert response_body["tasks"][0]["task_id"] == "DJANGO__django-1"


def test_get_benchmark_tasks_search_combines_with_status(client: TestClient, database_session: Session) -> None:
    """Task search and status filters must compose as an intersection.

    Test cases:
    - Results satisfy both the task-ID substring and requested status.
    """
    benchmark = _persist_benchmark(database_session)
    database_session.add_all(
        [
            _make_task(benchmark, "astropy__12907", TaskStatus.FINISHED),
            _make_task(benchmark, "astropy__13033", TaskStatus.ERROR),
            _make_task(benchmark, "django__11400", TaskStatus.ERROR),
        ]
    )
    database_session.commit()

    response = client.get(
        f"/benchmarks/{benchmark.id}/tasks?task_id_search=astropy&status=ERROR",
        headers={"Authorization": "Bearer fake"},
    )

    response_body = response.json()
    assert response_body["total_count"] == 1
    assert response_body["tasks"][0]["task_id"] == "astropy__13033"
