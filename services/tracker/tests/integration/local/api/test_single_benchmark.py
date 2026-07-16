"""Local integration tests for single-benchmark routes."""

from uuid import uuid4

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


def _make_bench(name="bench-1", status=BenchmarkStatus.FINISHED) -> Benchmark:
    return Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=status,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )


def test_get_single_benchmark_returns_payload(client, database_session):
    """Run detail must combine persisted benchmark metadata and task progress.

    Test cases:
    - An authenticated request returns the expected run payload and counts.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()

    resp = client.get(f"/benchmarks/{b.id}", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"] == str(b.id)
    assert data["name"] == "bench-1"
    assert data["status"] == "FINISHED"
    assert data["final_score"] is None
    assert data["total_tasks"] == 0


def test_get_single_benchmark_includes_final_score(client, database_session):
    """Completed run detail must include its persisted final score.

    Test cases:
    - A final-evaluation row appears in the API response.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(FinalEvaluation(org_id=b.org_id, benchmark=b.id, final_score=0.42))
    database_session.commit()

    resp = client.get(f"/benchmarks/{b.id}", headers={"Authorization": "Bearer fake"})
    assert resp.json()["final_score"] == 0.42


def test_get_single_benchmark_unknown_returns_404(client):
    """Unknown run IDs must return a stable not-found response.

    Test cases:
    - A missing benchmark UUID receives 404.
    """
    bogus = uuid4()
    resp = client.get(f"/benchmarks/{bogus}", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 404


def test_get_benchmark_tasks_paginates(client, database_session):
    """Task listing must honor limit and offset while reporting the full count.

    Test cases:
    - A page contains the requested rows and the unpaginated total.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    for i in range(3):
        database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id=f"t{i}", status=TaskStatus.PENDING))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?limit=2&offset=0",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["tasks"]) == 2
    assert data["total_count"] == 3


def test_get_benchmark_tasks_filters_by_status(client, database_session):
    """Task listing must apply comma-separated status filters.

    Test cases:
    - Only tasks in the requested statuses are returned and counted.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    finished_task = Task(org_id=b.org_id, benchmark=b.id, task_id="ok", status=TaskStatus.FINISHED)
    error_task = Task(org_id=b.org_id, benchmark=b.id, task_id="err", status=TaskStatus.ERROR)
    database_session.add_all([finished_task, error_task])
    database_session.flush()
    database_session.add(ErrorResult(org_id=b.org_id, task=finished_task.id, error_message="old boom"))
    database_session.add(ErrorResult(org_id=b.org_id, task=error_task.id, error_message="boom"))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?status=ERROR",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert len(data["tasks"]) == 1
    assert data["tasks"][0]["task_id"] == "err"
    assert data["tasks"][0]["error_message"] == "boom"

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?status=FINISHED",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert len(data["tasks"]) == 1
    assert data["tasks"][0]["task_id"] == "ok"
    assert data["tasks"][0]["error_message"] is None


def test_get_benchmark_tasks_sort_by_status_surfaces_errors_first(client, database_session):
    """Attention sorting must place task errors ahead of less urgent states.

    Test cases:
    - Descending status order returns errors before terminal and active tasks.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    # Insert out of priority order.
    for task_id, status in [
        ("a-pending", TaskStatus.PENDING),
        ("b-finished", TaskStatus.FINISHED),
        ("c-error", TaskStatus.ERROR),
        ("d-in-progress", TaskStatus.IN_PROGRESS),
    ]:
        database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id=task_id, status=status))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?sort=status",
        headers={"Authorization": "Bearer fake"},
    )
    statuses = [t["status"] for t in resp.json()["tasks"]]
    assert statuses == ["ERROR", "FINISHED", "IN_PROGRESS", "PENDING"]


_HARNESS_HEADERS = {
    "Authorization": "Bearer fake",
    "X-Harness-AWS-Access-Key-Id": "AKIA",
    "X-Harness-AWS-Secret-Access-Key": "secret",
    "X-Harness-AWS-Default-Region": "us-east-1",
    "X-Harness-S3-Bucket": "agentic-harness",
    "X-Harness-Log-Group": "benchmarks",
    "X-Harness-Log-Retention-Policy": "30",
}


def test_get_single_benchmark_builds_run_console_urls_from_harness_headers(client, database_session):
    """Run detail must build CloudWatch and S3 links from valid harness headers.

    Test cases:
    - A complete header set returns both console URLs for the benchmark.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()

    data = client.get(f"/benchmarks/{b.id}", headers=_HARNESS_HEADERS).json()
    assert "cloudwatch/home" in data["cloudwatch_url"]
    assert str(b.id) in data["cloudwatch_url"]
    assert "s3/buckets/agentic-harness" in data["s3_bucket_url"]
    assert str(b.id) in data["s3_bucket_url"]


def test_get_single_benchmark_omits_console_urls_without_harness_headers(client, database_session):
    """Run detail must remain usable when optional harness headers are absent.

    Test cases:
    - Missing headers return null CloudWatch and S3 links instead of an error.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()

    data = client.get(f"/benchmarks/{b.id}", headers={"Authorization": "Bearer fake"}).json()
    assert data["cloudwatch_url"] is None
    assert data["s3_bucket_url"] is None


def test_get_single_benchmark_s3_url_without_log_group_skips_cloudwatch(client, database_session):
    """S3 navigation must not depend on an optional CloudWatch log group.

    Test cases:
    - Complete AWS headers without a log group return only the S3 link.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()

    headers = {k: v for k, v in _HARNESS_HEADERS.items() if k != "X-Harness-Log-Group"}
    data = client.get(f"/benchmarks/{b.id}", headers=headers).json()
    assert data["cloudwatch_url"] is None
    assert "s3/buckets/agentic-harness" in data["s3_bucket_url"]


def test_get_benchmark_tasks_sort_by_task_id_ascending(client, database_session):
    """Task listing must support stable ascending task-ID order.

    Test cases:
    - Ascending task sort returns IDs in lexical order.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    for task_id in ["t-c", "t-a", "t-b"]:
        database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id=task_id, status=TaskStatus.PENDING))
    database_session.commit()

    data = client.get(
        f"/benchmarks/{b.id}/tasks?sort=task_id&sort_dir=asc",
        headers={"Authorization": "Bearer fake"},
    ).json()
    assert [t["task_id"] for t in data["tasks"]] == ["t-a", "t-b", "t-c"]


def test_unauth_returns_401(client):
    """Single-run routes must require authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    bogus = uuid4()
    assert client.get(f"/benchmarks/{bogus}").status_code == 401
    assert client.get(f"/benchmarks/{bogus}/tasks").status_code == 401


def test_get_benchmark_tasks_filters_by_task_id_search(client, database_session):
    """Task-ID search must narrow a run without changing the original rows.

    Test cases:
    - A substring query returns only matching task IDs.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__astropy-12907", status=TaskStatus.FINISHED)
    )
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="django__django-11400", status=TaskStatus.FINISHED)
    )
    database_session.add(
        Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__astropy-13033", status=TaskStatus.ERROR)
    )
    database_session.commit()

    # substring match (case-insensitive)
    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=astropy",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 2
    ids = sorted(t["task_id"] for t in data["tasks"])
    assert ids == ["astropy__astropy-12907", "astropy__astropy-13033"]


def test_get_benchmark_tasks_search_treats_like_wildcards_literally(client, database_session):
    """Task-ID search must treat SQL wildcard characters as user text.

    Test cases:
    - Percent and underscore characters match only literal task IDs.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="task_1", status=TaskStatus.PENDING))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="taskA1", status=TaskStatus.PENDING))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="task%1", status=TaskStatus.PENDING))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=task_1",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "task_1"

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=task%251",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "task%1"


def test_get_benchmark_tasks_search_case_insensitive(client, database_session):
    """Task-ID search must be case-insensitive for CLI and UI parity.

    Test cases:
    - A differently cased query still finds the persisted task ID.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="DJANGO__django-1", status=TaskStatus.PENDING))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__1", status=TaskStatus.PENDING))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=django",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "DJANGO__django-1"


def test_get_benchmark_tasks_search_combines_with_status(client, database_session):
    """Task search and status filters must compose as an intersection.

    Test cases:
    - Results satisfy both the task-ID substring and requested status.
    """
    b = _make_bench()
    database_session.add(b)
    database_session.commit()
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__12907", status=TaskStatus.FINISHED))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="astropy__13033", status=TaskStatus.ERROR))
    database_session.add(Task(org_id=b.org_id, benchmark=b.id, task_id="django__11400", status=TaskStatus.ERROR))
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks?task_id_search=astropy&status=ERROR",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert data["total_count"] == 1
    assert data["tasks"][0]["task_id"] == "astropy__13033"
