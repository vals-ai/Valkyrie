"""Run with `uv run pytest tests/integration/local/api/test_single_benchmark.py`.

Exercise single-benchmark routes through the real app and local database.
"""

import json
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    BenchmarkStatus,
    ErrorResult,
    FinalEvaluation,
    TaskStatus,
)


class TestSingleBenchmark:
    """Single benchmark responses and missing runs."""

    def test_get_single_benchmark_returns_payload(self, client: TestClient, database_session: Session) -> None:
        """Run detail must combine persisted benchmark metadata and task progress.

        Test cases:
        - An authenticated request returns the expected run payload and counts.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)

        response = client.get(f"/benchmarks/{benchmark.id}", headers={"Authorization": "Bearer fake"})

        assert response.status_code == 200, response.text
        response_body = response.json()
        assert response_body["id"] == str(benchmark.id)
        assert response_body["name"] == "bench-1"
        assert response_body["status"] == "FINISHED"
        assert response_body["final_score"] is None
        assert response_body["total_tasks"] == 0

    def test_get_single_benchmark_includes_final_score(self, client: TestClient, database_session: Session) -> None:
        """Completed run detail must include its persisted final score.

        Test cases:
        - A final-evaluation row appears in the API response.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        database_session.add(FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=0.42))
        database_session.commit()

        response = client.get(f"/benchmarks/{benchmark.id}", headers={"Authorization": "Bearer fake"})

        assert response.json()["final_score"] == 0.42

    def test_get_single_benchmark_unknown_returns_404(self, client: TestClient) -> None:
        """Unknown run IDs must return a stable not-found response.

        Test cases:
        - A missing benchmark UUID receives 404.
        """
        unknown_benchmark_id = uuid4()

        response = client.get(f"/benchmarks/{unknown_benchmark_id}", headers={"Authorization": "Bearer fake"})

        assert response.status_code == 404


class TestBenchmarkStatusStream:
    """Single-benchmark status streaming."""

    def test_terminal_benchmark_streams_status_and_completes(
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """Terminal run streams must return persisted state and close without polling.

        Test cases:
        - A finished benchmark emits its current payload as a data event.
        - The stream emits a completion event with SSE response headers.
        """
        benchmark = make_benchmark(name="streamed-benchmark", status=BenchmarkStatus.FINISHED, session=database_session)
        database_session.add_all(
            [
                make_task(benchmark, "completed-task", status=TaskStatus.FINISHED),
                FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=0.75),
            ]
        )
        database_session.commit()

        with client.stream(
            "GET",
            "/fetch-benchmark",
            params={"benchmark_id": str(benchmark.id), "connect": "true"},
            headers={"Authorization": "Bearer fake"},
        ) as response:
            event_lines = [line for line in response.iter_lines() if line]

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.headers["cache-control"] == "no-cache"
        assert event_lines[-1] == "event: complete"

        streamed_status = json.loads(event_lines[0].removeprefix("data: "))
        assert streamed_status["benchmark_id"] == str(benchmark.id)
        assert streamed_status["benchmark_name"] == "streamed-benchmark"
        assert streamed_status["details"]["status"] == "FINISHED"
        assert streamed_status["final_score"] == 0.75


class TestBenchmarkTaskListing:
    """Benchmark task pagination, filtering, and status sorting."""

    def test_get_benchmark_tasks_paginates(self, client: TestClient, database_session: Session) -> None:
        """Task listing must honor limit and offset while reporting the full count.

        Test cases:
        - A page contains the requested rows and the unpaginated total.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        for task_index in range(3):
            database_session.add(make_task(benchmark, f"task-{task_index}"))
        database_session.commit()

        response = client.get(
            f"/benchmarks/{benchmark.id}/tasks?limit=2&offset=0",
            headers={"Authorization": "Bearer fake"},
        )

        assert response.status_code == 200
        response_body = response.json()
        assert len(response_body["tasks"]) == 2
        assert response_body["total_count"] == 3

    def test_get_benchmark_tasks_filters_by_status(self, client: TestClient, database_session: Session) -> None:
        """Task listing must apply comma-separated status filters.

        Test cases:
        - Only tasks in the requested statuses are returned and counted.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        finished_task = make_task(benchmark, "ok", status=TaskStatus.FINISHED)
        error_task = make_task(benchmark, "err", status=TaskStatus.ERROR)
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
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """Attention sorting must place task errors ahead of less urgent states.

        Test cases:
        - Descending status order returns errors before terminal and active tasks.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)

        # Seed tasks out of priority order to prove the route performs the sort.
        for task_id, status in [
            ("a-pending", TaskStatus.PENDING),
            ("b-finished", TaskStatus.FINISHED),
            ("c-error", TaskStatus.ERROR),
            ("d-in-progress", TaskStatus.IN_PROGRESS),
        ]:
            database_session.add(make_task(benchmark, task_id, status=status))
        database_session.commit()

        response = client.get(
            f"/benchmarks/{benchmark.id}/tasks?sort=status",
            headers={"Authorization": "Bearer fake"},
        )

        statuses = [task["status"] for task in response.json()["tasks"]]
        assert statuses == ["ERROR", "FINISHED", "IN_PROGRESS", "PENDING"]

    def test_get_benchmark_tasks_sort_by_task_id_ascending(
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """Task listing must support stable ascending task-ID order.

        Test cases:
        - Ascending task sort returns IDs in lexical order.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        for task_id in ["t-c", "t-a", "t-b"]:
            database_session.add(make_task(benchmark, task_id))
        database_session.commit()

        response_body = client.get(
            f"/benchmarks/{benchmark.id}/tasks?sort=task_id&sort_dir=asc",
            headers={"Authorization": "Bearer fake"},
        ).json()

        assert [task["task_id"] for task in response_body["tasks"]] == ["t-a", "t-b", "t-c"]


_HARNESS_HEADERS: dict[str, str] = {
    "Authorization": "Bearer fake",
    "X-Harness-AWS-Access-Key-Id": "AKIA",
    "X-Harness-AWS-Secret-Access-Key": "secret",
    "X-Harness-AWS-Default-Region": "us-east-1",
    "X-Harness-S3-Bucket": "agentic-harness",
    "X-Harness-Log-Group": "benchmarks",
    "X-Harness-Log-Retention-Policy": "30",
}


class TestBenchmarkConsoleUrls:
    """Benchmark console URL construction from harness metadata."""

    def test_get_single_benchmark_builds_run_console_urls_from_harness_headers(
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """Run detail must build CloudWatch and S3 links from valid harness headers.

        Test cases:
        - A complete header set returns both console URLs for the benchmark.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)

        response_body = client.get(f"/benchmarks/{benchmark.id}", headers=_HARNESS_HEADERS).json()

        assert "cloudwatch/home" in response_body["cloudwatch_url"]
        assert str(benchmark.id) in response_body["cloudwatch_url"]
        assert "s3/buckets/agentic-harness" in response_body["s3_bucket_url"]
        assert str(benchmark.id) in response_body["s3_bucket_url"]

    def test_get_single_benchmark_omits_console_urls_without_harness_headers(
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """Run detail must remain usable when optional harness headers are absent.

        Test cases:
        - Missing headers return null CloudWatch and S3 links instead of an error.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)

        response_body = client.get(
            f"/benchmarks/{benchmark.id}",
            headers={"Authorization": "Bearer fake"},
        ).json()

        assert response_body["cloudwatch_url"] is None
        assert response_body["s3_bucket_url"] is None

    def test_get_single_benchmark_s3_url_without_log_group_skips_cloudwatch(
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """S3 navigation must not depend on an optional CloudWatch log group.

        Test cases:
        - Complete AWS headers without a log group return only the S3 link.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)

        headers = {header: value for header, value in _HARNESS_HEADERS.items() if header != "X-Harness-Log-Group"}
        response_body = client.get(f"/benchmarks/{benchmark.id}", headers=headers).json()

        assert response_body["cloudwatch_url"] is None
        assert "s3/buckets/agentic-harness" in response_body["s3_bucket_url"]


def test_unauth_returns_401(client: TestClient) -> None:
    """Single-run routes must require authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    unknown_benchmark_id = uuid4()

    assert client.get(f"/benchmarks/{unknown_benchmark_id}").status_code == 401
    assert client.get(f"/benchmarks/{unknown_benchmark_id}/tasks").status_code == 401


class TestBenchmarkTaskSearch:
    """Benchmark task search matching and filter composition."""

    def test_get_benchmark_tasks_filters_by_task_id_search(self, client: TestClient, database_session: Session) -> None:
        """Task-ID search must narrow a run without changing the original rows.

        Test cases:
        - A substring query returns only matching task IDs.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        database_session.add_all(
            [
                make_task(benchmark, "astropy__astropy-12907", status=TaskStatus.FINISHED),
                make_task(benchmark, "django__django-11400", status=TaskStatus.FINISHED),
                make_task(benchmark, "astropy__astropy-13033", status=TaskStatus.ERROR),
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
        self,
        client: TestClient,
        database_session: Session,
    ) -> None:
        """Task-ID search must treat SQL wildcard characters as user text.

        Test cases:
        - Percent and underscore characters match only literal task IDs.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        database_session.add_all(
            [
                make_task(benchmark, "task_1"),
                make_task(benchmark, "taskA1"),
                make_task(benchmark, "task%1"),
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

    def test_get_benchmark_tasks_search_case_insensitive(self, client: TestClient, database_session: Session) -> None:
        """Task-ID search must be case-insensitive for CLI and UI parity.

        Test cases:
        - A differently cased query still finds the persisted task ID.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        database_session.add_all(
            [
                make_task(benchmark, "DJANGO__django-1"),
                make_task(benchmark, "astropy__1"),
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

    def test_get_benchmark_tasks_search_combines_with_status(
        self, client: TestClient, database_session: Session
    ) -> None:
        """Task search and status filters must compose as an intersection.

        Test cases:
        - Results satisfy both the task-ID substring and requested status.
        """
        benchmark = make_benchmark(name="bench-1", status=BenchmarkStatus.FINISHED, session=database_session)
        database_session.add_all(
            [
                make_task(benchmark, "astropy__12907", status=TaskStatus.FINISHED),
                make_task(benchmark, "astropy__13033", status=TaskStatus.ERROR),
                make_task(benchmark, "django__11400", status=TaskStatus.ERROR),
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
