"""Run with `uv run pytest tests/integration/local/api/test_fetch_benchmarks_keyset.py`.

Exercise benchmark keyset pagination through the real app and local database.
"""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
)


def _seed_benchmarks(
    database_session: Session,
    count: int,
    started_by_email: str | None = None,
    start_status: BenchmarkStatus | None = None,
) -> list[Benchmark]:
    """Persist ordered benchmarks for pagination and filter scenarios."""
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    benchmarks: list[Benchmark] = []
    for benchmark_index in range(count):
        status = start_status
        if status is None:
            status = BenchmarkStatus.IN_PROGRESS if benchmark_index % 2 == 0 else BenchmarkStatus.FINISHED

        benchmark = make_benchmark(
            name=f"bench-{benchmark_index}",
            status=status,
            started_at=base + timedelta(minutes=benchmark_index),
            started_by_email=started_by_email,
        )
        database_session.add(benchmark)
        benchmarks.append(benchmark)
    database_session.commit()

    return benchmarks


class TestFetchBenchmarks:
    """Benchmark listing pagination, filters, and response fields."""

    def test_keyset_paginates_with_cursor(
        self,
        access_key_client: TestClient,
        database_session: Session,
    ) -> None:
        """Cursor pagination must return every benchmark once in stable order.

        Test cases:
        - Following the next cursor returns the remaining rows without overlap.
        """
        seeded_benchmarks = _seed_benchmarks(database_session, 5)
        first_response = access_key_client.get(
            "/fetch-benchmarks?limit=2&cursor=",
            headers={"x-api-key": "fake-key"},
        )

        assert first_response.status_code == 200, first_response.text
        first_page = first_response.json()
        assert len(first_page["benchmarks"]) == 2
        assert first_page["next_cursor"] is not None

        second_response = access_key_client.get(
            f"/fetch-benchmarks?limit=2&cursor={first_page['next_cursor']}",
            headers={"x-api-key": "fake-key"},
        )

        assert second_response.status_code == 200, second_response.text
        second_page = second_response.json()
        assert len(second_page["benchmarks"]) == 2
        assert second_page["next_cursor"] is not None

        third_response = access_key_client.get(
            f"/fetch-benchmarks?limit=2&cursor={second_page['next_cursor']}",
            headers={"x-api-key": "fake-key"},
        )

        assert third_response.status_code == 200, third_response.text
        third_page = third_response.json()
        assert len(third_page["benchmarks"]) == 1
        assert third_page["next_cursor"] is None

        returned_ids = {
            benchmark["id"] for page in (first_page, second_page, third_page) for benchmark in page["benchmarks"]
        }
        assert returned_ids == {str(benchmark.id) for benchmark in seeded_benchmarks}

    def test_filter_by_status(self, access_key_client: TestClient, database_session: Session) -> None:
        """Benchmark listing must apply status filters before pagination.

        Test cases:
        - A status query returns only matching benchmark rows.
        """
        seeded_benchmarks = _seed_benchmarks(database_session, 4)
        response = access_key_client.get(
            "/fetch-benchmarks?status=IN_PROGRESS",
            headers={"x-api-key": "fake-key"},
        )
        assert response.status_code == 200, response.text
        response_body = response.json()

        assert {benchmark["id"] for benchmark in response_body["benchmarks"]} == {
            str(benchmark.id) for benchmark in seeded_benchmarks[::2]
        }
        assert all(benchmark["status"] == "IN_PROGRESS" for benchmark in response_body["benchmarks"])

    def test_filter_by_started_after(self, access_key_client: TestClient, database_session: Session) -> None:
        """Benchmark listing must exclude rows older than the requested start boundary.

        Test cases:
        - The started-after filter returns only newer runs.
        """
        seeded_benchmarks = _seed_benchmarks(database_session, 5)

        # Let HTTPX encode the plus sign in the timezone offset.
        response = access_key_client.get(
            "/fetch-benchmarks",
            params={"started_after": "2026-01-01T00:02:00+00:00"},
            headers={"x-api-key": "fake-key"},
        )
        assert response.status_code == 200, response.text
        response_body = response.json()
        assert {benchmark["id"] for benchmark in response_body["benchmarks"]} == {
            str(benchmark.id) for benchmark in seeded_benchmarks[3:]
        }

    def test_legacy_offset_limit_still_works(
        self,
        access_key_client: TestClient,
        database_session: Session,
    ) -> None:
        """Existing offset clients must keep working alongside cursor pagination.

        Test cases:
        - Offset and limit select the expected legacy page.
        """
        _seed_benchmarks(database_session, 3)
        response = access_key_client.get(
            "/fetch-benchmarks?offset=0&limit=10",
            headers={"x-api-key": "fake-key"},
        )
        response_body = response.json()

        assert len(response_body["benchmarks"]) == 3
        assert response_body["total_count"] == 3

    def test_table_row_includes_started_by_email(
        self,
        access_key_client: TestClient,
        database_session: Session,
    ) -> None:
        """Run attribution must survive the benchmark list serialization path.

        Test cases:
        - A persisted starter email is returned in its benchmark table row.
        """
        _seed_benchmarks(database_session, 1, started_by_email="emailtest@x.com")

        response = access_key_client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})

        response_body = response.json()
        assert response_body["benchmarks"][0]["started_by_email"] == "emailtest@x.com"
