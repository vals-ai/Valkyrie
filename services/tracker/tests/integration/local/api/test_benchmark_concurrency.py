"""Exercise live concurrency updates through the authenticated tracker app.

Run: uv run pytest tests/integration/local/api/test_benchmark_concurrency.py
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import Benchmark, BenchmarkStatus


def test_authenticated_concurrency_update_persists_full_arguments(
    client: TestClient,
    database_session: Session,
) -> None:
    """An authenticated update must return and persist the requested active-run limit."""
    benchmark = make_benchmark(concurrency=2, session=database_session)
    original_arguments = benchmark.arguments

    response = client.patch(
        f"/benchmarks/{benchmark.id}/concurrency",
        headers={"Authorization": "Bearer fake"},
        json={"concurrency": 4},
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "benchmark_id": str(benchmark.id),
        "status": BenchmarkStatus.IN_PROGRESS,
        "concurrency": 4,
    }

    database_session.expire_all()
    stored_run = database_session.get(Benchmark, benchmark.id)
    assert stored_run is not None
    assert stored_run.arguments == original_arguments.model_copy(update={"concurrency": 4})
