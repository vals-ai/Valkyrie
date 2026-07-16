"""Local integration tests for benchmark status polling."""

from uuid import uuid4

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
)


def _make_bench(name: str, status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS) -> Benchmark:
    return Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=status,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="x", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )


def test_status_returns_requested_ids(client, database_session):
    """Status polling must return current counts for each requested run.

    Test cases:
    - An authenticated request receives entries for its requested benchmark IDs.
    """
    a = _make_bench("a", BenchmarkStatus.IN_PROGRESS)
    b = _make_bench("b", BenchmarkStatus.FINISHED)
    database_session.add_all([a, b])
    database_session.commit()

    resp = client.get(
        f"/benchmarks/status?ids={a.id},{b.id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    ids = {e["id"] for e in data["entries"]}
    assert ids == {str(a.id), str(b.id)}


def test_status_ignores_foreign_ids(client, database_session):
    """Status polling must not reveal runs from another organization.

    Test cases:
    - Requested foreign benchmark IDs are omitted from the response.
    """
    bench = _make_bench("own")
    database_session.add(bench)
    database_session.commit()
    foreign = uuid4()

    resp = client.get(
        f"/benchmarks/status?ids={bench.id},{foreign}",
        headers={"Authorization": "Bearer fake"},
    )
    data = resp.json()
    assert len(data["entries"]) == 1
    assert data["entries"][0]["id"] == str(bench.id)


def test_status_no_ids_returns_empty(client):
    """An empty status request must be a successful no-op.

    Test cases:
    - Omitting benchmark IDs returns an empty entries list.
    """
    resp = client.get("/benchmarks/status", headers={"Authorization": "Bearer fake"})
    assert resp.status_code == 200
    assert resp.json() == {"entries": []}


def test_status_unauthenticated_returns_401(client):
    """Benchmark status polling must require authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    resp = client.get("/benchmarks/status?ids=00000000-0000-0000-0000-000000000001")
    assert resp.status_code == 401
