from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
)


def _make(session: Session, name: str, agent: str) -> Benchmark:
    b = Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=BenchmarkStatus.FINISHED,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name=agent, install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(b)
    session.commit()
    return b


def test_filter_options_returns_distinct(client, database_session):
    _make(database_session, "swebench", "mini_sweagent")
    _make(database_session, "swebench", "claude_code")
    _make(database_session, "fab", "mini_sweagent")
    _make(database_session, "swebench", "mini_sweagent")  # duplicate

    resp = client.get(
        "/benchmarks/filter-options",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["benchmark_names"] == ["fab", "swebench"]
    assert sorted(data["agent_names"]) == ["claude_code", "mini_sweagent"]


def test_filter_options_unauth_401(client):
    resp = client.get("/benchmarks/filter-options")
    assert resp.status_code == 401
