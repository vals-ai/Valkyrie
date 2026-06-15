from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    Task,
    TaskStatus,
)


def _make_bench_with_task(session: Session, task_id: str = "astropy__12907") -> tuple[Benchmark, Task]:
    b = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(b)
    session.commit()
    t = Task(org_id=b.org_id, benchmark=b.id, task_id=task_id, status=TaskStatus.FINISHED)
    session.add(t)
    session.commit()
    return b, t


def test_get_single_task_returns_payload(client, database_session):
    b, t = _make_bench_with_task(database_session)
    database_session.add(
        EvaluationResult(
            org_id=t.org_id,
            task=t.id,
            instance_id=f"{b.id}/{t.task_id}",
            result={"resolved": True, "f2p_score": 0.9},
        )
    )
    database_session.commit()

    resp = client.get(
        f"/benchmarks/{b.id}/tasks/{t.task_id}",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["task_id"] == t.task_id
    assert data["status"] == "FINISHED"
    assert data["evaluation_result"]["resolved"] is True


def test_get_single_task_404_unknown(client, database_session):
    b, _ = _make_bench_with_task(database_session)
    resp = client.get(
        f"/benchmarks/{b.id}/tasks/nonexistent",
        headers={"Authorization": "Bearer fake"},
    )
    assert resp.status_code == 404


def test_unauthenticated_returns_401(client, database_session):
    b, t = _make_bench_with_task(database_session)
    assert client.get(f"/benchmarks/{b.id}/tasks/{t.task_id}").status_code == 401
