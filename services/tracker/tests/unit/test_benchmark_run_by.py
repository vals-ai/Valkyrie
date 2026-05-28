from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    User,
)


def test_benchmark_run_by_id_populated(database_session: Session) -> None:
    user = User(org_id=TEST_ORG_ID, email="u@x.com", descope_user_id="U_x")
    database_session.add(user)
    database_session.commit()

    bench = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        run_by_id=user.id,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    database_session.add(bench)
    database_session.commit()

    fetched = database_session.exec(select(Benchmark).where(Benchmark.id == bench.id)).one()
    assert fetched.run_by_id == user.id
