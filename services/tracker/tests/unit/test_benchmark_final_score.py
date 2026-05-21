from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    FinalEvaluation,
)


def _bench(session: Session) -> Benchmark:
    b = Benchmark(
        org_id=TEST_ORG_ID,
        name="x",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="a", install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(b)
    session.commit()
    return b


def test_fetch_final_score_returns_none_when_missing(database_session: Session):
    b = _bench(database_session)
    assert b.fetch_final_score(database_session) is None


def test_fetch_final_score_returns_value(database_session: Session):
    b = _bench(database_session)
    database_session.add(FinalEvaluation(org_id=b.org_id, benchmark=b.id, final_score=0.873))
    database_session.commit()
    assert b.fetch_final_score(database_session) == 0.873
