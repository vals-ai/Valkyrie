from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID, FakeAccessKeys
from tracker.database.models import BenchmarkStatus, Org, RunAccessKey
from tracker.run_access_keys import delete_finished_run_keys


def test_sweep_skips_keys_another_sweep_holds(
    postgres_engine: Engine,
    postgres_session: Session,
    fake_access_keys: FakeAccessKeys,
) -> None:
    postgres_session.add(Org(id=TEST_ORG_ID, name="default"))
    postgres_session.commit()
    for status, key_id in (
        (BenchmarkStatus.STOPPED, "K-held"),
        (BenchmarkStatus.FINISHED, "K-free"),
        (BenchmarkStatus.IN_PROGRESS, "K-running"),
    ):
        benchmark = make_benchmark(status=status)
        postgres_session.add_all([benchmark, RunAccessKey(key_id=key_id, benchmark_id=benchmark.id)])
    postgres_session.commit()

    with Session(postgres_engine) as resume:
        resume.exec(select(RunAccessKey).where(RunAccessKey.key_id == "K-held").with_for_update()).one()
        delete_finished_run_keys(postgres_engine)
        assert fake_access_keys.deleted == ["K-free"]

    delete_finished_run_keys(postgres_engine)
    assert fake_access_keys.deleted == ["K-free", "K-held"]
    assert set(postgres_session.exec(select(RunAccessKey.key_id)).all()) == {"K-running"}
