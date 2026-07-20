"""Run with `uv run pytest tests/integration/local/database/test_release_control.py`.

Exercise release lifecycle locking against disposable PostgreSQL.
"""

from collections.abc import Callable
from threading import Event, Thread
from time import sleep

from sqlalchemy.engine import Engine
from sqlmodel import Session

from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus
from tracker.release_control import ReleaseControlError, promote_release, register_release, retire_if_empty


def _release(release_id: str) -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
    )


def _run_while_first_transaction_holds_locks(
    first: Callable[[Session], object],
    second: Callable[[Session], object],
    database_bind: Engine,
) -> list[str]:
    first_locked = Event()
    release_first = Event()
    second_started = Event()
    outcomes: list[str] = []

    def run_first() -> None:
        with Session(database_bind) as session:
            first(session)
            first_locked.set()
            assert release_first.wait(5)
            session.commit()
            outcomes.append("first-committed")

    def run_second() -> None:
        assert first_locked.wait(5)
        second_started.set()
        with Session(database_bind) as session:
            try:
                second(session)
                session.commit()
                outcomes.append("second-committed")
            except ReleaseControlError:
                session.rollback()
                outcomes.append("second-rejected")

    first_thread = Thread(target=run_first)
    second_thread = Thread(target=run_second)
    first_thread.start()
    second_thread.start()
    assert second_started.wait(5)
    sleep(0.1)
    assert second_thread.is_alive()
    release_first.set()
    first_thread.join(5)
    second_thread.join(5)
    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    return outcomes


def test_retirement_and_rollback_serialize_in_both_lock_orders(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    for release_id in ("v1", "v2", "v3", "v4"):
        register_release(postgres_session, _release(release_id))
    promote_release(postgres_session, "v1")
    promote_release(postgres_session, "v2")
    postgres_session.commit()

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: retire_if_empty(session, "v1"),
        lambda session: promote_release(session, "v1"),
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-rejected"]
    postgres_session.expire_all()
    retired = postgres_session.get(ExecutorRelease, "v1")
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert retired is not None
    assert admission is not None
    assert retired.status == ExecutorReleaseStatus.RETIRED
    assert admission.release_id == "v2"

    promote_release(postgres_session, "v3")
    promote_release(postgres_session, "v4")
    postgres_session.commit()

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: promote_release(session, "v3"),
        lambda session: retire_if_empty(session, "v3"),
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-rejected"]
    postgres_session.expire_all()
    active = postgres_session.get(ExecutorRelease, "v3")
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert active is not None
    assert admission is not None
    assert active.status == ExecutorReleaseStatus.ACTIVE
    assert admission.release_id == "v3"
