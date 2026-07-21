"""Run with `uv run pytest tests/integration/local/database/test_release_control.py`.

Exercise release lifecycle locking against disposable PostgreSQL.
"""

from collections.abc import Callable
from threading import Event, Thread
from time import sleep
from uuid import UUID, uuid4

from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
)
from tracker.release_control import (
    ReleaseControlError,
    create_executor_dispatch,
    pin_benchmark_to_release,
    promote_release,
    register_release,
    resolve_current_execution_release,
    retire_if_empty,
    select_active_release,
)
from tracker.utils.resources import fetch_benchmark_row


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


def test_terminal_recovery_and_promotion_use_the_winning_admission_lock_order(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    race_org_id = uuid4()
    postgres_session.add(Org(id=race_org_id, name="release-race-org"))
    release_a = register_release(postgres_session, _release("race-a"))
    register_release(postgres_session, _release("race-b"))
    promote_release(postgres_session, release_a.id)
    benchmark_a = Benchmark(
        org_id=race_org_id,
        name="recovery-first",
        status=BenchmarkStatus.STOPPED,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )
    pin_benchmark_to_release(benchmark_a, release_a)
    postgres_session.add(benchmark_a)
    postgres_session.commit()

    def recover(session: Session, benchmark_id: UUID) -> None:
        release = select_active_release(session, for_update=True)
        benchmark = session.get(Benchmark, benchmark_id, with_for_update=True)
        assert benchmark is not None
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        benchmark.current_execution_release_id = release.id
        session.add(benchmark)
        session.add(create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.RESUME))
        session.flush()

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: recover(session, benchmark_a.id),
        lambda session: promote_release(session, "race-b"),
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    stored_a = postgres_session.get(Benchmark, benchmark_a.id)
    dispatch_a = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_a.id)
    ).one()
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert stored_a is not None
    assert admission is not None
    assert stored_a.current_execution_release_id == "race-a"
    assert dispatch_a.executor_release_id == "race-a"
    assert admission.release_id == "race-b"

    release_b = postgres_session.get(ExecutorRelease, "race-b")
    assert release_b is not None
    register_release(postgres_session, _release("race-c"))
    benchmark_b = Benchmark(
        org_id=race_org_id,
        name="promotion-first",
        status=BenchmarkStatus.STOPPED,
        arguments=benchmark_a.arguments,
    )
    pin_benchmark_to_release(benchmark_b, release_b)
    postgres_session.add(benchmark_b)
    postgres_session.commit()

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: promote_release(session, "race-c"),
        lambda session: recover(session, benchmark_b.id),
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    stored_b = postgres_session.get(Benchmark, benchmark_b.id)
    dispatch_b = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_b.id)
    ).one()
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert stored_b is not None
    assert admission is not None
    assert stored_b.current_execution_release_id == "race-c"
    assert dispatch_b.executor_release_id == "race-c"
    assert admission.release_id == "race-c"


def test_start_and_promotion_use_the_winning_admission_lock_order(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    race_org_id = uuid4()
    org = Org(id=race_org_id, name="start-promotion-race-org")
    postgres_session.add(org)
    for release_id in ("start-a", "start-b", "start-c"):
        register_release(postgres_session, _release(release_id))
    promote_release(postgres_session, "start-a")
    postgres_session.commit()

    def start(session: Session, *, benchmark_id: UUID, name: str) -> None:
        release = select_active_release(session, for_update=True)
        benchmark = Benchmark(
            id=benchmark_id,
            org_id=race_org_id,
            name=name,
            status=BenchmarkStatus.IN_PROGRESS,
            arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
                concurrency=1,
            ),
        )
        pin_benchmark_to_release(benchmark, release)
        session.add(benchmark)
        session.add(create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START))
        session.flush()

    start_first_id = uuid4()
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: start(session, benchmark_id=start_first_id, name="start-first"),
        lambda session: promote_release(session, "start-b"),
        postgres_engine,
    )

    assert outcomes == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    start_first = postgres_session.get(Benchmark, start_first_id)
    start_first_dispatch = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == start_first_id)
    ).one()
    assert start_first is not None
    assert start_first.current_execution_release_id == "start-a"
    assert start_first_dispatch.executor_release_id == "start-a"

    promotion_first_id = uuid4()
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: promote_release(session, "start-c"),
        lambda session: start(session, benchmark_id=promotion_first_id, name="promotion-first"),
        postgres_engine,
    )

    assert outcomes == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    promotion_first = postgres_session.get(Benchmark, promotion_first_id)
    promotion_first_dispatch = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == promotion_first_id)
    ).one()
    assert promotion_first is not None
    assert promotion_first.current_execution_release_id == "start-c"
    assert promotion_first_dispatch.executor_release_id == "start-c"


def test_retirement_and_retry_serialize_on_the_owned_release(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    race_org_id = uuid4()
    postgres_session.add(Org(id=race_org_id, name="retire-retry-race-org"))
    for release_id in ("retry-a", "retry-b", "retry-c", "retry-d"):
        register_release(postgres_session, _release(release_id))
    promote_release(postgres_session, "retry-a")
    promote_release(postgres_session, "retry-b")

    retry_first = Benchmark(
        org_id=race_org_id,
        name="retry-first",
        status=BenchmarkStatus.ERROR,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )
    release_a = postgres_session.get(ExecutorRelease, "retry-a")
    assert release_a is not None
    pin_benchmark_to_release(retry_first, release_a)
    postgres_session.add(retry_first)
    postgres_session.commit()

    def retry(session: Session, benchmark_id: UUID) -> None:
        benchmark = session.get(Benchmark, benchmark_id, populate_existing=True, with_for_update=True)
        assert benchmark is not None
        release = resolve_current_execution_release(session, benchmark, for_update=True)
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        session.add(benchmark)
        session.add(create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.RETRY))
        session.flush()

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: retry(session, retry_first.id),
        lambda session: retire_if_empty(session, "retry-a"),
        postgres_engine,
    )

    assert outcomes == ["first-committed", "second-rejected"]
    postgres_session.expire_all()
    stored_retry_first = postgres_session.get(Benchmark, retry_first.id)
    stored_release_a = postgres_session.get(ExecutorRelease, "retry-a")
    assert stored_retry_first is not None
    assert stored_release_a is not None
    assert stored_retry_first.status == BenchmarkStatus.IN_PROGRESS
    assert stored_release_a.status == ExecutorReleaseStatus.DRAINING

    promote_release(postgres_session, "retry-c")
    promote_release(postgres_session, "retry-d")
    retire_first = Benchmark(
        org_id=race_org_id,
        name="retire-first",
        status=BenchmarkStatus.ERROR,
        arguments=retry_first.arguments,
    )
    release_c = postgres_session.get(ExecutorRelease, "retry-c")
    assert release_c is not None
    pin_benchmark_to_release(retire_first, release_c)
    postgres_session.add(retire_first)
    postgres_session.commit()

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: retire_if_empty(session, "retry-c"),
        lambda session: retry(session, retire_first.id),
        postgres_engine,
    )

    assert outcomes == ["first-committed", "second-rejected"]
    postgres_session.expire_all()
    stored_retire_first = postgres_session.get(Benchmark, retire_first.id)
    stored_release_c = postgres_session.get(ExecutorRelease, "retry-c")
    assert stored_retire_first is not None
    assert stored_release_c is not None
    assert stored_retire_first.status == BenchmarkStatus.ERROR
    assert stored_release_c.status == ExecutorReleaseStatus.RETIRED


def test_whole_stop_and_retry_serialize_on_the_benchmark_row(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    race_org_id = uuid4()
    org = Org(id=race_org_id, name="stop-retry-race-org")
    postgres_session.add(org)
    release = register_release(postgres_session, _release("stop-retry"))
    promote_release(postgres_session, release.id)

    def add_error_benchmark(name: str) -> Benchmark:
        benchmark = Benchmark(
            org_id=race_org_id,
            name=name,
            status=BenchmarkStatus.ERROR,
            arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
                concurrency=1,
            ),
        )
        pin_benchmark_to_release(benchmark, release)
        postgres_session.add(benchmark)
        postgres_session.commit()
        return benchmark

    def whole_stop(session: Session, benchmark_id: UUID) -> None:
        benchmark = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark.status not in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPING):
            raise ReleaseControlError(f"Cannot stop benchmark from {benchmark.status}")
        benchmark.status = BenchmarkStatus.STOPPING
        session.add(benchmark)
        session.flush()

    def retry(session: Session, benchmark_id: UUID) -> None:
        benchmark = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark.status == BenchmarkStatus.STOPPING:
            raise ReleaseControlError("Cannot retry a stopping benchmark")
        dispatch_release = (
            resolve_current_execution_release(session, benchmark, for_update=True)
            if benchmark.status == BenchmarkStatus.IN_PROGRESS
            else select_active_release(session, for_update=True)
        )
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        benchmark.current_execution_release_id = dispatch_release.id
        session.add(benchmark)
        session.add(create_executor_dispatch(benchmark.id, dispatch_release, ExecutorDispatchKind.RETRY))
        session.flush()

    stop_first = add_error_benchmark("stop-first")
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: whole_stop(session, stop_first.id),
        lambda session: retry(session, stop_first.id),
        postgres_engine,
    )
    assert outcomes == ["first-committed", "second-rejected"]

    retry_first = add_error_benchmark("retry-first")
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: retry(session, retry_first.id),
        lambda session: whole_stop(session, retry_first.id),
        postgres_engine,
    )
    assert outcomes == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    stored_retry_first = postgres_session.get(Benchmark, retry_first.id)
    assert stored_retry_first is not None
    assert stored_retry_first.status == BenchmarkStatus.STOPPING
