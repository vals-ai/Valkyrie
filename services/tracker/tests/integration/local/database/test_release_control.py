"""Run with `uv run pytest tests/integration/local/database/test_release_control.py`.

Exercise release lifecycle locking against disposable PostgreSQL.
"""

import hashlib
from collections.abc import Callable
from io import BytesIO
from threading import Event, Thread
from time import monotonic, sleep
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from tracker.aws.executor_artifacts import S3ExecutorArtifactReader
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
    Task,
    TaskStatus,
)
from tracker.executor.dispatch_control import admit_recovery_dispatch, admit_start_dispatch
from tracker.executor.maintenance_control import begin_maintenance
from tracker.executor.release_control import (
    ReleaseControlError,
    activate_release,
    pin_benchmark_to_release,
    promote_release,
    register_release,
    retire_drained_releases,
)
from tracker.utils.resources import fetch_benchmark_row
from tracker.utils.run_control import apply_stop_benchmark


_EXECUTOR_ARTIFACT = b"immutable executor artifact"
_EXECUTOR_ARTIFACT_DIGEST = hashlib.sha256(_EXECUTOR_ARTIFACT).hexdigest()


class _S3Client:
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "artifacts"
        assert Key == "releases/concurrent-activation/executor.pex"
        return {"Body": BytesIO(_EXECUTOR_ARTIFACT)}


def _release(release_id: str) -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
    )


def _activation_candidate() -> ExecutorRelease:
    return ExecutorRelease(
        id="concurrent-activation",
        artifact_uri="s3://artifacts/releases/concurrent-activation/executor.pex",
        artifact_digest=_EXECUTOR_ARTIFACT_DIGEST,
        protocol_version="1",
    )


def _run_while_first_transaction_holds_locks(
    first: Callable[[Session], object],
    second: Callable[[Session], object],
    database_bind: Engine,
) -> list[str]:
    first_locked = Event()
    release_first = Event()
    second_connection_ready = Event()
    second_backend_pid: list[int] = []
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
        with Session(database_bind) as session:
            backend_pid = cast(int, session.connection().execute(text("SELECT pg_backend_pid()")).scalar_one())
            second_backend_pid.append(backend_pid)
            second_connection_ready.set()
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
    assert second_connection_ready.wait(5)

    deadline = monotonic() + 5
    with Session(database_bind) as observer:
        while monotonic() < deadline:
            wait_event_type = cast(
                str | None,
                observer.connection()
                .execute(
                    text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
                    {"pid": second_backend_pid[0]},
                )
                .scalar_one_or_none(),
            )
            if wait_event_type == "Lock":
                break
            assert second_thread.is_alive(), "second transaction completed without waiting on a PostgreSQL lock"
            sleep(0.01)
        else:
            raise AssertionError("second transaction did not enter a PostgreSQL lock wait")

    release_first.set()
    first_thread.join(5)
    second_thread.join(5)
    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    return outcomes


def test_concurrent_first_activation_serializes_create_or_match(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    admission = postgres_session.get(ExecutorAdmission, 1)
    if admission is None:
        postgres_session.add(ExecutorAdmission())
    else:
        admission.release_id = None
        postgres_session.add(admission)
    postgres_session.commit()

    def activate(session: Session) -> ExecutorRelease:
        return activate_release(
            session,
            _activation_candidate(),
            expected_bucket="artifacts",
            expected_prefix="releases",
            artifact_reader=S3ExecutorArtifactReader(_S3Client()),
        )

    outcomes = _run_while_first_transaction_holds_locks(
        activate,
        activate,
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    release = postgres_session.get(ExecutorRelease, "concurrent-activation")
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert release is not None
    assert admission is not None
    assert release.status == ExecutorReleaseStatus.ACTIVE
    assert release.readiness_verified
    assert admission.release_id == release.id


def test_reconciliation_and_activation_serialize_in_both_lock_orders(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    for release_id in ("v1", "v2", "v3", "v4"):
        register_release(postgres_session, _release(release_id))
    promote_release(postgres_session, "v1")
    promote_release(postgres_session, "v2")
    postgres_session.commit()

    outcomes = _run_while_first_transaction_holds_locks(
        retire_drained_releases,
        lambda session: promote_release(session, "v3"),
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    release_v1 = postgres_session.get(ExecutorRelease, "v1")
    release_v2 = postgres_session.get(ExecutorRelease, "v2")
    release_v3 = postgres_session.get(ExecutorRelease, "v3")
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert release_v1 is not None
    assert release_v2 is not None
    assert release_v3 is not None
    assert admission is not None
    assert release_v1.status == ExecutorReleaseStatus.RETIRED
    assert release_v2.status == ExecutorReleaseStatus.DRAINING
    assert release_v3.status == ExecutorReleaseStatus.ACTIVE
    assert admission.release_id == "v3"

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: promote_release(session, "v4"),
        retire_drained_releases,
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    release_v2 = postgres_session.get(ExecutorRelease, "v2")
    release_v3 = postgres_session.get(ExecutorRelease, "v3")
    release_v4 = postgres_session.get(ExecutorRelease, "v4")
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert release_v2 is not None
    assert release_v3 is not None
    assert release_v4 is not None
    assert admission is not None
    assert release_v2.status == ExecutorReleaseStatus.RETIRED
    assert release_v3.status == ExecutorReleaseStatus.RETIRED
    assert release_v4.status == ExecutorReleaseStatus.ACTIVE
    assert admission.release_id == "v4"


def test_concurrent_reconciliation_retires_each_release_once(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    register_release(postgres_session, _release("reconcile-old"))
    register_release(postgres_session, _release("reconcile-active"))
    promote_release(postgres_session, "reconcile-old")
    promote_release(postgres_session, "reconcile-active")
    postgres_session.commit()
    retired_results: list[list[str]] = []

    def reconcile(session: Session) -> None:
        retired_results.append(retire_drained_releases(session))

    outcomes = _run_while_first_transaction_holds_locks(
        reconcile,
        reconcile,
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    assert [] in retired_results
    assert sum("reconcile-old" in result for result in retired_results) == 1
    postgres_session.expire_all()
    release = postgres_session.get(ExecutorRelease, "reconcile-old")
    assert release is not None
    assert release.status == ExecutorReleaseStatus.RETIRED


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
        benchmark = session.get(Benchmark, benchmark_id, with_for_update=True)
        assert benchmark is not None
        pre_action_status = benchmark.status
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        admit_recovery_dispatch(
            session,
            benchmark=benchmark,
            pre_action_status=pre_action_status,
            dispatch_id=uuid4(),
            kind=ExecutorDispatchKind.RESUME,
        )

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


def test_start_admission_persists_benchmark_before_pending_task_autoflush(
    postgres_session: Session,
) -> None:
    org_id = uuid4()
    benchmark_id = uuid4()
    postgres_session.add(Org(id=org_id, name="start-autoflush-org"))
    register_release(postgres_session, _release("start-autoflush"))
    promote_release(postgres_session, "start-autoflush")
    postgres_session.commit()
    benchmark = Benchmark(
        id=benchmark_id,
        org_id=org_id,
        name="start-autoflush",
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="autoflush-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )
    task = Task(
        org_id=org_id,
        benchmark=benchmark_id,
        task_id="task-1",
        status=TaskStatus.PENDING,
    )
    postgres_session.add(task)

    dispatch = admit_start_dispatch(postgres_session, benchmark=benchmark, dispatch_id=uuid4())
    postgres_session.commit()

    postgres_session.refresh(benchmark)
    postgres_session.refresh(task)
    assert benchmark.current_execution_release_id == "start-autoflush"
    assert task.benchmark == benchmark_id
    assert dispatch.executor_release_id == "start-autoflush"


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
        admit_start_dispatch(session, benchmark=benchmark, dispatch_id=uuid4())

    start_first_id = uuid4()
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: start(session, benchmark_id=start_first_id, name="start-first"),
        lambda session: promote_release(session, "start-b"),
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
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

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    promotion_first = postgres_session.get(Benchmark, promotion_first_id)
    promotion_first_dispatch = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == promotion_first_id)
    ).one()
    assert promotion_first is not None
    assert promotion_first.current_execution_release_id == "start-c"
    assert promotion_first_dispatch.executor_release_id == "start-c"


def test_in_progress_retry_blocks_retirement_of_the_owned_release(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    race_org_id = uuid4()
    postgres_session.add(Org(id=race_org_id, name="retire-retry-race-org"))
    for release_id in ("retry-a", "retry-b"):
        register_release(postgres_session, _release(release_id))
    promote_release(postgres_session, "retry-a")
    promote_release(postgres_session, "retry-b")

    retry_first = Benchmark(
        org_id=race_org_id,
        name="retry-first",
        status=BenchmarkStatus.IN_PROGRESS,
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
        pre_action_status = benchmark.status
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        admit_recovery_dispatch(
            session,
            benchmark=benchmark,
            pre_action_status=pre_action_status,
            dispatch_id=uuid4(),
            kind=ExecutorDispatchKind.RETRY,
        )

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: retry(session, retry_first.id),
        retire_drained_releases,
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    stored_retry_first = postgres_session.get(Benchmark, retry_first.id)
    stored_release_a = postgres_session.get(ExecutorRelease, "retry-a")
    dispatch = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == retry_first.id)
    ).one()
    assert stored_retry_first is not None
    assert stored_release_a is not None
    assert stored_retry_first.status == BenchmarkStatus.IN_PROGRESS
    assert stored_release_a.status == ExecutorReleaseStatus.DRAINING
    assert dispatch.executor_release_id == "retry-a"


def test_terminal_retry_after_retirement_uses_the_active_release(postgres_session: Session) -> None:
    org_id = uuid4()
    postgres_session.add(Org(id=org_id, name="terminal-retire-retry-org"))
    old_release = register_release(postgres_session, _release("terminal-old"))
    register_release(postgres_session, _release("terminal-active"))
    promote_release(postgres_session, old_release.id)
    promote_release(postgres_session, "terminal-active")

    benchmark = Benchmark(
        org_id=org_id,
        name="terminal-retry",
        status=BenchmarkStatus.ERROR,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )
    pin_benchmark_to_release(benchmark, old_release)
    postgres_session.add(benchmark)
    postgres_session.commit()

    retired_ids = retire_drained_releases(postgres_session)
    assert old_release.id in retired_ids
    postgres_session.commit()
    postgres_session.expire_all()

    benchmark = postgres_session.get(Benchmark, benchmark.id, populate_existing=True, with_for_update=True)
    assert benchmark is not None
    pre_action_status = benchmark.status
    benchmark.status = BenchmarkStatus.IN_PROGRESS
    dispatch = admit_recovery_dispatch(
        postgres_session,
        benchmark=benchmark,
        pre_action_status=pre_action_status,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RETRY,
    )
    postgres_session.commit()

    stored_old_release = postgres_session.get(ExecutorRelease, old_release.id)
    assert stored_old_release is not None
    assert stored_old_release.status == ExecutorReleaseStatus.RETIRED
    assert benchmark.current_execution_release_id == "terminal-active"
    assert dispatch.executor_release_id == "terminal-active"


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
        apply_stop_benchmark(benchmark, session, force=True, org=org)
        session.flush()

    def retry(session: Session, benchmark_id: UUID) -> None:
        benchmark = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark.status == BenchmarkStatus.STOPPING:
            raise ReleaseControlError("Cannot retry a stopping benchmark")
        pre_action_status = benchmark.status
        benchmark.status = BenchmarkStatus.IN_PROGRESS
        admit_recovery_dispatch(
            session,
            benchmark=benchmark,
            pre_action_status=pre_action_status,
            dispatch_id=uuid4(),
            kind=ExecutorDispatchKind.RETRY,
        )

    stop_first = add_error_benchmark("stop-first")
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: whole_stop(session, stop_first.id),
        lambda session: retry(session, stop_first.id),
        postgres_engine,
    )
    assert sorted(outcomes) == ["first-committed", "second-committed"]

    retry_first = add_error_benchmark("retry-first")
    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: retry(session, retry_first.id),
        lambda session: whole_stop(session, retry_first.id),
        postgres_engine,
    )
    assert sorted(outcomes) == ["first-committed", "second-committed"]
    postgres_session.expire_all()
    stored_retry_first = postgres_session.get(Benchmark, retry_first.id)
    assert stored_retry_first is not None
    assert stored_retry_first.status == BenchmarkStatus.STOPPED


def test_maintenance_commit_rejects_start_waiting_on_admission_lock(
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    release = _release("maintenance-race")
    release.status = ExecutorReleaseStatus.ACTIVE
    postgres_session.add(release)
    admission = postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    admission.release_id = release.id
    postgres_session.add(admission)
    org = Org(id=uuid4(), name="maintenance-race-org")
    postgres_session.add(org)
    postgres_session.commit()

    def admit_start(session: Session) -> None:
        benchmark = Benchmark(
            id=uuid4(),
            org_id=org.id,
            name="maintenance-race-start",
            status=BenchmarkStatus.IN_PROGRESS,
            arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="race-agent", install_cmd="true", run_cmd="true"),
                concurrency=1,
            ),
        )
        admit_start_dispatch(session, benchmark=benchmark, dispatch_id=uuid4())

    outcomes = _run_while_first_transaction_holds_locks(
        lambda session: begin_maintenance(session, target_sha="a" * 40),
        admit_start,
        postgres_engine,
    )

    assert sorted(outcomes) == ["first-committed", "second-rejected"]
