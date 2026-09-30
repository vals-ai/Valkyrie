"""PostgreSQL integration tests for sandbox build reservation storage."""

from collections.abc import Callable
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy.engine import Engine
from sqlmodel import Session

from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    Org,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.scheduler import store

_ATTEMPT = datetime(2026, 9, 29, 12)
_LATER_ATTEMPT = _ATTEMPT + timedelta(minutes=5)


def _task(session: Session, pool_id: str, started_at: datetime = _ATTEMPT) -> tuple[Benchmark, Task]:
    org = Org(name=f"reservation-{uuid4()}")
    session.add(org)
    session.flush()
    benchmark = Benchmark(
        org_id=org.id,
        name=f"run-{uuid4()}",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            concurrency=10,
            queue_pool_id=pool_id,
        ),
    )
    session.add(benchmark)
    session.flush()
    task = Task(
        org_id=org.id,
        task_id=f"task-{uuid4()}",
        benchmark=benchmark.id,
        status=TaskStatus.PENDING,
        started_at=started_at,
    )
    session.add(task)
    session.commit()
    return benchmark, task


def _claim(
    session: Session,
    pool_id: str,
    task: Task,
    authority: ExecutionAuthority,
    demand: tuple[int, int, int, int],
) -> UUID:
    build_id = uuid4()
    assert store.claim_eligible_task_with_reservation(
        session,
        pool_id,
        task.id,
        task.started_at,
        build_id=build_id,
        executor_dispatch_id=authority.dispatch_id,
        requested_vcpu=demand[0],
        requested_memory=demand[1],
        requested_disk=demand[2],
        requested_gpu=demand[3],
    )
    return build_id


def test_claim_and_resource_sum_share_the_caller_transaction(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Callable[..., ExecutionAuthority],
) -> None:
    pool_id = store.queue_pool_id(f"provider:{uuid4()}")
    runs = (
        _task(postgres_session, pool_id),
        _task(postgres_session, pool_id, _ATTEMPT + timedelta(microseconds=1)),
    )
    tasks = tuple(task for _, task in runs)
    authorities = tuple(executor_authority(benchmark, session=postgres_session) for benchmark, _ in runs)
    build_ids = (
        _claim(postgres_session, pool_id, tasks[0], authorities[0], (2, 4, 8, 1)),
        _claim(postgres_session, pool_id, tasks[1], authorities[1], (3, 6, 12, 0)),
    )

    assert store.active_reservation_resources(postgres_session, pool_id) == store.ReservationResourceTotals(
        vcpu=5, memory=10, disk=20, gpu=1
    )
    assert all(postgres_session.get(Task, task.id).status == TaskStatus.BUILDING for task in tasks)  # type: ignore[union-attr]
    assert all(postgres_session.get(SandboxBuildReservation, build_id) is not None for build_id in build_ids)

    postgres_session.rollback()
    with Session(postgres_engine) as observer:
        assert all(observer.get(Task, task.id).status == TaskStatus.PENDING for task in tasks)  # type: ignore[union-attr]
        assert all(observer.get(SandboxBuildReservation, build_id) is None for build_id in build_ids)


def test_reservation_protects_reset_then_promotion_allows_a_later_attempt(
    postgres_session: Session,
    executor_authority: Callable[..., ExecutionAuthority],
) -> None:
    pool_id = store.queue_pool_id(f"provider:{uuid4()}")
    benchmark, task = _task(postgres_session, pool_id)
    authority = executor_authority(benchmark, session=postgres_session)
    first_build = _claim(postgres_session, pool_id, task, authority, (1, 2, 3, 0))
    postgres_session.commit()

    store.reset_abandoned_builds(postgres_session, pool_id, _LATER_ATTEMPT)
    postgres_session.commit()
    assert task.status == TaskStatus.BUILDING
    assert postgres_session.get(SandboxBuildReservation, first_build) is not None

    assert store.promote_reserved_task(
        postgres_session,
        task_row_id=task.id,
        expected_started_at=task.started_at,
        build_id=first_build,
    )
    postgres_session.commit()
    assert task.status == TaskStatus.IN_PROGRESS
    assert postgres_session.get(SandboxBuildReservation, first_build) is None

    task.status = TaskStatus.PENDING
    task.started_at = _LATER_ATTEMPT
    postgres_session.add(task)
    postgres_session.commit()
    second_build = _claim(postgres_session, pool_id, task, authority, (1, 2, 3, 0))
    postgres_session.commit()

    assert second_build != first_build
    assert postgres_session.get(SandboxBuildReservation, second_build) is not None


def test_revoked_build_retains_capacity_and_blocks_a_new_attempt(
    postgres_session: Session,
    executor_authority: Callable[..., ExecutionAuthority],
) -> None:
    pool_id = store.queue_pool_id(f"provider:{uuid4()}")
    benchmark, task = _task(postgres_session, pool_id)
    authority = executor_authority(benchmark, session=postgres_session)
    build_id = _claim(postgres_session, pool_id, task, authority, (1, 2, 3, 0))
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.FAILED
    postgres_session.add(dispatch)
    postgres_session.commit()

    store.reset_abandoned_builds(postgres_session, pool_id, _LATER_ATTEMPT)
    postgres_session.commit()
    assert task.status == TaskStatus.BUILDING
    assert postgres_session.get(SandboxBuildReservation, build_id) is not None

    # Retrying the terminal task must not allocate while the old outcome is unknown.
    task.status = TaskStatus.PENDING
    task.started_at = _LATER_ATTEMPT
    postgres_session.add(task)
    postgres_session.commit()
    assert not store.eligible_task_is(postgres_session, pool_id, task.id, _LATER_ATTEMPT)
    assert not store.claim_eligible_task(postgres_session, pool_id, task.id, _LATER_ATTEMPT)
    assert store.active_reservation_resources(postgres_session, pool_id).vcpu == 1
