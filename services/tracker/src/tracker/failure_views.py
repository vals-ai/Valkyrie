"""Read projections for canonical failure records."""

from collections.abc import Sequence
from uuid import UUID

from sqlmodel import Session, col, desc, select

from tracker.database.models import Benchmark, BenchmarkStatus, FailureRecord, Task, TaskStatus
from tracker.types import FailureDetail, FailureSummary


def summarize_failure(record: FailureRecord) -> FailureSummary:
    return FailureSummary(
        id=record.id,
        benchmark_id=record.benchmark_id,
        task_row_id=record.task,
        task_attempt_id=record.task_attempt_id,
        dispatch_id=record.dispatch_id,
        occurred_at=record.occurred_at,
        producer=record.producer,
        operation=record.operation,
        error_type=record.error_type,
        message=record.message,
        cause_code=record.cause_code,
        retry_scheduled=record.retry_scheduled,
    )


def detail_failure(record: FailureRecord) -> FailureDetail:
    return FailureDetail(
        **summarize_failure(record).model_dump(),
        safe_details=record.safe_details,
    )


def _select_current_task_failure(task: Task, records: Sequence[FailureRecord]) -> FailureRecord | None:
    if task.status != TaskStatus.ERROR:
        return None

    if task.active_attempt_id is not None:
        for record in records:
            if record.task_attempt_id == task.active_attempt_id:
                return record

    for record in records:
        if record.task_attempt_id is None:
            return record

    return None


def current_task_failure_records(
    session: Session,
    tasks: Sequence[Task],
) -> dict[UUID, FailureRecord]:
    error_tasks = [task for task in tasks if task.status == TaskStatus.ERROR]
    if not error_tasks:
        return {}

    task_ids = [task.id for task in error_tasks]
    records = session.exec(
        select(FailureRecord)
        .where(col(FailureRecord.task).in_(task_ids))
        .where(FailureRecord.org_id == error_tasks[0].org_id)
        .where(col(FailureRecord.retry_scheduled).is_(False))
        .order_by(desc(FailureRecord.occurred_at), desc(FailureRecord.id))
    ).all()

    records_by_task: dict[UUID, list[FailureRecord]] = {}
    for record in records:
        assert record.task is not None
        records_by_task.setdefault(record.task, []).append(record)

    current: dict[UUID, FailureRecord] = {}
    for task in error_tasks:
        record = _select_current_task_failure(task, records_by_task.get(task.id, []))
        if record is not None:
            current[task.id] = record
    return current


def current_task_failure_record(session: Session, task: Task) -> FailureRecord | None:
    return current_task_failure_records(session, [task]).get(task.id)


def benchmark_task_failure_views(
    session: Session,
    benchmark_id: UUID,
    org_id: UUID,
) -> tuple[dict[str, str] | None, dict[str, FailureRecord]]:
    tasks = session.exec(
        select(Task)
        .where(Task.benchmark == benchmark_id)
        .where(Task.org_id == org_id)
        .where(Task.status == TaskStatus.ERROR)
        .order_by(Task.task_id)
    ).all()
    records = current_task_failure_records(session, tasks)
    task_errors = (
        {
            task.task_id: (records[task.id].message if task.id in records else "No error message was provided")
            for task in tasks
        }
        if tasks
        else None
    )
    task_failures = {task.task_id: records[task.id] for task in tasks if task.id in records}
    return task_errors, task_failures


def current_run_failure_records(
    session: Session,
    benchmarks: Sequence[Benchmark],
) -> dict[UUID, FailureRecord]:
    error_benchmarks = [benchmark for benchmark in benchmarks if benchmark.status == BenchmarkStatus.ERROR]
    if not error_benchmarks:
        return {}

    benchmark_ids = [benchmark.id for benchmark in error_benchmarks]
    records = session.exec(
        select(FailureRecord)
        .where(col(FailureRecord.benchmark_id).in_(benchmark_ids))
        .where(FailureRecord.org_id == error_benchmarks[0].org_id)
        .where(col(FailureRecord.task).is_(None))
        .where(col(FailureRecord.retry_scheduled).is_(False))
        .order_by(desc(FailureRecord.occurred_at), desc(FailureRecord.id))
    ).all()

    current: dict[UUID, FailureRecord] = {}
    for record in records:
        current.setdefault(record.benchmark_id, record)
    return current


def current_run_failure_record(session: Session, benchmark: Benchmark) -> FailureRecord | None:
    return current_run_failure_records(session, [benchmark]).get(benchmark.id)


def task_failure_history(
    session: Session,
    task: Task,
    *,
    limit: int,
) -> tuple[list[FailureDetail], bool]:
    records = session.exec(
        select(FailureRecord)
        .where(FailureRecord.org_id == task.org_id)
        .where(FailureRecord.task == task.id)
        .order_by(desc(FailureRecord.occurred_at), desc(FailureRecord.id))
        .limit(limit + 1)
    ).all()
    truncated = len(records) > limit
    return [detail_failure(record) for record in records[:limit]], truncated
