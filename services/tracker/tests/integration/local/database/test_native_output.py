"""Attempt-fenced native descriptor persistence on an isolated local database.

Run: .venv/bin/pytest tests/integration/local/database/test_native_output.py -q
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from benchmark_service import ComposeSource, SnapshotSource
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    ActiveNativeOutput,
    Benchmark,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Org,
    TaskStatus,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.native_output import clear_native_output, publish_native_output
from tracker.utils.run_control import apply_shed_benchmark, apply_stop_benchmark

_STARTED_AT = datetime(2026, 7, 1, tzinfo=UTC)


def _descriptor(sandbox_id: str) -> ActiveNativeOutput:
    return ActiveNativeOutput(
        attempt_started_at=_STARTED_AT,
        sandbox_id=sandbox_id,
        runtime_source=ComposeSource(
            outer=SnapshotSource(snapshot="original-snapshot"),
            service="agent",
            compose_command="docker compose -f /run.yml",
        ),
        output_path="/agent/claude_code.log",
    )


class TestNativeOutputPersistence:
    """Publish under current authority and revoke only the original generation."""

    @pytest.mark.parametrize("status", [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS])
    def test_old_cleanup_preserves_replacement_and_revocation_allows_own_cleanup(
        self,
        database_session: Session,
        executor_authority: Callable[[Benchmark], ExecutionAuthority],
        status: TaskStatus,
    ) -> None:
        benchmark = make_benchmark(session=database_session)
        task = make_task(benchmark, "task", status=status, started_at=_STARTED_AT)
        database_session.add(task)
        database_session.commit()
        authority = executor_authority(benchmark)
        old = _descriptor("old-sandbox")
        current = _descriptor("replacement-sandbox")

        publish_native_output(database_session, task.id, old, authority)
        database_session.refresh(task)

        assert task.active_native_output is not None
        assert task.active_native_output == old
        assert task.active_native_output.generation == old.generation

        publish_native_output(database_session, task.id, current, authority)
        clear_native_output(database_session, task.id, old)
        database_session.refresh(task)

        assert task.active_native_output == current
        assert current.generation != old.generation

        dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FINISHED
        task.status = TaskStatus.STOPPED
        database_session.add(dispatch)
        database_session.add(task)
        database_session.commit()

        clear_native_output(database_session, task.id, current)
        database_session.refresh(task)

        assert task.active_native_output is None
        assert task.status == TaskStatus.STOPPED

    @pytest.mark.parametrize(
        ("operation", "stopped"), [("force-stop", {"older"}), ("stop", {"newer", "evaluating"}), ("shed", {"newer"})]
    )
    def test_stop_revokes_selected_references_only(
        self, database_session: Session, operation: str, stopped: set[str]
    ) -> None:
        benchmark = make_benchmark(session=database_session)
        tasks = [
            make_task(benchmark, name, status=status, started_at=_STARTED_AT + timedelta(seconds=index))
            for index, (name, status) in enumerate(
                [
                    ("older", TaskStatus.IN_PROGRESS),
                    ("newer", TaskStatus.BUILDING),
                    ("evaluating", TaskStatus.EVALUATING),
                ]
            )
        ]
        for task in tasks:
            task.active_native_output = _descriptor(task.task_id).model_copy(
                update={"attempt_started_at": task.started_at}
            )
            database_session.add(task)
        database_session.commit()
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        if operation == "shed":
            apply_shed_benchmark(benchmark, 2, database_session, org)
        else:
            apply_stop_benchmark(
                benchmark,
                database_session,
                operation == "force-stop",
                org,
                task_ids=["older"] if operation == "force-stop" else None,
            )
        database_session.commit()

        for task in tasks:
            database_session.refresh(task)
            assert (task.status == TaskStatus.STOPPED) == (task.task_id in stopped)
            if task.task_id in stopped:
                assert task.active_native_output is None
            else:
                assert task.active_native_output is not None
                assert task.active_native_output.sandbox_id == task.task_id

    @pytest.mark.parametrize("changed", ["attempt", "status", "evaluating", "dispatch", "benchmark"])
    def test_stale_publish_is_rejected(
        self, database_session: Session, executor_authority: Callable[[Benchmark], ExecutionAuthority], changed: str
    ) -> None:
        benchmark = make_benchmark(session=database_session)
        task = make_task(benchmark, "task", status=TaskStatus.BUILDING, started_at=_STARTED_AT)
        database_session.add(task)
        database_session.commit()
        authority = executor_authority(benchmark)
        if changed == "attempt":
            task.started_at += timedelta(seconds=1)
        elif changed in ("status", "evaluating"):
            task.status = TaskStatus.EVALUATING if changed == "evaluating" else TaskStatus.STOPPED
        elif changed == "dispatch":
            dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
            assert dispatch is not None
            dispatch.status = ExecutorDispatchStatus.FINISHED
            database_session.add(dispatch)
        else:
            other = make_benchmark(session=database_session)
            task.benchmark = other.id
        database_session.add(task)
        database_session.commit()

        with pytest.raises(ExecutionAuthorityRevoked):
            publish_native_output(database_session, task.id, _descriptor("stale-sandbox"), authority)
        database_session.rollback()
        database_session.refresh(task)

        assert task.active_native_output is None
