"""Run with `uv run pytest tests/integration/local/api/test_single_task.py`.

Exercise single-task routes through the real app and local database.
"""

from datetime import datetime
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark, make_error_result, make_evaluation_result, make_task
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    BenchmarkStatus,
    Org,
    Task,
    TaskStatus,
)

_UTC = ZoneInfo("UTC")


def _persist_benchmark_with_task(
    database_session: Session,
    task_id: str = "astropy__12907",
) -> tuple[Benchmark, Task]:
    """Persist a benchmark and finished task for one route scenario."""
    benchmark = make_benchmark(status=BenchmarkStatus.IN_PROGRESS)
    database_session.add(benchmark)
    database_session.commit()
    task = make_task(benchmark, task_id, status=TaskStatus.FINISHED)
    database_session.add(task)
    database_session.commit()

    return benchmark, task


def _created_at(hour: int) -> datetime:
    """Return a stable timestamp for ordering result history."""
    return datetime(2026, 6, 24, hour, tzinfo=_UTC)


class TestSingleTask:
    """Single task responses, missing tasks, and authentication."""

    def test_get_single_task_returns_current_result_or_error(
        self, client: TestClient, database_session: Session
    ) -> None:
        """The single task endpoint should expose only the current task outcome.

        Test cases:
        - A finished task returns its latest evaluation result and no stale error message.
        - An errored task returns its latest error message and no stale evaluation result.
        """
        benchmark, finished_task = _persist_benchmark_with_task(database_session, task_id="finished")
        error_task = make_task(benchmark, "errored", status=TaskStatus.ERROR)
        database_session.add(error_task)
        database_session.flush()

        old_created_at = _created_at(12)
        new_created_at = _created_at(13)
        for result_row in (
            make_error_result(finished_task, "old finished task error", old_created_at),
            make_evaluation_result(finished_task, "finished-old", {"attempt": "old"}, old_created_at),
            make_evaluation_result(finished_task, "finished-new", {"attempt": "new"}, new_created_at),
            make_evaluation_result(error_task, "errored-old", {"attempt": "old success"}, old_created_at),
            make_error_result(error_task, "old error", old_created_at),
            make_error_result(error_task, "new error", new_created_at),
        ):
            database_session.add(result_row)
        database_session.commit()

        finished_response = client.get(
            f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}",
            headers={"Authorization": "Bearer fake"},
        )
        assert finished_response.status_code == 200, finished_response.text
        finished_data: dict[str, Any] = finished_response.json()
        assert finished_data["task_id"] == finished_task.task_id
        assert finished_data["status"] == "FINISHED"
        assert finished_data["error_message"] is None
        assert finished_data["evaluation_result"] == {"attempt": "new"}

        error_response = client.get(
            f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}",
            headers={"Authorization": "Bearer fake"},
        )
        assert error_response.status_code == 200, error_response.text
        error_data: dict[str, Any] = error_response.json()
        assert error_data["task_id"] == error_task.task_id
        assert error_data["status"] == "ERROR"
        assert error_data["error_message"] == "new error"
        assert error_data["evaluation_result"] is None

    def test_get_single_task_404_unknown(self, client: TestClient, database_session: Session) -> None:
        """Unknown tasks must return not found inside an existing benchmark.

        Test cases:
        - A missing task ID receives 404 without altering the benchmark.
        """
        benchmark, _task = _persist_benchmark_with_task(database_session)

        response = client.get(
            f"/benchmarks/{benchmark.id}/tasks/nonexistent",
            headers={"Authorization": "Bearer fake"},
        )

        assert response.status_code == 404

    def test_unauthenticated_returns_401(self, client: TestClient, database_session: Session) -> None:
        """Single-task detail must require authentication.

        Test cases:
        - A request without a bearer session receives 401.
        """
        benchmark, task = _persist_benchmark_with_task(database_session)

        assert client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}").status_code == 401
        assert client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts").status_code == 401

    def test_get_task_attempts_pages_mixed_outcomes_without_collapsing_errors(
        self, client: TestClient, database_session: Session
    ) -> None:
        """Attempts retain every persisted error and evaluation in stable newest-first order."""
        benchmark, task = _persist_benchmark_with_task(database_session)
        newest_evaluation = make_evaluation_result(
            task,
            "evaluation-new",
            {"score": 0.75, "resolved": True},
            _created_at(16),
        )
        repeated_error_new = make_error_result(task, "identical failure", _created_at(15))
        repeated_error_old = make_error_result(task, "identical failure", _created_at(15))
        older_evaluation = make_evaluation_result(
            task,
            "evaluation-old",
            {"score": 0.25},
            _created_at(14),
            exit_reason=AgentCausedExitReason.TIMEOUT,
        )
        newest_evaluation.id = UUID("00000000-0000-0000-0000-000000000004")
        newest_evaluation.attempt_id = "a4"
        repeated_error_new.id = UUID("00000000-0000-0000-0000-000000000003")
        repeated_error_new.attempt_id = "a3"
        repeated_error_old.id = UUID("00000000-0000-0000-0000-000000000002")
        older_evaluation.id = UUID("00000000-0000-0000-0000-000000000001")
        older_evaluation.attempt_id = "a1"
        database_session.add_all(
            [
                newest_evaluation,
                repeated_error_new,
                repeated_error_old,
                older_evaluation,
            ]
        )
        database_session.commit()

        first_response = client.get(
            f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts?limit=2",
            headers={"Authorization": "Bearer fake"},
        )
        second_response = client.get(
            f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts?limit=2&offset=2",
            headers={"Authorization": "Bearer fake"},
        )

        assert first_response.status_code == 200, first_response.text
        assert second_response.status_code == 200, second_response.text
        first_page: dict[str, Any] = first_response.json()
        second_page: dict[str, Any] = second_response.json()
        assert first_page["total_count"] == second_page["total_count"] == 4
        assert first_page["attempts"] == [
            {
                "kind": "evaluation",
                "id": str(newest_evaluation.id),
                "attempt_id": "a4",
                "created_at": "2026-06-24T16:00:00+00:00",
                "instance_id": "evaluation-new",
                "agent_caused_exit_reason": None,
            },
            {
                "kind": "error",
                "id": str(repeated_error_new.id),
                "attempt_id": "a3",
                "created_at": "2026-06-24T15:00:00+00:00",
                "error_message": "identical failure",
                "error_message_truncated": False,
                "error_fingerprint": sha256(b"identical failure").hexdigest(),
            },
        ]
        assert second_page["attempts"] == [
            {
                "kind": "error",
                "id": str(repeated_error_old.id),
                "attempt_id": None,
                "created_at": "2026-06-24T15:00:00+00:00",
                "error_message": "identical failure",
                "error_message_truncated": False,
                "error_fingerprint": sha256(b"identical failure").hexdigest(),
            },
            {
                "kind": "evaluation",
                "id": str(older_evaluation.id),
                "attempt_id": "a1",
                "created_at": "2026-06-24T14:00:00+00:00",
                "instance_id": "evaluation-old",
                "agent_caused_exit_reason": "TIMEOUT",
            },
        ]

    def test_get_task_attempts_isolates_task_run_and_org(self, client: TestClient, database_session: Session) -> None:
        """Only outcomes for the authenticated org's exact run and task are returned."""
        benchmark, task = _persist_benchmark_with_task(database_session, task_id="same-task-id")
        sibling_task = make_task(benchmark, "sibling", status=TaskStatus.ERROR)
        other_benchmark = make_benchmark()
        other_run_task = make_task(other_benchmark, task.task_id, status=TaskStatus.ERROR)
        other_org = Org(id=uuid4(), name="other")
        foreign_benchmark = make_benchmark(org_id=other_org.id)
        foreign_task = make_task(foreign_benchmark, task.task_id, status=TaskStatus.ERROR)
        database_session.add_all(
            [
                sibling_task,
                other_benchmark,
                other_run_task,
                other_org,
                foreign_benchmark,
                foreign_task,
            ]
        )
        database_session.flush()

        target_error = make_error_result(task, "target", _created_at(12))
        wrong_task_error = make_error_result(sibling_task, "wrong task", _created_at(16))
        wrong_run_error = make_error_result(other_run_task, "wrong run", _created_at(16))
        wrong_org_error = make_error_result(foreign_task, "wrong org", _created_at(16))
        mismatched_org_error = make_error_result(task, "wrong result org", _created_at(16))
        mismatched_org_error.org_id = other_org.id
        database_session.add_all(
            [
                target_error,
                wrong_task_error,
                wrong_run_error,
                wrong_org_error,
                mismatched_org_error,
            ]
        )
        database_session.commit()

        response = client.get(
            f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts",
            headers={"Authorization": "Bearer fake"},
        )
        foreign_response = client.get(
            f"/benchmarks/{foreign_benchmark.id}/tasks/{foreign_task.task_id}/attempts",
            headers={"Authorization": "Bearer fake"},
        )

        assert response.status_code == 200, response.text
        assert response.json() == {
            "attempts": [
                {
                    "kind": "error",
                    "id": str(target_error.id),
                    "attempt_id": None,
                    "created_at": "2026-06-24T12:00:00+00:00",
                    "error_message": "target",
                    "error_message_truncated": False,
                    "error_fingerprint": sha256(b"target").hexdigest(),
                }
            ],
            "total_count": 1,
        }
        assert foreign_response.status_code == 404
