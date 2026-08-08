"""Tests for named tracker persistence repositories.

Run: uv run pytest tests/unit/database/repositories/test_repositories.py

Covers organization scope and representative benchmark/task read operations.
"""

from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import inspect
from sqlmodel import Session

from tests.factories import make_benchmark, make_error_result, make_evaluation_result, make_task
from tracker.database.models import BenchmarkStatus, DocentReadingStatus, FinalEvaluation, Org, TaskStatus
from tracker.database.repositories import (
    BenchmarkRepository,
    OrgRepository,
    ReportingRepository,
    TaskRepository,
)
from tracker.exceptions import TrackerServiceError
from tracker.types import FetchBenchmarksRequest, Order
from tracker.utils.reporting import BenchmarkContext
from tracker.utils.resources import fetch_benchmark_row, fetch_task_row


class TestOrgRepository:
    """Organization lookup behavior."""

    def test_get_by_id_returns_only_requested_organization(self, empty_database_session: Session) -> None:
        owner = Org(id=uuid4(), name="owner")
        empty_database_session.add(owner)
        empty_database_session.commit()

        repository = OrgRepository(empty_database_session)

        assert repository.get_by_id(owner.id) == owner
        assert repository.get_by_id(uuid4()) is None

    def test_find_by_name_returns_matching_organization_only(self, empty_database_session: Session) -> None:
        """Tenant lookup returns the matching organization and no result for an unknown name."""
        first_org = Org(id=uuid4(), name="first-org")
        second_org = Org(id=uuid4(), name="second-org")
        empty_database_session.add_all([first_org, second_org])
        empty_database_session.commit()

        repository = OrgRepository(empty_database_session)

        assert repository.find_by_name("first-org") == first_org
        assert repository.find_by_name("missing-org") is None

    def test_ensure_by_name_is_idempotent_and_reports_creation(self, empty_database_session: Session) -> None:
        """Organization creation is conflict-safe and leaves commit ownership with the caller."""
        repository = OrgRepository(empty_database_session)

        first_org, first_created = repository.ensure_by_name("tenant")
        assert first_org is not None
        assert first_created is True
        empty_database_session.rollback()
        assert repository.find_by_name("tenant") is None

        first_org, first_created = repository.ensure_by_name("tenant")
        assert first_org is not None
        assert first_created is True
        empty_database_session.commit()

        second_org, second_created = repository.ensure_by_name("tenant")
        assert second_org is not None
        assert second_org.id == first_org.id
        assert second_created is False
        empty_database_session.rollback()


class TestBenchmarkRepository:
    """Organization-scoped benchmark and task read behavior."""

    def test_get_for_org_hides_benchmark_from_other_organization(self, empty_database_session: Session) -> None:
        """Benchmark lookup returns no row when the requested organization does not own it."""
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)

        repository = BenchmarkRepository(empty_database_session)

        assert repository.get_for_org(benchmark.id, owner.id) == benchmark
        assert repository.get_for_org(benchmark.id, other_org.id) is None

    def test_benchmark_writes_stage_without_commit(self, empty_database_session: Session) -> None:
        benchmark = make_benchmark(session=empty_database_session)
        evaluation = FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=2.0, properties={})
        empty_database_session.add(evaluation)
        empty_database_session.commit()
        empty_database_session.refresh(benchmark)

        repository = BenchmarkRepository(empty_database_session)
        repository.stage_concurrency(benchmark, 9)
        repository.stage_resume_arguments(benchmark, benchmark.arguments, "https://service.example")
        repository.stage_docent_running(benchmark.id)
        repository.stage_docent_done(benchmark, "https://reading.example")
        repository.replace_final_evaluation(
            benchmark,
            FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=3.0, properties={}),
        )
        repository.stage_final_status(benchmark, BenchmarkStatus.FINISHED)

        empty_database_session.rollback()
        empty_database_session.refresh(benchmark)
        assert benchmark.arguments.concurrency != 9
        assert benchmark.custom_benchmark_service is None
        assert benchmark.docent_reading_status == DocentReadingStatus.IDLE
        assert benchmark.docent_reading_url is None
        assert benchmark.status == BenchmarkStatus.IN_PROGRESS
        assert benchmark.final_evaluation is not None
        assert benchmark.final_evaluation.final_score == 2.0

    def test_get_for_org_with_final_evaluation_eagerly_loads_and_scopes(self, empty_database_session: Session) -> None:
        """Eager final-evaluation lookup returns only an organization-owned benchmark."""
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
        evaluation = FinalEvaluation(org_id=owner.id, benchmark=benchmark.id, final_score=42.0, properties={})
        foreign_evaluation = FinalEvaluation(
            org_id=other_org.id, benchmark=benchmark.id, final_score=99.0, properties={}
        )
        empty_database_session.add_all([evaluation, foreign_evaluation])
        empty_database_session.commit()
        empty_database_session.expire_all()

        repository = BenchmarkRepository(empty_database_session)
        loaded = repository.get_for_org_with_final_evaluation(benchmark.id, owner.id)

        assert loaded is not None
        assert loaded.final_evaluation is not None
        assert loaded.final_evaluation.final_score == 42.0
        loaded_state = inspect(loaded)
        assert loaded_state is not None
        assert loaded_state.attrs.final_evaluation.loaded_value is loaded.final_evaluation
        assert repository.get_for_org_with_final_evaluation(benchmark.id, other_org.id) is None

    def test_scoped_eager_lookup_replaces_a_previously_loaded_foreign_evaluation(
        self, empty_database_session: Session
    ) -> None:
        """Scoped eager loading replaces an already-loaded foreign final evaluation."""
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
        empty_database_session.add(
            FinalEvaluation(org_id=other_org.id, benchmark=benchmark.id, final_score=99.0, properties={})
        )
        empty_database_session.commit()
        empty_database_session.expire_all()

        repository = BenchmarkRepository(empty_database_session)
        previously_loaded = repository.get_by_id(benchmark.id)
        assert previously_loaded is not None
        assert previously_loaded.final_evaluation is not None
        assert previously_loaded.final_evaluation.final_score == 99.0

        scoped = repository.get_for_org_with_final_evaluation(benchmark.id, owner.id)

        assert scoped is not None
        assert scoped.final_evaluation is None

    def test_get_final_score_scopes_evaluation_to_organization(self, empty_database_session: Session) -> None:
        """Final-score lookup does not expose a result row owned by another organization."""
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
        empty_database_session.add(
            FinalEvaluation(org_id=other_org.id, benchmark=benchmark.id, final_score=99.0, properties={})
        )
        empty_database_session.commit()

        repository = BenchmarkRepository(empty_database_session)

        assert repository.get_final_score(benchmark.id, owner.id) is None
        assert repository.get_final_score(benchmark.id, other_org.id) == 99.0

    def test_get_task_state_counts_scopes_tasks_and_groups_statuses(self, empty_database_session: Session) -> None:
        """Task counts include only the requested benchmark and organization."""
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
        other_benchmark = make_benchmark(org_id=other_org.id, session=empty_database_session)
        empty_database_session.add_all(
            [
                make_task(benchmark, "finished", status=TaskStatus.FINISHED),
                make_task(benchmark, "pending"),
                make_task(other_benchmark, "other"),
            ]
        )
        empty_database_session.commit()

        counts = BenchmarkRepository(empty_database_session).get_task_state_counts(benchmark.id, owner.id)

        assert counts == {TaskStatus.FINISHED: 1, TaskStatus.PENDING: 1}

    def test_get_task_status_counts_scopes_batch_to_organization(self, empty_database_session: Session) -> None:
        """Batch task counts include only requested benchmarks and organization-owned tasks."""
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
        other_benchmark = make_benchmark(org_id=other_org.id, session=empty_database_session)
        foreign_task = make_task(benchmark, "foreign", status=TaskStatus.ERROR)
        foreign_task.org_id = other_org.id
        empty_database_session.add_all(
            [
                make_task(benchmark, "finished", status=TaskStatus.FINISHED),
                make_task(benchmark, "stopped", status=TaskStatus.STOPPED),
                foreign_task,
                make_task(other_benchmark, "other", status=TaskStatus.ERROR),
            ]
        )
        empty_database_session.commit()

        counts = BenchmarkRepository(empty_database_session).get_task_status_counts([benchmark.id], owner.id)

        assert counts == {benchmark.id: {TaskStatus.FINISHED: 1, TaskStatus.STOPPED: 1}}


class TestTaskRepository:
    """Task and terminal-result lookup behavior."""

    def test_task_operations_preserve_organization_scope(self, empty_database_session: Session) -> None:
        """Task and terminal-result lookups hide rows requested through another organization."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        task = make_task(benchmark, "error", status=TaskStatus.ERROR)
        empty_database_session.add(task)
        empty_database_session.flush()
        now = datetime.now(ZoneInfo("UTC"))
        empty_database_session.add_all(
            [
                make_error_result(task, "old failure", now - timedelta(minutes=1)),
                make_error_result(task, "new failure", now),
            ]
        )
        empty_database_session.commit()

        repository = TaskRepository(empty_database_session)

        assert repository.get_for_benchmark(benchmark.id, task.task_id, other_org.id) is None
        assert repository.get_terminal_result(task, other_org.id) == (None, None)
        assert repository.get_terminal_result(task, org.id) == (None, "new failure")

    def test_get_nonterminal_for_benchmark_scopes_status_and_task_ids(self, empty_database_session: Session) -> None:
        owner = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([owner, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
        other_benchmark = make_benchmark(org_id=other_org.id, session=empty_database_session)
        foreign_task = make_task(benchmark, "foreign", status=TaskStatus.PENDING)
        foreign_task.org_id = other_org.id
        empty_database_session.add_all(
            [
                make_task(benchmark, "pending", status=TaskStatus.PENDING),
                make_task(benchmark, "stopped", status=TaskStatus.STOPPED),
                make_task(benchmark, "finished", status=TaskStatus.FINISHED),
                foreign_task,
                make_task(other_benchmark, "other", status=TaskStatus.PENDING),
            ]
        )
        empty_database_session.commit()

        repository = TaskRepository(empty_database_session)

        assert [task.task_id for task in repository.get_nonterminal_for_benchmark(benchmark.id, owner.id)] == [
            "pending"
        ]
        assert [
            task.task_id
            for task in repository.get_nonterminal_for_benchmark(
                benchmark.id, owner.id, task_ids=["pending", "foreign"]
            )
        ] == ["pending"]
        assert repository.get_nonterminal_for_benchmark(benchmark.id, other_org.id) == []

    def test_get_existing_task_ids_scopes_benchmark_and_organization(self, empty_database_session: Session) -> None:
        """Task validation returns only IDs owned by the requested organization and benchmark."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        other_benchmark = make_benchmark(org_id=other_org.id, session=empty_database_session)
        foreign_task = make_task(other_benchmark, "foreign")
        foreign_task.org_id = org.id
        empty_database_session.add_all([make_task(benchmark, "owned"), foreign_task])
        empty_database_session.commit()

        repository = TaskRepository(empty_database_session)

        assert repository.get_existing_task_ids(benchmark.id, ["foreign", "owned"], org.id) == {"owned"}
        assert repository.get_existing_task_ids(other_benchmark.id, ["foreign"], org.id) == set()
        assert repository.get_existing_task_ids(benchmark.id, ["owned"], other_org.id) == set()

    def test_task_creation_and_runnable_selection_preserve_order_and_are_idempotent(
        self, empty_database_session: Session
    ) -> None:
        """Task creation preserves requested order, filters statuses, and does not duplicate rows."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        empty_database_session.add_all(
            [
                make_task(benchmark, "finished", status=TaskStatus.FINISHED),
                make_task(benchmark, "pending"),
            ]
        )
        empty_database_session.commit()
        repository = TaskRepository(empty_database_session)

        requested = ["new", "finished", "pending", "new"]
        first_created = repository.create_missing_task_rows(benchmark.id, requested, org.id)
        first_rows = repository.get_runnable_for_benchmark(benchmark.id, requested, org.id)
        second_created = repository.create_missing_task_rows(benchmark.id, requested, org.id)
        second_rows = repository.get_runnable_for_benchmark(benchmark.id, requested, org.id)

        assert [task.task_id for task in first_created] == ["new", "finished", "pending"]
        assert [task.task_id for task in second_created] == ["new", "finished", "pending"]
        assert [task_id for task_id, _task in first_rows] == ["new", "pending"]
        assert [task_id for task_id, _task in second_rows] == ["new", "pending"]
        assert repository.get_existing_task_ids(benchmark.id, requested, org.id) == {"new", "finished", "pending"}
        assert repository.create_missing_task_rows(benchmark.id, ["foreign"], other_org.id) == []
        assert repository.get_runnable_for_benchmark(benchmark.id, ["foreign"], other_org.id) == []


class TestReportingRepository:
    """Reporting query behavior and organization isolation."""

    def test_evaluation_results_include_latest_result_and_ordered_history(
        self, empty_database_session: Session
    ) -> None:
        """Evaluation reporting selects the latest attempt and orders prior attempts newest first."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        finished_at = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))
        task = make_task(benchmark, "finished", status=TaskStatus.FINISHED, finished_at=finished_at)
        error_task = make_task(benchmark, "error", status=TaskStatus.ERROR, finished_at=finished_at)
        empty_database_session.add_all([task, error_task])
        empty_database_session.flush()
        foreign_evaluation = make_evaluation_result(task, "foreign", {"score": 999}, finished_at + timedelta(minutes=1))
        foreign_evaluation.org_id = other_org.id
        foreign_error = make_error_result(error_task, "foreign failure", finished_at + timedelta(minutes=1))
        foreign_error.org_id = other_org.id
        empty_database_session.add_all(
            [
                make_evaluation_result(
                    task,
                    "old",
                    {"score": 1},
                    finished_at - timedelta(minutes=2),
                ),
                make_error_result(error_task, "failed", finished_at - timedelta(minutes=1)),
                make_error_result(task, "retry failed", finished_at - timedelta(minutes=1)),
                make_evaluation_result(task, "latest", {"score": 2}, finished_at),
                foreign_evaluation,
                foreign_error,
            ]
        )
        empty_database_session.commit()

        repository = ReportingRepository(empty_database_session)

        results = repository.fetch_evaluation_results(benchmark.id, org.id)

        assert results["finished"]["score"] == 2
        assert results["finished"]["attempts"] == 3
        history = results["finished"]["history"]
        assert history[0]["error_message"] == "retry failed"
        assert history[1]["result"] == {"score": 1}
        assert history[0]["created_at"] > history[1]["created_at"]
        assert repository.get_task_errors(benchmark.id, org.id) == {"error": "failed"}
        assert repository.fetch_evaluation_results(benchmark.id, uuid4()) == {}

    def test_benchmark_task_counts_preserve_context_shape_and_scope(self, empty_database_session: Session) -> None:
        """Task aggregates preserve reporting totals and exclude rows from another organization."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        finished_at = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))
        finished = make_task(benchmark, "finished", status=TaskStatus.FINISHED, finished_at=finished_at)
        error = make_task(benchmark, "error", status=TaskStatus.ERROR, finished_at=finished_at)
        stopped = make_task(benchmark, "stopped", status=TaskStatus.STOPPED)
        pending = make_task(benchmark, "pending")
        foreign = make_task(benchmark, "foreign", status=TaskStatus.ERROR, finished_at=finished_at)
        foreign.org_id = other_org.id
        empty_database_session.add_all([finished, error, stopped, pending, foreign])
        empty_database_session.commit()

        reporting_repository = ReportingRepository(empty_database_session)
        counts = reporting_repository.get_benchmark_task_counts(benchmark.id, org.id)
        details = BenchmarkContext(benchmark, reporting_repository, org).benchmark_details

        assert counts.total_tasks == 4
        assert counts.finished_tasks == 3
        assert counts.failed_tasks == 1
        assert counts.status_counts == {
            TaskStatus.FINISHED: 1,
            TaskStatus.ERROR: 1,
            TaskStatus.STOPPED: 1,
            TaskStatus.PENDING: 1,
        }
        assert details.total_tasks == counts.total_tasks
        assert details.finished_tasks == counts.finished_tasks
        assert details.task_breakdown == counts.status_counts
        assert (
            ReportingRepository(empty_database_session)
            .get_benchmark_task_counts(benchmark.id, other_org.id)
            .total_tasks
            == 1
        )

    def test_final_score_inputs_select_latest_results_and_preserve_unfinished_tasks(
        self, empty_database_session: Session
    ) -> None:
        """Final-score inputs select the newest tenant-owned result and map unfinished tasks to None."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        finished = make_task(
            benchmark, "finished", status=TaskStatus.FINISHED, finished_at=datetime.now(ZoneInfo("UTC"))
        )
        pending = make_task(benchmark, "pending")
        empty_database_session.add_all([finished, pending])
        empty_database_session.flush()
        created_at = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))
        old_result = make_evaluation_result(finished, "old", {"score": 1}, created_at)
        latest_result = make_evaluation_result(finished, "latest", {"score": 2}, created_at + timedelta(minutes=1))
        foreign_result = make_evaluation_result(finished, "foreign", {"score": 999}, created_at + timedelta(minutes=2))
        foreign_result.org_id = other_org.id
        empty_database_session.add_all([old_result, latest_result, foreign_result])
        empty_database_session.commit()

        repository = ReportingRepository(empty_database_session)

        assert repository.fetch_final_score_inputs(benchmark.id, org.id) == {
            "finished": {"score": 2},
            "pending": None,
        }
        assert repository.fetch_final_score_inputs(benchmark.id, other_org.id) == {}

    def test_filtered_benchmarks_support_offset_and_keyset_pages(self, empty_database_session: Session) -> None:
        """Filtered benchmark reporting preserves organization scope across pagination modes."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        started_at = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))
        benchmarks = [
            make_benchmark(
                org_id=org.id,
                name=f"benchmark-{index}",
                started_at=started_at + timedelta(minutes=index),
                session=empty_database_session,
            )
            for index in range(3)
        ]
        make_benchmark(org_id=other_org.id, started_at=started_at, session=empty_database_session)
        empty_database_session.add(
            FinalEvaluation(org_id=other_org.id, benchmark=benchmarks[0].id, final_score=99.0, properties={})
        )
        empty_database_session.commit()
        loaded = BenchmarkRepository(empty_database_session).get_by_id(benchmarks[0].id)
        assert loaded is not None
        assert loaded.final_evaluation is not None
        assert loaded.final_evaluation.final_score == 99.0
        repository = ReportingRepository(empty_database_session)

        ascending_page = repository.fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(limit=3, order_by=Order.ASC), org.id, cursor=None
        )
        offset_page = repository.fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(limit=1, offset=1), org.id, cursor=None
        )
        keyset_page = repository.fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(limit=1, cursor=""), org.id, cursor=None
        )
        next_page = repository.fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(limit=1, cursor="ignored"),
            org.id,
            cursor=(keyset_page.rows[0].started_at, keyset_page.rows[0].id),
        )
        empty_page = repository.fetch_filtered_benchmark_rows(FetchBenchmarksRequest(limit=1), uuid4(), cursor=None)

        assert ascending_page.rows[0].id == benchmarks[0].id
        assert ascending_page.rows[0].final_evaluation is None
        assert offset_page.total_count == 3
        assert offset_page.rows[0].id == benchmarks[1].id
        assert keyset_page.has_next_page is True
        assert next_page.rows[0].id != keyset_page.rows[0].id
        assert empty_page.rows == []
        assert empty_page.total_count == 0

    def test_task_counts_and_resource_adapters_preserve_scope(self, empty_database_session: Session) -> None:
        """Reporting counts and resource adapters reject missing or cross-organization rows."""
        org = Org(id=uuid4(), name="owner")
        other_org = Org(id=uuid4(), name="other")
        empty_database_session.add_all([org, other_org])
        empty_database_session.commit()
        benchmark = make_benchmark(org_id=org.id, session=empty_database_session)
        finished = make_task(benchmark, "finished", status=TaskStatus.FINISHED)
        finished.finished_at = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))
        stopped = make_task(benchmark, "stopped", status=TaskStatus.STOPPED)
        empty_database_session.add_all([finished, stopped])
        empty_database_session.commit()
        repository = ReportingRepository(empty_database_session)
        benchmark_repository = BenchmarkRepository(empty_database_session)
        task_repository = TaskRepository(empty_database_session)

        assert repository.get_stopped_task_count(benchmark.id, org.id) == 1
        assert fetch_benchmark_row(benchmark.id, benchmark_repository, org).id == benchmark.id
        assert fetch_task_row(finished.id, task_repository, org).id == finished.id

        with pytest.raises(ValueError, match="does not belong"):
            fetch_benchmark_row(benchmark.id, benchmark_repository, other_org)
        with pytest.raises(TrackerServiceError, match="does not belong"):
            fetch_task_row(finished.id, task_repository, other_org)
        with pytest.raises(TrackerServiceError, match="not found"):
            fetch_task_row(uuid4(), task_repository, org)
