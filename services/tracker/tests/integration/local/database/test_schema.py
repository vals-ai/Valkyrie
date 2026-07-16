"""Run with `uv run pytest tests/integration/local/database/test_schema.py`.

Exercise schema and model events against disposable Postgres.
"""

from sqlalchemy.engine import Engine
from sqlmodel import Session, inspect

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    Org,
    Task,
    TaskStatus,
)


class TestTrackerSchema:
    """Tracker schema tables and database status hooks."""

    def test_tracker_schema_contains_core_tables(self, postgres_engine: Engine) -> None:
        """Migrations and models must create the tracker persistence boundary.

        Test cases:
        - Disposable Postgres contains the benchmark and task tables.
        - Evaluation and final-score tables are also present.
        """
        tables = set(inspect(postgres_engine).get_table_names())

        assert {"benchmark", "task", "evaluationresult", "finalevaluation"} <= tables

    def test_terminal_statuses_set_finished_timestamps_in_postgres(self, postgres_session: Session) -> None:
        """Terminal state transitions must persist completion timestamps in production Postgres.

        Test cases:
        - Finishing a benchmark sets its completion timestamp.
        - Marking a task as errored sets its completion timestamp.
        """
        postgres_session.add(Org(id=TEST_ORG_ID, name="postgres-status-org"))
        postgres_session.flush()

        benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name="postgres-status-events",
            arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="status-agent", install_cmd="true", run_cmd="true"),
                concurrency=1,
            ),
        )
        postgres_session.add(benchmark)
        postgres_session.flush()
        assert benchmark.finished_at is None

        benchmark.status = BenchmarkStatus.FINISHED
        postgres_session.add(benchmark)
        postgres_session.flush()
        assert benchmark.finished_at is not None

        task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="status-task")
        postgres_session.add(task)
        postgres_session.flush()
        assert task.finished_at is None

        task.status = TaskStatus.ERROR
        postgres_session.add(task)
        postgres_session.flush()
        assert task.finished_at is not None
