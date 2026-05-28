from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus
from tracker.types import BenchmarkTableRow, FetchBenchmarksResponse

from valkyrie.cli.main import cli
from valkyrie.cli.utils import format_fetch_benchmarks_response


def test_format_fetch_benchmarks_response_shows_dataset(capsys):
    response = FetchBenchmarksResponse(
        total_count=2,
        benchmarks=[
            BenchmarkTableRow(
                id=uuid4(),
                name="terminal-bench",
                agent_name="terminus2",
                model="grok/grok-4.3",
                started_by_email="omar@vals.ai",
                started_at=datetime(2026, 5, 25, 19, 43, tzinfo=ZoneInfo("UTC")),
                finished_at=None,
                status=BenchmarkStatus.FINISHED,
                total_tasks=178,
                finished_tasks=178,
                final_score=47.191,
                dataset="terminal-bench-2.1",
            ),
            BenchmarkTableRow(
                id=uuid4(),
                name="swebench",
                agent_name="dummy",
                model=None,
                started_by_email=None,
                started_at=datetime(2026, 5, 25, 20, 43, tzinfo=ZoneInfo("UTC")),
                finished_at=None,
                status=BenchmarkStatus.IN_PROGRESS,
                total_tasks=1,
                finished_tasks=0,
                final_score=None,
                dataset=None,
            ),
        ],
    )

    format_fetch_benchmarks_response(response)

    output = capsys.readouterr().out
    assert "Dataset" in output
    assert "terminal-bench-2.1" in output
    assert "default" in output


def test_run_list_accepts_dataset_filter(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    class FakeTrackerService:
        def __enter__(self) -> "FakeTrackerService":
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    def fake_paginate_benchmarks(
        _tracker: FakeTrackerService,
        agent_name: str | None,
        benchmark_name: str | None,
        model: str | None,
        dataset: str | None,
        status: str | None,
        order_by: str,
        *,
        started_by: list[str] | None = None,
    ) -> None:
        captured.update(
            {
                "agent_name": agent_name,
                "benchmark_name": benchmark_name,
                "model": model,
                "dataset": dataset,
                "status": status,
                "order_by": order_by,
                "started_by": started_by,
            }
        )

    monkeypatch.setattr("valkyrie.cli.main.TrackerService", FakeTrackerService)
    monkeypatch.setattr("valkyrie.cli.main.check_tracker_service_health", lambda _tracker: True)
    monkeypatch.setattr("valkyrie.cli.main.paginate_benchmarks", fake_paginate_benchmarks)

    result = CliRunner().invoke(cli, ["run", "list", "--dataset", "terminal-bench-2.1"])

    assert result.exit_code == 0
    assert captured["dataset"] == "terminal-bench-2.1"
