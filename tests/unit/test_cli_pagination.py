from datetime import UTC, datetime
from uuid import uuid4

import pytest
from tracker.database.models import BenchmarkStatus
from tracker.types import BenchmarkTableRow, FetchBenchmarksRequest, FetchBenchmarksResponse

from valkyrie.cli.utils import paginate_benchmarks


class FakeTracker:
    def __init__(self) -> None:
        self.requests: list[FetchBenchmarksRequest] = []

    def fetch_benchmarks(self, request: FetchBenchmarksRequest) -> FetchBenchmarksResponse:
        self.requests.append(request)
        return FetchBenchmarksResponse(
            benchmarks=[
                BenchmarkTableRow(
                    id=uuid4(),
                    name="code-migration",
                    agent_name="mini_swe_code_migration",
                    model="openai/gpt-5.5",
                    started_by_email=None,
                    started_at=datetime(2026, 6, 3, tzinfo=UTC),
                    finished_at=None,
                    status=BenchmarkStatus.IN_PROGRESS,
                    total_tasks=10,
                    finished_tasks=5,
                )
            ],
            total_count=6,
        )


def test_paginate_benchmarks_returns_in_noninteractive_shell(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("valkyrie.cli.utils.click.getchar", lambda: pytest.fail("getchar should not be called"))
    tracker = FakeTracker()

    paginate_benchmarks(
        tracker,
        agent_name=None,
        benchmark_name="code-migration",
        model=None,
        dataset=None,
        status=None,
        order_by="desc",
    )

    assert len(tracker.requests) == 1
    output = capsys.readouterr().out
    assert "Total: 6 run(s)" in output
    assert "next" not in output
