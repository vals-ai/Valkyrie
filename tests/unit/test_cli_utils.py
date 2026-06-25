from datetime import datetime, timezone
from uuid import uuid4

import pytest
from tracker.database.models import BenchmarkStatus, DocentReadingStatus, TaskStatus
from tracker.types import BenchmarkDetails, FetchBenchmarkResponse

from valkyrie.cli.utils import format_benchmark_status


def test_format_benchmark_status_prints_final_score(capsys: pytest.CaptureFixture[str]) -> None:
    """Run fetch output should show the stored final score when it exists.

    Test cases:
    - A response with a final score renders that score as a percentage.
    - The existing progress line still renders.
    """
    response = FetchBenchmarkResponse(
        benchmark_name="swebench",
        benchmark_id=uuid4(),
        details=BenchmarkDetails(
            status=BenchmarkStatus.FINISHED,
            started_at=datetime(2026, 6, 24, tzinfo=timezone.utc),
            total_tasks=4,
            finished_tasks=3,
            task_breakdown={TaskStatus.FINISHED: 3, TaskStatus.ERROR: 1},
            docent_reading_status=DocentReadingStatus.IDLE,
        ),
        s3_bucket_url="https://example.com/run",
        final_score=83.25,
    )

    format_benchmark_status(response)

    output = capsys.readouterr().out
    assert "Final score:" in output
    assert "83.2%" in output
    assert "3/4 (75.0%)" in output
