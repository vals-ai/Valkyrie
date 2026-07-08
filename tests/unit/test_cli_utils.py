import io
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tracker.database.models import BenchmarkStatus, DocentReadingStatus, TaskStatus
from tracker.types import BenchmarkDetails, FetchBenchmarkResponse, StartBenchmarkResponse

from valkyrie.cli.run.outputs import download_run_outputs
from valkyrie.cli.run.start import format_start_benchmark_response
from valkyrie.cli.run.progress import _stream_next_steps, format_benchmark_status


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


def test_format_start_benchmark_response_prints_run_outputs_command(capsys: pytest.CaptureFixture[str]) -> None:
    run_id = uuid4()
    response = StartBenchmarkResponse(
        benchmark_name="swebench",
        agent_name="agent",
        benchmark_id=run_id,
        concurrency=4,
        started_at=datetime(2026, 6, 24, tzinfo=timezone.utc),
        task_count=10,
        cloudwatch_url="https://example.com/cloudwatch",
        s3_bucket_url="s3://bucket/run",
    )

    format_start_benchmark_response(response)

    output = capsys.readouterr().out
    assert "Run outputs:" in output
    assert f"valkyrie run outputs {run_id} --output-dir ." in output
    assert "Agent outputs:" not in output


def test_stream_next_steps_prints_run_outputs_command(capsys: pytest.CaptureFixture[str]) -> None:
    run_id = uuid4()

    _stream_next_steps(run_id, s3_url="s3://bucket/run")

    output = capsys.readouterr().out
    assert "Run outputs:" in output
    assert f"valkyrie run outputs {run_id} --output-dir ." in output
    assert "s3://bucket/run" in output


def test_download_run_outputs_extracts_archive_and_nested_tars(tmp_path: Path) -> None:
    nested_bytes = io.BytesIO()
    with tarfile.open(fileobj=nested_bytes, mode="w:gz") as nested_tar:
        nested_content = b"nested contents"
        nested_info = tarfile.TarInfo("nested.txt")
        nested_info.size = len(nested_content)
        nested_tar.addfile(nested_info, io.BytesIO(nested_content))

    response_bytes = io.BytesIO()
    with tarfile.open(fileobj=response_bytes, mode="w") as tar:
        output_content = b"run output"
        output_info = tarfile.TarInfo("task/output.txt")
        output_info.size = len(output_content)
        tar.addfile(output_info, io.BytesIO(output_content))

        nested_payload = nested_bytes.getvalue()
        nested_info = tarfile.TarInfo("task/artifacts.tar.gz")
        nested_info.size = len(nested_payload)
        tar.addfile(nested_info, io.BytesIO(nested_payload))

    download_run_outputs(httpx.Response(200, content=response_bytes.getvalue()), tmp_path)

    assert (tmp_path / "task" / "output.txt").read_bytes() == b"run output"
    assert (tmp_path / "task" / "artifacts" / "nested.txt").read_bytes() == b"nested contents"
    assert not (tmp_path / "task" / "artifacts.tar.gz").exists()
