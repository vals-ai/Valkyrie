"""Tests for shared run output and display behavior.

Run: uv run pytest tests/unit/cli/run/test_output_helpers.py
"""

import io
import tarfile
import tempfile
from collections.abc import Iterator
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus, DocentReadingStatus, TaskStatus
from tracker.types import (
    BenchmarkDetails,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    StartBenchmarkResponse,
)

from valkyrie.cli.display import paginate_cli_pages
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.fetch import fetch
from valkyrie.cli.run.outputs import download_run_outputs
from valkyrie.cli.run.progress import format_benchmark_status, stream_benchmark_status
from valkyrie.cli.run.start import format_start_benchmark_response
from valkyrie.cli.tracker_client import TrackerService

from tests.unit.cli.factories import make_fetch_metadata, make_fetch_response

fetch_module = import_module("valkyrie.cli.run.fetch")


class StubProgressTracker:
    def __init__(
        self,
        response: FetchBenchmarkResponse,
        metadata: FetchBenchmarkMetadataResponse | TrackerServiceError,
        events: tuple[str, ...] = ("event: disconnect",),
    ) -> None:
        self.response = response
        self.metadata = metadata
        self.events = events
        self.metadata_calls = 0
        self.stream_calls = 0

    def fetch_benchmark(self, _run_id: UUID) -> FetchBenchmarkResponse:
        return self.response

    def fetch_benchmark_metadata(self, _run_id: UUID) -> FetchBenchmarkMetadataResponse:
        self.metadata_calls += 1
        if isinstance(self.metadata, TrackerServiceError):
            raise self.metadata
        return self.metadata

    def stream_benchmark(self, _run_id: UUID) -> Iterator[str]:
        self.stream_calls += 1
        yield from self.events


def test_format_benchmark_status_prints_terminal_details(capsys: pytest.CaptureFixture[str]) -> None:
    """Run fetch output should show the stored terminal result when it exists.

    Test cases:
    - A response with a final score renders that score as a percentage.
    - An errored response renders its stored run-level error.
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

    error_response = FetchBenchmarkResponse(
        benchmark_name="terminal-bench",
        benchmark_id=uuid4(),
        details=BenchmarkDetails(
            status=BenchmarkStatus.ERROR,
            started_at=datetime(2026, 6, 24, tzinfo=timezone.utc),
            total_tasks=4,
            finished_tasks=4,
            task_breakdown={TaskStatus.ERROR: 4},
            docent_reading_status=DocentReadingStatus.IDLE,
        ),
        s3_bucket_url="https://example.com/run",
        error_message="Dominant task error affecting 4/4 tasks",
    )

    format_benchmark_status(error_response)

    error_output = capsys.readouterr().out
    assert "Error:" in error_output
    assert "Dominant task error affecting 4/4 tasks" in error_output


def test_connected_fetch_prints_rich_identity(capsys: pytest.CaptureFixture[str]) -> None:
    run_id = uuid4()
    tracker = StubProgressTracker(make_fetch_response(run_id), make_fetch_metadata(run_id))

    stream_benchmark_status(cast(TrackerService, tracker), run_id, show_identity=True)

    output = capsys.readouterr().out
    assert "Run Details" in output
    assert "swebench" in output
    assert "mini_sweagent" in output
    assert "openai/gpt-5" in output
    assert "verified" in output
    assert str(run_id) in output
    assert "release-candidate" in output
    assert "runner@vals.ai" in output
    assert "20" in output
    assert "Streaming run updates" in output
    assert "API_KEY" not in output
    assert "secret" not in output
    assert tracker.metadata_calls == 1


def test_connected_fetch_uses_terminal_error_status(capsys: pytest.CaptureFixture[str]) -> None:
    """Connected output should render terminal errors without a misleading success message.

    Test cases:
    - A run that is already errored uses the normal fetch output without opening a stream.
    - A live run whose completion event carries an error response prints the final error status.
    """
    run_id = uuid4()
    error_message = "No tasks were completed successfully. 1 distinct error:\n- 4/4 tasks: Secret error"
    response = make_fetch_response(run_id, status=BenchmarkStatus.ERROR, finished_tasks=4)
    error_details = response.details.model_copy(update={"task_breakdown": {TaskStatus.ERROR: 4}})
    error_response = response.model_copy(update={"details": error_details, "error_message": error_message})

    # Terminal runs should use the same renderer as a regular fetch.
    terminal_tracker = StubProgressTracker(error_response, make_fetch_metadata(run_id))

    stream_benchmark_status(cast(TrackerService, terminal_tracker), run_id, show_identity=True)

    terminal_output = capsys.readouterr().out
    assert "Run Status" in terminal_output
    assert "Streaming run updates" not in terminal_output
    assert f"Error: {error_message}" in terminal_output
    assert terminal_tracker.stream_calls == 0

    # Live streams should trust the final payload status over the event name.
    live_tracker = StubProgressTracker(
        make_fetch_response(run_id),
        make_fetch_metadata(run_id),
        events=(f"data: {error_response.model_dump_json()}", "event: complete"),
    )

    stream_benchmark_status(cast(TrackerService, live_tracker), run_id, show_identity=True)

    live_output = capsys.readouterr().out
    assert "✗ Run errored." in live_output
    assert "✓ Run completed!" not in live_output
    assert f"Error: {error_message}" in live_output
    assert live_tracker.stream_calls == 1


def test_connected_fetch_continues_when_metadata_is_unavailable(capsys: pytest.CaptureFixture[str]) -> None:
    run_id = uuid4()
    tracker = StubProgressTracker(make_fetch_response(run_id), TrackerServiceError("metadata unavailable"))

    stream_benchmark_status(cast(TrackerService, tracker), run_id, show_identity=True)

    output = capsys.readouterr().out
    assert "Run Details" in output
    assert "Metadata:" in output
    assert "unavailable" in output
    assert "Streaming run updates" in output


def test_shared_connected_stream_omits_identity_by_default(capsys: pytest.CaptureFixture[str]) -> None:
    run_id = uuid4()
    tracker = StubProgressTracker(make_fetch_response(run_id), make_fetch_metadata(run_id))

    stream_benchmark_status(cast(TrackerService, tracker), run_id)

    output = capsys.readouterr().out
    assert "Run Details" not in output
    assert tracker.metadata_calls == 0


def test_fetch_connect_enables_identity_header(monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = uuid4()
    stream_calls: list[tuple[UUID, bool]] = []

    class StubFetchTrackerService:
        def __enter__(self):
            return self

        def __exit__(self, *_exc_info: object) -> None:
            return None

    def record_stream(_tracker: StubFetchTrackerService, actual_run_id: UUID, *, show_identity: bool = False) -> None:
        stream_calls.append((actual_run_id, show_identity))

    monkeypatch.setattr(fetch_module, "TrackerService", StubFetchTrackerService)
    monkeypatch.setattr(fetch_module, "stream_benchmark_status", record_stream)

    result = CliRunner().invoke(fetch, [str(run_id), "--connect"])

    assert result.exit_code == 0, result.output
    assert stream_calls == [(run_id, True)]


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


@pytest.mark.parametrize("nested", [False, True], ids=["outer-archive", "nested-archive"])
def test_download_run_outputs_rejects_archive_traversal(
    nested: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run output archives must not write outside their extraction directory.

    Test cases:
    - A parent-directory member in the response archive is rejected.
    - A parent-directory member in a nested task archive is rejected.
    - The temporary response archive is removed after either failure.
    """
    response_bytes = io.BytesIO()
    if nested:
        nested_bytes = io.BytesIO()
        with tarfile.open(fileobj=nested_bytes, mode="w:gz") as nested_tar:
            content = b"escaped"
            info = tarfile.TarInfo("../../escaped.txt")
            info.size = len(content)
            nested_tar.addfile(info, io.BytesIO(content))

        with tarfile.open(fileobj=response_bytes, mode="w") as outer_tar:
            payload = nested_bytes.getvalue()
            info = tarfile.TarInfo("task/artifacts.tar.gz")
            info.size = len(payload)
            outer_tar.addfile(info, io.BytesIO(payload))
        escaped_path = tmp_path / "output" / "escaped.txt"
    else:
        with tarfile.open(fileobj=response_bytes, mode="w") as outer_tar:
            content = b"escaped"
            info = tarfile.TarInfo("../escaped.txt")
            info.size = len(content)
            outer_tar.addfile(info, io.BytesIO(content))
        escaped_path = tmp_path / "escaped.txt"

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

    with pytest.raises(tarfile.FilterError):
        download_run_outputs(httpx.Response(200, content=response_bytes.getvalue()), tmp_path / "output")

    assert not escaped_path.exists()
    assert not list(tmp_path.glob("tmp*.tar"))


def test_paginate_cli_pages_clears_between_pages_and_handles_filtered_totals(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Shared table pagination should clear each render and recover when totals shrink.

    Test cases:
    - Moving forward clears the previous table before rendering the next page.
    - If a filtered/refetched total invalidates the current page, pagination reloads a valid page.
    """
    load_offsets: list[int] = []
    rendered_pages: list[tuple[list[str], int, int, int]] = []
    page_responses = [
        (8, ["one", "two"]),
        (8, ["three", "four"]),
        (8, ["five", "six"]),
        (4, []),
        (1, []),
        (1, ["filtered"]),
    ]
    keys = iter(["l", "l", "l"])

    def load_page(offset: int, _limit: int) -> tuple[int, list[str]]:
        response = page_responses[len(load_offsets)]
        load_offsets.append(offset)

        return response

    def render_page(page: list[str], current_page: int, total_pages: int, total_count: int) -> None:
        rendered_pages.append((page, current_page, total_pages, total_count))
        print(f"page={current_page}/{total_pages}:{','.join(page)}")

    monkeypatch.setattr("valkyrie.cli.display.click.getchar", lambda: next(keys))

    paginate_cli_pages(
        load_page,
        render_page,
        limit=2,
        render_empty=lambda: print("empty"),
    )

    output = capsys.readouterr().out
    assert load_offsets == [0, 2, 4, 6, 2, 0]
    assert rendered_pages == [
        (["one", "two"], 1, 4, 8),
        (["three", "four"], 2, 4, 8),
        (["five", "six"], 3, 4, 8),
        (["filtered"], 1, 1, 1),
    ]
    assert output.count("\033[2J\033[3J\033[1;1H") == 4
    assert "page=2/1" not in output
    assert "empty" not in output
