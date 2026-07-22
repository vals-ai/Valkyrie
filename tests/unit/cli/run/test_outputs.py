"""Tests for run output download commands.

Run: uv run pytest tests/unit/cli/run/test_outputs.py
"""

from importlib import import_module
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner
from tracker.database.models import AgentContractRequest, BenchmarkArguments
from tracker.types import FetchBenchmarkMetadataResponse

from valkyrie.cli.run.outputs import output_path, outputs

outputs_module = import_module("valkyrie.cli.run.outputs")

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


class MockOutputsTracker:
    """Record output retrieval while returning real tracker response models."""

    def __init__(self) -> None:
        self.response = httpx.Response(200, content=b"archive")
        self.task_ids: list[str] | None = None

    def __enter__(self) -> "MockOutputsTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark_metadata(self, _run_id: UUID) -> FetchBenchmarkMetadataResponse:
        return FetchBenchmarkMetadataResponse(
            benchmark_id=_RUN_ID,
            benchmark_name="swebench",
            benchmark_arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="agent", install_cmd="install", run_cmd="run"),
                concurrency=1,
            ),
        )

    def fetch_run_outputs(self, _run_id: UUID, task_ids: list[str] | None = None) -> httpx.Response:
        self.task_ids = task_ids
        return self.response


class TestOutputsCommands:
    """Tracker archive and direct S3 output download behavior."""

    def test_outputs_uses_metadata_default_and_task_selection(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Archive downloads must preserve task selection and choose a useful default directory.

        Test cases:
        - Comma-separated task IDs are normalized before the tracker request.
        - The default directory contains benchmark, agent, and run identity.
        - The returned archive is passed to the extraction boundary once.
        """
        tracker = MockOutputsTracker()
        extraction_calls: list[tuple[httpx.Response, Path]] = []

        def record_extraction(response: httpx.Response, output_dir: Path) -> None:
            extraction_calls.append((response, output_dir))

        monkeypatch.setattr(outputs_module, "TrackerService", lambda: tracker)
        monkeypatch.setattr(outputs_module, "download_run_outputs", record_extraction)

        result = cli_runner.invoke(outputs, [str(_RUN_ID), "--task-ids", "task-a, task-b"])

        assert result.exit_code == 0, result.output
        assert tracker.task_ids == ["task-a", "task-b"]
        assert extraction_calls == [(tracker.response, Path(f"swebench_agent_{_RUN_ID}"))]
        assert "Run outputs extracted" in result.output

    def test_output_path_builds_scoped_s3_key_and_reports_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Direct downloads must stay under the run prefix and keep failures visible.

        Test cases:
        - Surrounding subpath slashes are removed before constructing the S3 key.
        - Download errors abort the command with the original failure message.
        """
        mock_download = AsyncMock(return_value=2)
        monkeypatch.setattr(outputs_module, "download_s3_path", mock_download)

        result = cli_runner.invoke(output_path, [str(_RUN_ID), "/task-a/", "--output-dir", "/tmp/output"])

        assert result.exit_code == 0, result.output
        mock_download.assert_awaited_once_with(f"benchmarks/{_RUN_ID}/task-a", Path("/tmp/output"))
        assert "2 file(s) downloaded" in result.output

        mock_download.reset_mock(side_effect=True)
        mock_download.side_effect = RuntimeError("download unavailable")
        failed_result = cli_runner.invoke(output_path, [str(_RUN_ID)])

        assert failed_result.exit_code == 1
        assert "download unavailable" in failed_result.stderr
