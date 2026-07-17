"""Tests for run result retrieval and file persistence.

Run: uv run pytest tests/unit/cli/run/test_results.py
"""

import json
from importlib import import_module
from pathlib import Path
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus
from tracker.types import FinalViewResponse, RetrieveResultsResponse, S3UploadResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.results import results

from tests.unit.cli.factories import make_final_view

results_module = import_module("valkyrie.cli.run.results")

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


class MockResultsTracker:
    """Return configured result payloads and record retrieval choices."""

    def __init__(
        self,
        response: RetrieveResultsResponse | TrackerServiceError,
        *,
        results_exist: bool = False,
    ) -> None:
        self.response = response
        self.results_exist = results_exist
        self.retrieve_calls: list[tuple[UUID, bool, list[str] | None]] = []

    def __enter__(self) -> "MockResultsTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def check_results_exist_in_s3(self, _run_id: UUID) -> bool:
        return self.results_exist

    def retrieve_results(
        self,
        run_id: UUID,
        s3: bool,
        task_ids: list[str] | None = None,
    ) -> RetrieveResultsResponse:
        self.retrieve_calls.append((run_id, s3, task_ids))
        if isinstance(self.response, TrackerServiceError):
            raise self.response

        return self.response


class TestResultsCommand:
    """Local files, subset summaries, S3 links, and overwrite protection."""

    def test_local_results_write_complete_payload_and_subset_summary(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Local result retrieval must persist the tracker payload and explain partial subsets.

        Test cases:
        - Evaluation results and task errors are written to the selected JSON path.
        - The subset summary reports scored and requested task counts.
        - Task selection is forwarded once to the tracker.
        - Agent secrets and private runtime kwargs are excluded from the saved file.
        """
        response = make_final_view(
            _RUN_ID,
            status=BenchmarkStatus.FINISHED,
            error_message=None,
            task_errors={"task-b": "evaluation failed"},
            evaluation_results={"task-a": {"score": 1}},
        )
        tracker = MockResultsTracker(response)
        output_path = tmp_path / "results.json"
        monkeypatch.setattr(results_module, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(
            results,
            [str(_RUN_ID), "--path", str(output_path), "--task-ids", "task-a,task-b,missing"],
        )

        assert result.exit_code == 0, result.output
        assert "Scored over 2 of 3 subset task ids" in result.output
        assert tracker.retrieve_calls == [(_RUN_ID, False, ["task-a", "task-b", "missing"])]

        saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
        assert saved_payload["benchmark_id"] == str(_RUN_ID)
        assert saved_payload["evaluation_results"] == {"task-a": {"score": 1}}
        assert saved_payload["task_errors"] == {"task-b": "evaluation failed"}
        assert "secrets" not in saved_payload["benchmark_arguments"]["contract"]
        assert "kwargs" not in saved_payload["benchmark_arguments"]["contract"]

    def test_s3_results_render_links_and_protect_existing_uploads(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """S3 result retrieval must show usable links without silently overwriting existing data.

        Test cases:
        - A new S3 result prints its presigned and console URLs.
        - Declining an existing-result overwrite aborts before retrieval.
        """
        response = S3UploadResultsResponse(
            s3_url="s3://bucket/results.json",
            presigned_url="https://download.example/results",
            console_url="https://console.aws.amazon.com/s3/object/results",
        )
        tracker = MockResultsTracker(response)
        monkeypatch.setattr(results_module, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(results, [str(_RUN_ID), "--s3"])

        assert result.exit_code == 0, result.output
        assert "https://download.example/results" in result.output
        assert "https://console.aws.amazon.com/s3/object/results" in result.output
        assert tracker.retrieve_calls == [(_RUN_ID, True, None)]

        existing_tracker = MockResultsTracker(response, results_exist=True)
        monkeypatch.setattr(results_module, "TrackerService", lambda: existing_tracker)

        declined_result = cli_runner.invoke(results, [str(_RUN_ID), "--s3"], input="n\n")

        assert declined_result.exit_code == 1
        assert "Overwrite" in declined_result.output
        assert existing_tracker.retrieve_calls == []

    @pytest.mark.parametrize(
        ("path", "expected_message"),
        [
            ("missing/results.json", "directory does not exist"),
            ("existing.json", "Aborted"),
        ],
    )
    def test_local_output_refuses_invalid_or_declined_paths(
        self,
        path: str,
        expected_message: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Result files must not be created in missing directories or overwrite without consent.

        Test cases:
        - A missing parent directory returns an actionable error.
        - Declining overwrite preserves the existing file contents.
        """
        response: FinalViewResponse = make_final_view(_RUN_ID)
        tracker = MockResultsTracker(response)
        output_path = tmp_path / path
        if output_path.name == "existing.json":
            output_path.write_text("original", encoding="utf-8")
        monkeypatch.setattr(results_module, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(
            results,
            [str(_RUN_ID), "--path", str(output_path)],
            input="n\n",
        )

        assert result.exit_code == 1
        assert expected_message in result.output
        if output_path.exists():
            assert output_path.read_text(encoding="utf-8") == "original"
