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
from tracker.types import FetchBenchmarkResponse, FinalViewResponse, RetrieveResultsResponse, S3UploadResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.results import results

from tests.unit.cli.factories import make_fetch_response, make_final_view

results_module = import_module("valkyrie.cli.run.results")

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


class MockResultsTracker:
    """Return configured result payloads and record retrieval choices."""

    def __init__(
        self,
        response: RetrieveResultsResponse | TrackerServiceError,
        *,
        results_exist: bool = False,
        status: BenchmarkStatus = BenchmarkStatus.FINISHED,
    ) -> None:
        self.response = response
        self.results_exist = results_exist
        self.status = status
        self.retrieve_calls: list[tuple[UUID, bool, list[str] | None, bool]] = []

    def __enter__(self) -> "MockResultsTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def fetch_benchmark(self, run_id: UUID) -> FetchBenchmarkResponse:
        return make_fetch_response(run_id, status=self.status)

    def check_results_exist_in_s3(self, _run_id: UUID) -> bool:
        return self.results_exist

    def retrieve_results(
        self,
        run_id: UUID,
        s3: bool,
        task_ids: list[str] | None = None,
        *,
        preview: bool = False,
    ) -> RetrieveResultsResponse:
        self.retrieve_calls.append((run_id, s3, task_ids, preview))
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
        assert tracker.retrieve_calls == [(_RUN_ID, False, ["task-a", "task-b", "missing"], False)]

        saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
        assert saved_payload["benchmark_id"] == str(_RUN_ID)
        assert saved_payload["evaluation_results"] == {"task-a": {"score": 1}}
        assert saved_payload["task_errors"] == {"task-b": "evaluation failed"}
        assert "secrets" not in saved_payload["benchmark_arguments"]["contract"]
        assert "kwargs" not in saved_payload["benchmark_arguments"]["contract"]
        assert "results are partial" not in result.output

    @pytest.mark.parametrize(
        ("status", "warns"),
        [
            (BenchmarkStatus.IN_PROGRESS, True),
            (BenchmarkStatus.STOPPING, True),
            (BenchmarkStatus.STOPPED, False),
            (BenchmarkStatus.ERROR, False),
        ],
    )
    def test_unfinished_runs_warn_that_results_are_partial(
        self,
        status: BenchmarkStatus,
        warns: bool,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Users retrieving results mid-run must learn the payload is incomplete.

        Test cases:
        - IN_PROGRESS and STOPPING runs print a partial-results warning naming the status.
        - Terminal statuses save results without the warning.
        - The warning appears for local files and for S3 uploads, before the overwrite prompt.
        """
        warning = f"Run is {status.value}; results are partial"
        tracker = MockResultsTracker(make_final_view(_RUN_ID, status=status), status=status)
        output_path = tmp_path / "results.json"
        monkeypatch.setattr(results_module, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(results, [str(_RUN_ID), "--path", str(output_path)])

        assert result.exit_code == 0, result.output
        assert output_path.exists()
        assert (warning in result.output) is warns

        s3_response = S3UploadResultsResponse(
            s3_url="s3://bucket/results.json",
            presigned_url="https://download.example/results",
            console_url="https://console.aws.amazon.com/s3/object/results",
        )
        s3_tracker = MockResultsTracker(s3_response, results_exist=True, status=status)
        monkeypatch.setattr(results_module, "TrackerService", lambda: s3_tracker)

        s3_result = cli_runner.invoke(results, [str(_RUN_ID), "--s3"], input="n\n")

        assert s3_result.exit_code == 1
        assert (warning in s3_result.output) is warns
        if warns:
            assert s3_result.output.index(warning) < s3_result.output.index("Overwrite")

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
        assert "Download (expires in 1 day):" in result.output
        assert "https://download.example/results" in result.output
        assert "https://console.aws.amazon.com/s3/object/results" in result.output
        assert tracker.retrieve_calls == [(_RUN_ID, True, None, False)]

        managed_tracker = MockResultsTracker(response.model_copy(update={"expires_in": 3600}))
        monkeypatch.setattr(results_module, "TrackerService", lambda: managed_tracker)

        managed_result = cli_runner.invoke(results, [str(_RUN_ID), "--s3"])

        assert managed_result.exit_code == 0, managed_result.output
        assert "Download (expires in 1 hour):" in managed_result.output

        existing_tracker = MockResultsTracker(response, results_exist=True)
        monkeypatch.setattr(results_module, "TrackerService", lambda: existing_tracker)

        declined_result = cli_runner.invoke(results, [str(_RUN_ID), "--s3"], input="n\n")

        assert declined_result.exit_code == 1
        assert "Overwrite" in declined_result.output
        assert existing_tracker.retrieve_calls == []

        preview_tracker = MockResultsTracker(response, results_exist=True)
        monkeypatch.setattr(results_module, "TrackerService", lambda: preview_tracker)

        preview_result = cli_runner.invoke(
            results,
            [str(_RUN_ID), "--preview", "--task-ids", "task-a,task-b"],
        )

        assert preview_result.exit_code == 0, preview_result.output
        assert "Overwrite" not in preview_result.output
        assert preview_tracker.retrieve_calls == [(_RUN_ID, True, ["task-a", "task-b"], True)]

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
