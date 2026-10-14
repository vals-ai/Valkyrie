"""Tests for run result retrieval and file persistence.

Run: uv run pytest tests/unit/cli/run/test_results.py
"""

import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk.models import BenchmarkStatus, S3UploadResultsResponse

from valkyrie.cli.run.results import results

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_final_view, ok

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_S3_RESULTS = S3UploadResultsResponse(
    s3_url="s3://bucket/results.json",
    presigned_url="https://download.example/results",
    console_url="https://console.aws.amazon.com/s3/object/results",
)


class TestResultsCommand:
    """Local files, subset summaries, S3 links, and overwrite protection."""

    def test_local_results_write_complete_payload_and_subset_summary(
        self,
        tmp_path: Path,
        tracker: MockTracker,
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
        tracker.reply("GET", "/retrieve-results", ok(response))
        output_path = tmp_path / "results.json"

        result = cli_runner.invoke(
            results,
            [str(_RUN_ID), "--path", str(output_path), "--task-ids", "task-a,task-b,missing"],
        )

        assert result.exit_code == 0, result.output
        assert "Scored over 2 of 3 subset task ids" in result.output
        (request,) = tracker.requests
        assert request.url.params["s3"] == "false"
        assert request.url.params.get_list("task_ids") == ["task-a", "task-b", "missing"]

        saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
        assert saved_payload["benchmark_id"] == str(_RUN_ID)
        assert saved_payload["evaluation_results"] == {"task-a": {"score": 1}}
        assert saved_payload["task_errors"] == {"task-b": "evaluation failed"}
        assert "secrets" not in saved_payload["benchmark_arguments"]["contract"]
        assert "kwargs" not in saved_payload["benchmark_arguments"]["contract"]

    @pytest.mark.parametrize(
        ("arguments", "results_exist", "answer", "requested_path", "expires"),
        [
            (["--s3"], False, None, "/retrieve-results", "1 day"),
            (["--s3"], True, "y\n", "/retrieve-results", "1 day"),
            (["--s3"], True, "n\n", None, None),
            (["--preview", "--task-ids", "task-a,task-b"], True, None, "/preview-results", "1 hour"),
        ],
        ids=["new-upload", "confirmed-overwrite", "declined-overwrite", "preview-skips-the-prompt"],
    )
    def test_s3_results_render_links_and_protect_existing_uploads(
        self,
        tracker: MockTracker,
        cli_runner: CliRunner,
        arguments: list[str],
        results_exist: bool,
        answer: str | None,
        requested_path: str | None,
        expires: str | None,
    ) -> None:
        """S3 uploads print usable links, and only an explicit yes replaces results already in S3."""
        tracker.reply("GET", "/check-results-exist", httpx.Response(200, json={"exists": results_exist}))
        tracker.reply("GET", "/retrieve-results", ok(_S3_RESULTS))
        tracker.reply("GET", "/preview-results", ok(_S3_RESULTS.model_copy(update={"expires_in": 3600})))

        result = cli_runner.invoke(results, [str(_RUN_ID), *arguments], input=answer)

        uploads = [request for request in tracker.requests if request.url.path != "/check-results-exist"]
        if requested_path is None:
            assert result.exit_code == 1
            assert uploads == []
            return
        assert result.exit_code == 0, result.output
        assert [request.url.path for request in uploads] == [requested_path]
        assert f"Download (expires in {expires}):" in result.output
        assert "https://download.example/results" in result.output
        assert "https://console.aws.amazon.com/s3/object/results" in result.output

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
        tracker: MockTracker,
        cli_runner: CliRunner,
    ) -> None:
        """Result files must not be created in missing directories or overwrite without consent.

        Test cases:
        - A missing parent directory returns an actionable error.
        - Declining overwrite preserves the existing file contents.
        """
        tracker.reply("GET", "/retrieve-results", ok(make_final_view(_RUN_ID)))
        output_path = tmp_path / path
        if output_path.name == "existing.json":
            output_path.write_text("original", encoding="utf-8")

        result = cli_runner.invoke(
            results,
            [str(_RUN_ID), "--path", str(output_path)],
            input="n\n",
        )

        assert result.exit_code == 1
        assert expected_message in result.output
        if output_path.exists():
            assert output_path.read_text(encoding="utf-8") == "original"
