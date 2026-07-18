"""Tests for run analysis command behavior.

Run: uv run pytest tests/unit/cli/run/test_analyze.py
"""

from importlib import import_module
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.database.models import AgentContractRequest, BenchmarkArguments
from tracker.exceptions import S3Error
from tracker.types import FetchBenchmarkMetadataResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.analyze import analyze

analyze_module = import_module("valkyrie.cli.run.analyze")

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


def _metadata() -> FetchBenchmarkMetadataResponse:
    return FetchBenchmarkMetadataResponse(
        benchmark_id=_RUN_ID,
        benchmark_name="swebench",
        benchmark_arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="analysis-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )


def _tracker() -> MagicMock:
    tracker = MagicMock()
    tracker.__enter__.return_value = tracker
    tracker.fetch_benchmark_metadata.return_value = _metadata()

    return tracker


class TestAnalyzeCommand:
    """Analysis event rendering and actionable failure behavior."""

    def test_analysis_stream_reports_started_and_completed_plan(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """A successful analysis stream must surface progress and the final reading plan.

        Test cases:
        - The current pushed contract selects the analyzer Lambda.
        - Started, heartbeat, and done events render without exposing raw SSE.
        - The no-cache option reaches the tracker request.
        """
        tracker = _tracker()
        events: list[tuple[str, dict[str, str]]] = [
            ("started", {"lambda_function": "docent-ingest"}),
            ("heartbeat", {}),
            ("done", {"reading_plan_url": "https://docent.example/plan"}),
        ]
        tracker.analyze_benchmark.return_value = iter(events)
        mock_ingest_lookup = AsyncMock(return_value="docent-ingest")
        monkeypatch.setattr(analyze_module, "TrackerService", lambda: tracker)
        monkeypatch.setattr(analyze_module, "get_ingest_lambda_from_s3", mock_ingest_lookup)

        result = cli_runner.invoke(analyze, [str(_RUN_ID), "--no-cache"])

        assert result.exit_code == 0, result.output
        assert "Invoking docent-ingest" in result.output
        assert "https://docent.example/plan" in result.output
        assert "event:" not in result.output
        mock_ingest_lookup.assert_awaited_once_with("analysis-agent")
        tracker.analyze_benchmark.assert_called_once_with(
            _RUN_ID,
            no_cache=True,
            lambda_function="docent-ingest",
        )

    @pytest.mark.parametrize(
        ("lookup_result", "lookup_error", "expected_message"),
        [
            (None, None, "has no `ingest_lambda` set"),
            (None, S3Error("missing archive"), "Could not load contract for agent 'analysis-agent'"),
        ],
    )
    def test_missing_analysis_contract_reports_remediation(
        self,
        lookup_result: str | None,
        lookup_error: S3Error | None,
        expected_message: str,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Missing analysis configuration must stop before invoking the tracker analyzer.

        Test cases:
        - A contract without an analyzer directs the user to declare and push one.
        - A missing S3 contract directs the user to push the agent first.
        """
        tracker = _tracker()
        mock_ingest_lookup = AsyncMock(return_value=lookup_result, side_effect=lookup_error)
        monkeypatch.setattr(analyze_module, "TrackerService", lambda: tracker)
        monkeypatch.setattr(analyze_module, "get_ingest_lambda_from_s3", mock_ingest_lookup)

        result = cli_runner.invoke(analyze, [str(_RUN_ID)])

        assert result.exit_code == 1
        assert expected_message in result.output
        tracker.analyze_benchmark.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "expected_message"),
        [
            ("IN_PROGRESS", "is still in progress"),
            ("STOPPING", "is stopping"),
            ("STOPPED", "was stopped before completion"),
            ("ERROR", "errored before completing"),
        ],
    )
    def test_non_finished_runs_report_state_without_internal_error(
        self,
        status: str,
        expected_message: str,
        monkeypatch: pytest.MonkeyPatch,
        cli_runner: CliRunner,
    ) -> None:
        """Non-finished runs must explain why analysis cannot start.

        Test cases:
        - Active, stopping, stopped, and errored states each produce specific guidance.
        - Every state exits nonzero without rendering a traceback.
        """
        tracker = _tracker()
        tracker.analyze_benchmark.side_effect = TrackerServiceError(
            f"Cannot analyze run {_RUN_ID}: status is {status} (must be FINISHED)."
        )
        monkeypatch.setattr(analyze_module, "TrackerService", lambda: tracker)
        monkeypatch.setattr(analyze_module, "get_ingest_lambda_from_s3", AsyncMock(return_value="docent-ingest"))

        result = cli_runner.invoke(analyze, [str(_RUN_ID)])

        assert result.exit_code == 1
        assert expected_message in result.output
        assert "Traceback" not in result.output
