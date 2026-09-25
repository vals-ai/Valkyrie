"""Task alignment, safe output, and comparison command behavior.

Run: uv run pytest tests/unit/cli/run/test_compare.py
"""

import json
from importlib import import_module
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from click.testing import CliRunner
from tracker.database.models import BenchmarkStatus

from tests.unit.cli.factories import make_final_view
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.compare import compare, compare_tasks

_MODULE = import_module("valkyrie.cli.run.compare")
_BASELINE = UUID("11111111-1111-4111-8111-111111111111")
_CANDIDATE = UUID("22222222-2222-4222-8222-222222222222")


class TestCompare:
    """Compare results without treating unavailable metrics as losses."""

    def test_aligns_tasks_and_excludes_errors(self) -> None:
        baseline = make_final_view(
            _BASELINE,
            evaluation_results={
                "gain": {"score": 0},
                "loss": {"score": 1},
                "same": {"score": 0.5},
                "absent": {"score": 1},
                "error": {"score": 1},
            },
        )
        candidate = make_final_view(
            _CANDIDATE,
            evaluation_results={
                "gain": {"score": 1},
                "loss": {"score": 0},
                "same": {"score": 0.5},
                "new": {"score": 1},
                "error": {"score": 1},
            },
            task_errors={"error": "private error details"},
        )

        rows = {row.task_id: row for row in compare_tasks(baseline, candidate, "score", False)}

        assert rows["gain"].outcome == "improved"
        assert rows["loss"].outcome == "regressed"
        assert rows["same"].outcome == "unchanged"
        assert rows["absent"].candidate_state == "missing"
        assert rows["new"].baseline_state == "missing"
        assert rows["error"].candidate_state == "error"
        assert rows["error"].delta is None

    @pytest.mark.parametrize("value", [None, "1", float("nan"), float("inf"), {}, 10**400])
    def test_invalid_metric_is_unscored(self, value: object) -> None:
        baseline = make_final_view(_BASELINE, evaluation_results={"task": {"score": 1}})
        candidate = make_final_view(_CANDIDATE, evaluation_results={"task": {"score": value}})

        row = compare_tasks(baseline, candidate, "score", False)[0]

        assert row.outcome == "not comparable"
        assert row.candidate_state == "unscored"
        assert row.delta is None

    @pytest.mark.parametrize(
        ("before", "after", "delta", "outcome"),
        [
            (4, 2, -2, "improved"),
            (-1e308, 1e308, None, "not comparable"),
        ],
    )
    def test_nested_metric_and_lower_is_better(
        self, before: float, after: float, delta: float | None, outcome: str
    ) -> None:
        baseline = make_final_view(_BASELINE, evaluation_results={"task": {"metrics": {"cost": before}}})
        candidate = make_final_view(_CANDIDATE, evaluation_results={"task": {"metrics": {"cost": after}}})

        row = compare_tasks(baseline, candidate, "metrics.cost", True)[0]

        assert row.outcome == outcome
        assert row.delta == delta

    @pytest.mark.parametrize("output_format", ["text", "json"])
    def test_command_reports_changes_without_private_data(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, output_format: str
    ) -> None:
        baseline = make_final_view(
            _BASELINE, evaluation_results={"gain": {"score": 0}, "loss": {"score": 1}, "same": {"score": 1}}
        )
        candidate = make_final_view(
            _CANDIDATE, evaluation_results={"gain": {"score": 1}, "loss": {"score": 0}, "same": {"score": 1}}
        )
        tracker = MagicMock()
        tracker.__enter__.return_value = tracker
        tracker.retrieve_results.side_effect = [baseline, candidate]
        monkeypatch.setattr(_MODULE, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(
            compare, [str(_BASELINE), str(_CANDIDATE), "--format", output_format, "--limit", "1"]
        )

        assert result.exit_code == 0, result.output
        assert "excluded-" not in result.output
        assert "\x1b" not in result.output
        assert "partial snapshot" in result.output
        if output_format == "json":
            payload = json.loads(result.output)
            assert payload["counts"]["improved"] == 1
            assert payload["matched_tasks"] == 3
            assert payload["mean_delta"] == 0
            assert len(payload["tasks"]) == 3
        else:
            assert "loss" in result.output
            assert "gain" not in result.output
            assert "same" not in result.output
            assert "Showing 1/2" in result.output

    @pytest.mark.parametrize("width", [24, 40, 80])
    def test_text_wraps_and_escapes_controls(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, width: int
    ) -> None:
        baseline = make_final_view(_BASELINE, evaluation_results={"task\x1b[2J" + "x" * 100: {"score": 0}})
        tracker = MagicMock()
        tracker.__enter__.return_value = tracker
        tracker.retrieve_results.return_value = baseline
        monkeypatch.setattr(_MODULE, "TrackerService", lambda: tracker)
        monkeypatch.setenv("COLUMNS", str(width))

        result = cli_runner.invoke(compare, [str(_BASELINE), str(_CANDIDATE), "--show-unchanged"])

        assert result.exit_code == 0, result.output
        assert "\\x1b" in result.output
        assert "\x1b" not in result.output
        assert max(map(len, result.output.splitlines())) <= width

    @pytest.mark.parametrize("output_format", ["text", "json"])
    @pytest.mark.parametrize("empty", [True, False])
    def test_no_matched_scores(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, output_format: str, empty: bool
    ) -> None:
        baseline = make_final_view(_BASELINE)
        candidate = make_final_view(_CANDIDATE)
        baseline.evaluation_results = {} if empty else {"old": {"score": 1}}
        candidate.evaluation_results = {} if empty else {"new": {"score": 1}}
        tracker = MagicMock()
        tracker.__enter__.return_value = tracker
        tracker.retrieve_results.side_effect = [baseline, candidate]
        monkeypatch.setattr(_MODULE, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(compare, [str(_BASELINE), str(_CANDIDATE), "--format", output_format])

        assert result.exit_code == 0, result.output
        if output_format == "json":
            payload = json.loads(result.output)
            assert payload["matched_tasks"] == 0
            assert payload["mean_delta"] is None
            assert payload["counts"]["not comparable"] == (0 if empty else 2)
        else:
            assert "No shared tasks" in result.output

    @pytest.mark.parametrize("mismatch", ["benchmark", "dataset", "network"])
    def test_rejects_incompatible_or_unavailable_runs(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, mismatch: str
    ) -> None:
        baseline = make_final_view(_BASELINE, status=BenchmarkStatus.FINISHED)
        candidate = baseline.model_copy(deep=True)
        if mismatch == "benchmark":
            candidate.benchmark_name = "other"
        elif mismatch == "dataset":
            candidate.benchmark_arguments.dataset = "other"
        tracker = MagicMock()
        tracker.__enter__.return_value = tracker
        tracker.retrieve_results.side_effect = (
            TrackerServiceError("unavailable") if mismatch == "network" else [baseline, candidate]
        )
        monkeypatch.setattr(_MODULE, "TrackerService", lambda: tracker)

        result = cli_runner.invoke(compare, [str(_BASELINE), str(_CANDIDATE)])

        assert result.exit_code == 1
        assert (
            "unavailable" in result.output if mismatch == "network" else "same benchmark and dataset" in result.output
        )
