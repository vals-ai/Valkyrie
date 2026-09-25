"""Compare run snapshots without modifying stored results."""

import json
import math
import shutil
import textwrap
from collections import Counter
from dataclasses import asdict, dataclass
from itertools import zip_longest
from statistics import mean
from uuid import UUID

import click
from tracker.types import FinalViewResponse

from valkyrie.cli.display import terminal_safe
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService


@dataclass(frozen=True)
class TaskComparison:
    """One aligned task, including unavailable scores."""

    task_id: str
    baseline: float | None
    candidate: float | None
    baseline_state: str
    candidate_state: str
    delta: float | None
    outcome: str


def _score(result: dict[str, object], metric: str) -> float | None:
    """Read a finite numeric metric; never coerce strings or missing values to zero."""
    value: object = result
    for key in metric.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    if not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None

    return number if math.isfinite(number) else None


def _task_score(view: FinalViewResponse, task_id: str, metric: str) -> tuple[float | None, str]:
    """Give task errors precedence over any stale evaluation result."""
    if task_id in (view.task_errors or {}):
        return None, "error"

    result = (view.evaluation_results or {}).get(task_id)
    if result is None:
        return None, "missing"

    score = _score(result, metric)

    return score, "scored" if score is not None else "unscored"


def compare_tasks(
    baseline: FinalViewResponse, candidate: FinalViewResponse, metric: str, lower_is_better: bool
) -> list[TaskComparison]:
    """Align observed tasks by ID and keep errors out of numeric comparisons."""
    task_ids = (
        set(baseline.evaluation_results or {})
        | set(candidate.evaluation_results or {})
        | set(baseline.task_errors or {})
        | set(candidate.task_errors or {})
    )
    rows: list[TaskComparison] = []

    for task_id in sorted(task_ids):
        before, before_state = _task_score(baseline, task_id, metric)
        after, after_state = _task_score(candidate, task_id, metric)
        delta = after - before if before is not None and after is not None else None
        if delta is not None and not math.isfinite(delta):
            delta = None

        outcome = "not comparable"
        if delta is not None:
            improvement = -delta if lower_is_better else delta
            if improvement > 0:
                outcome = "improved"
            elif improvement < 0:
                outcome = "regressed"
            else:
                outcome = "unchanged"

        rows.append(TaskComparison(task_id, before, after, before_state, after_state, delta, outcome))

    return rows


def _run_summary(view: FinalViewResponse) -> dict[str, object]:
    """Expose only identity and summary fields, excluding private contract data."""
    contract = view.benchmark_arguments.contract
    score = view.final_evaluation.final_score if view.final_evaluation else None

    return {
        "run_id": str(view.benchmark_id),
        "benchmark": view.benchmark_name,
        "dataset": view.benchmark_arguments.dataset or "default",
        "agent": contract.name,
        "model": contract.model,
        "status": view.status.value,
        "final_score": score if score is not None and math.isfinite(score) else None,
    }


def _number(value: float | None, *, signed: bool = False) -> str:
    """Keep scores compact without assuming a percentage scale."""
    if value is None:
        return "n/a"

    return format(value, "+.6g" if signed else ".6g")


def render_comparison(
    baseline: FinalViewResponse,
    candidate: FinalViewResponse,
    rows: list[TaskComparison],
    metric: str,
    lower_is_better: bool,
    warnings: list[str],
    limit: int,
    show_unchanged: bool,
) -> None:
    """Render aligned run metadata and dense task rows, wrapping on narrow terminals."""
    width = max(16, min(shutil.get_terminal_size().columns, 100))

    def line(value: str, *, fg: str | None = None, bold: bool = False, dim: bool = False) -> None:
        safe = terminal_safe(value, preserve_newlines=False)
        for part in textwrap.wrap(safe, width=width, subsequent_indent="  ") or [""]:
            click.echo(click.style(part, fg=fg, bold=bold or None, dim=dim or None))

    def columns(left: str, right: str) -> None:
        """Wrap metadata independently so columns remain aligned."""
        column_width = (width - 3) // 2
        left_lines = textwrap.wrap(terminal_safe(left, preserve_newlines=False), column_width) or [""]
        right_lines = textwrap.wrap(terminal_safe(right, preserve_newlines=False), column_width) or [""]
        for first, second in zip_longest(left_lines, right_lines, fillvalue=""):
            line(f"{first:<{column_width}}   {second}")

    line(
        f"{baseline.benchmark_name} / {baseline.benchmark_arguments.dataset or 'default'}"
        f" · {metric} ({'lower' if lower_is_better else 'higher'} is better)",
        fg="cyan",
        bold=True,
    )
    columns("BASELINE", "CANDIDATE")
    columns(str(baseline.benchmark_id), str(candidate.benchmark_id))
    columns(baseline.benchmark_arguments.contract.name, candidate.benchmark_arguments.contract.name)
    baseline_model = baseline.benchmark_arguments.contract.model or "model unspecified"
    candidate_model = candidate.benchmark_arguments.contract.model or "model unspecified"
    if baseline_model == candidate_model and baseline.status == candidate.status:
        line(f"Both: {baseline_model} · {baseline.status.value}", dim=True)
    else:
        columns(f"{baseline_model} · {baseline.status.value}", f"{candidate_model} · {candidate.status.value}")

    counts = Counter(row.outcome for row in rows)
    line(
        f"{counts['improved']} improved · {counts['regressed']} regressed · "
        f"{counts['unchanged']} unchanged · {counts['not comparable']} not comparable",
        bold=True,
    )
    paired = [row for row in rows if row.delta is not None]
    if paired:
        before = mean(row.baseline for row in paired if row.baseline is not None)
        after = mean(row.candidate for row in paired if row.candidate is not None)
        mean_delta = mean(row.delta for row in paired if row.delta is not None)
        line(
            f"Matched mean: {_number(before)} → {_number(after)}  "
            f"Δ {_number(mean_delta, signed=True)} · {len(paired)} tasks · Δ = candidate - baseline"
        )
    else:
        line("No shared tasks with numeric scores for this metric.", fg="yellow")

    order = {"regressed": 0, "improved": 1, "not comparable": 2, "unchanged": 3}
    selected = sorted(
        (row for row in rows if show_unchanged or row.outcome != "unchanged"),
        key=lambda row: (order[row.outcome], -abs(row.delta or 0), row.task_id),
    )
    details = [
        (
            terminal_safe(row.task_id, preserve_newlines=False),
            _number(row.baseline) if row.baseline is not None else row.baseline_state,
            _number(row.candidate) if row.candidate is not None else row.candidate_state,
            _number(row.delta, signed=True),
            row.outcome,
        )
        for row in selected[:limit]
    ]
    headers = ("TASK", "BASE", "CAND", "DELTA", "CHANGE")
    column_widths = [len(header) for header in headers]
    for detail in details:
        column_widths = [max(width, len(cell)) for width, cell in zip(column_widths, detail)]
    task_width = width - sum(column_widths[1:]) - 8
    click.echo()
    if task_width >= 16:
        column_widths[0] = min(task_width, column_widths[0])
        line("  ".join(f"{header:<{size}}" for header, size in zip(headers, column_widths)), dim=True)
        line("─" * (sum(column_widths) + 8), dim=True)
    for task_id, before_text, after_text, delta_text, outcome in details:
        color = {"regressed": "red", "improved": "green", "not comparable": "yellow"}.get(outcome)
        if task_width >= 16:
            task_lines = textwrap.wrap(task_id, task_width) or [""]
            cells = (task_lines[0], before_text, after_text, delta_text, outcome)
            line("  ".join(f"{cell:<{size}}" for cell, size in zip(cells, column_widths)), fg=color)
            for continuation in task_lines[1:]:
                line(continuation, dim=True)
        else:
            line(f"{outcome}: {task_id}", fg=color)
            line(f"  {before_text} → {after_text}  Δ {delta_text}")
    if not selected:
        line("No task changes to show.", dim=True)
    if len(selected) > limit:
        line(f"Showing {limit}/{len(selected)} tasks. Use --limit {len(selected)} or --format json for all.", dim=True)
    for warning in warnings:
        line(f"Note: {warning}", dim=True)


@click.command(help="Compare BASELINE and CANDIDATE runs by task ID. Reads snapshots without saving results.")
@click.argument("baseline", type=UUID)
@click.argument("candidate", type=UUID)
@click.option(
    "--metric", default="score", show_default=True, help="Numeric task field, optionally dotted (e.g. metrics.reward)."
)
@click.option("--lower-is-better", is_flag=True, help="Treat smaller metric values as improvements.")
@click.option(
    "--limit", type=click.IntRange(min=1), default=20, show_default=True, help="Maximum tasks shown in text output."
)
@click.option("--show-unchanged", is_flag=True, help="Include unchanged tasks in text output.")
@click.option("--format", "output_format", type=click.Choice(["text", "json"], case_sensitive=False), default="text")
def compare(
    baseline: UUID,
    candidate: UUID,
    metric: str,
    lower_is_better: bool,
    limit: int,
    show_unchanged: bool,
    output_format: str,
) -> None:
    """Compare compatible run snapshots and report incomplete coverage explicitly."""
    if not metric or any(not key for key in metric.split(".")):
        raise click.BadParameter("Use a field name or dotted path.", param_hint="--metric")

    try:
        with TrackerService() as tracker:
            before = tracker.retrieve_results(baseline, False)
            after = tracker.retrieve_results(candidate, False)
    except TrackerServiceError as error:
        raise click.ClickException(str(error)) from error

    if not isinstance(before, FinalViewResponse) or not isinstance(after, FinalViewResponse):
        raise click.ClickException("Tracker did not return inline results.")
    if (before.benchmark_name, before.benchmark_arguments.dataset or "default") != (
        after.benchmark_name,
        after.benchmark_arguments.dataset or "default",
    ):
        raise click.ClickException("Runs must use the same benchmark and dataset.")

    rows = compare_tasks(before, after, metric, lower_is_better)
    warnings = ["Matched scores only; errors and missing metrics excluded."]
    if any(row.baseline is not None and row.candidate is not None and row.delta is None for row in rows):
        warnings.append("Some metric deltas exceed the numeric range and are excluded.")
    if before.status.value != "FINISHED" or after.status.value != "FINISHED":
        warnings.append("At least one run is not FINISHED. This comparison is a partial snapshot.")
    warnings.append("Benchmark versions unverified; matched means are not aggregate scores.")

    if output_format == "json":
        paired = [row for row in rows if row.delta is not None]
        click.echo(
            json.dumps(
                {
                    "schema_version": 1,
                    "baseline": _run_summary(before),
                    "candidate": _run_summary(after),
                    "metric": metric,
                    "lower_is_better": lower_is_better,
                    "counts": {
                        outcome: sum(row.outcome == outcome for row in rows)
                        for outcome in ("improved", "regressed", "unchanged", "not comparable")
                    },
                    "matched_tasks": len(paired),
                    "mean_delta": mean(row.delta for row in paired if row.delta is not None) if paired else None,
                    "tasks": [asdict(row) for row in rows],
                    "warnings": warnings,
                },
                indent=2,
                allow_nan=False,
            )
        )
    else:
        render_comparison(before, after, rows, metric, lower_is_better, warnings, limit, show_unchanged)
