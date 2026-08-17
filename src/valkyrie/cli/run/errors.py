"""Inspect stored run errors without downloading a results file."""

import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

import click
from tracker.types import FailureSummary, FinalViewResponse

from valkyrie.cli.display import terminal_safe
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService

_TASK_ID_PREVIEW_LIMIT = 5
_FAILURE_CATEGORY_LABELS = {
    "valkyrie": "Platform",
    "harness": "Harness",
    "unknown": "Unknown",
}


def _utc_isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _display_error_message(message: str) -> str:
    if message == "":
        return "(empty error message)"
    return terminal_safe(message, preserve_newlines=True)


def _indent_message(message: str) -> str:
    return "\n".join(f"  {line}" for line in message.split("\n"))


def _count_label(count: int, singular: str) -> str:
    return f"{count} {singular if count == 1 else f'{singular}s'}"


def _format_task_id_preview(task_ids: tuple[str, ...]) -> str:
    visible_ids = task_ids[:_TASK_ID_PREVIEW_LIMIT]
    preview = ", ".join(terminal_safe(task_id, preserve_newlines=False) for task_id in visible_ids)
    omitted_count = len(task_ids) - len(visible_ids)
    return f"{preview} (+{omitted_count} more)" if omitted_count else preview


def group_task_errors(task_errors: Mapping[str, str]) -> list[tuple[str, tuple[str, ...]]]:
    """Group task IDs by identical raw messages in deterministic display order."""
    grouped: defaultdict[str, list[str]] = defaultdict(list)
    for task_id, message in task_errors.items():
        grouped[message].append(task_id)

    groups = [(message, tuple(sorted(task_ids))) for message, task_ids in grouped.items()]
    return sorted(groups, key=lambda group: (-len(group[1]), group[1][0]))


def _failure_payload(failure: FailureSummary) -> dict[str, object]:
    """Project a failure onto the public schema-v2 CLI allowlist."""
    return {
        "id": str(failure.id),
        "schema_version": failure.schema_version,
        "category": failure.category.value,
        "benchmark_id": str(failure.benchmark_id),
        "task_row_id": str(failure.task_row_id) if failure.task_row_id is not None else None,
        "task_attempt_id": str(failure.task_attempt_id) if failure.task_attempt_id is not None else None,
        "retry_sequence": failure.retry_sequence,
        "occurred_at": _utc_isoformat(failure.occurred_at),
        "producer": failure.producer,
        "operation": failure.operation,
        "error_type": failure.error_type,
        "message": failure.message,
        "classification_state": failure.classification_state.value,
        "cause_code": failure.cause_code,
        "terminal_effect": failure.terminal_effect.value,
    }


def build_run_errors_payload(
    response: FinalViewResponse,
    *,
    schema_version: Literal[1, 2] = 1,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Build a versioned allowlist containing only run-error diagnostics."""
    task_errors = dict(sorted((response.task_errors or {}).items()))
    payload: dict[str, object] = {
        "schema_version": schema_version,
        "kind": "run_errors",
        "observed_at": _utc_isoformat(observed_at or datetime.now(timezone.utc)),
        "run_id": str(response.benchmark_id),
        "benchmark_name": response.benchmark_name,
        "status": response.status.value,
        "error_message": response.error_message,
        "task_error_count": len(task_errors),
        "task_errors": task_errors,
    }
    if schema_version == 1:
        return payload

    task_failures = {
        task_id: _failure_payload(failure) for task_id, failure in sorted((response.task_failures or {}).items())
    }
    return {
        **payload,
        "run_failure": _failure_payload(response.run_failure) if response.run_failure is not None else None,
        "task_failures": task_failures,
        "recovered_failure_count": response.recovered_failure_count,
        "secondary_failure_count": response.secondary_failure_count,
    }


def format_run_errors_json(
    response: FinalViewResponse,
    *,
    schema_version: Literal[1, 2] = 1,
) -> str:
    """Serialize one compact, machine-readable run-errors document."""
    return json.dumps(
        build_run_errors_payload(response, schema_version=schema_version),
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _failure_label(value: str | None, fallback: str, *, capitalize_words: bool) -> str:
    if value is None:
        return fallback

    safe_value = terminal_safe(value, preserve_newlines=False).replace("_", " ")
    if capitalize_words:
        return " ".join(word[:1].upper() + word[1:] for word in safe_value.split(" "))

    label = safe_value[:1].upper() + safe_value[1:]
    return label.replace("Websocket", "WebSocket")


def _failure_context(failure: FailureSummary) -> str:
    category = _FAILURE_CATEGORY_LABELS[failure.category.value]
    component = _failure_label(failure.producer, "Unknown component", capitalize_words=True)
    operation = _failure_label(failure.operation, "Unknown operation", capitalize_words=False)
    lines = [f"{category} / {component} / {operation}"]

    if failure.cause_code is not None:
        safe_cause = terminal_safe(failure.cause_code, preserve_newlines=False)
        lines.append(f"Cause: {safe_cause}")
    elif failure.classification_state.value == "details_unavailable":
        lines.append("Details unavailable")

    return "\n".join(lines)


def format_run_errors_text(response: FinalViewResponse) -> None:
    """Render stored run and current task errors for a human reader."""
    task_errors = response.task_errors or {}
    task_failures = response.task_failures or {}
    groups = group_task_errors(task_errors)

    click.echo(click.style("Run Errors", bold=True))
    click.echo(f"{'Run ID:':<12}{response.benchmark_id}")
    click.echo(f"{'Benchmark:':<12}{terminal_safe(response.benchmark_name, preserve_newlines=False)}")
    click.echo(f"{'Status:':<12}{response.status.value.replace('_', ' ').title()}")

    if response.error_message is not None:
        click.echo()
        click.echo(click.style("Stored run error", bold=True))
        click.echo(_indent_message(_display_error_message(response.error_message)))
        if response.run_failure is not None:
            click.echo(_indent_message(_failure_context(response.run_failure)))

    if groups:
        click.echo()
        click.echo(
            click.style(
                f"Task errors ({_count_label(len(task_errors), 'task')}, "
                f"{_count_label(len(groups), 'distinct message')})",
                bold=True,
            )
        )

        for message, task_ids in groups:
            click.echo()
            click.echo(f"[{_count_label(len(task_ids), 'task')}] {_format_task_id_preview(task_ids)}")
            click.echo(_indent_message(_display_error_message(message)))

    if task_failures:
        click.echo()
        click.echo(click.style("Task failure provenance", bold=True))
        for task_id, failure in sorted(task_failures.items()):
            safe_task_id = terminal_safe(task_id, preserve_newlines=False)
            click.echo(f"{safe_task_id}:")
            click.echo(_indent_message(_failure_context(failure)))

    if response.recovered_failure_count or response.secondary_failure_count:
        click.echo()
        click.echo(click.style("Historical non-terminal failures", bold=True))
        click.echo(f"  Recovered: {response.recovered_failure_count}")
        click.echo(f"  Secondary: {response.secondary_failure_count}")

    if not groups and response.error_message is None:
        click.echo()
        click.echo("No current error messages recorded.")


@click.command(
    help=(
        "Show stored run and current task error messages.\n\n"
        "Example:\n"
        "valkyrie run errors 123e4567-e89b-12d3-a456-426614174000"
    ),
)
@click.argument("run_id", type=UUID)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--schema-version",
    type=click.IntRange(min=1, max=2),
    default=1,
    show_default=True,
    help="JSON output schema version.",
)
def errors(run_id: UUID, output_format: str, schema_version: Literal[1, 2]) -> None:
    """Show stored run and current task error messages."""
    if output_format != "json" and schema_version != 1:
        raise click.UsageError("--schema-version 2 requires --format json")

    try:
        with TrackerService() as tracker:
            response = tracker.retrieve_results(run_id, s3=False)
        if not isinstance(response, FinalViewResponse):
            raise TrackerServiceError("Tracker returned an unexpected response while fetching run errors")
    except TrackerServiceError as error:
        safe_error = terminal_safe(str(error), preserve_newlines=False)
        raise click.ClickException(safe_error) from error

    if output_format == "json":
        click.echo(format_run_errors_json(response, schema_version=schema_version))
    else:
        format_run_errors_text(response)
