"""Inspect stored run errors without downloading a results file."""

import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from uuid import UUID

import click
from tracker.types import FinalViewResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService

_TASK_ID_PREVIEW_LIMIT = 5


def _utc_isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _terminal_safe(value: str, *, preserve_newlines: bool) -> str:
    """Escape controls in untrusted text before writing it to a terminal."""
    escaped: list[str] = []
    for character in value:
        if character == "\n" and preserve_newlines:
            escaped.append(character)
        elif character == "\t":
            escaped.append("    ")
        elif character.isprintable():
            escaped.append(character)
        else:
            escaped.append(character.encode("unicode_escape").decode("ascii"))
    return "".join(escaped)


def _display_error_message(message: str) -> str:
    if message == "":
        return "(empty error message)"
    return _terminal_safe(message, preserve_newlines=True)


def _indent_message(message: str) -> str:
    return "\n".join(f"  {line}" for line in message.split("\n"))


def _count_label(count: int, singular: str) -> str:
    return f"{count} {singular if count == 1 else f'{singular}s'}"


def _format_task_id_preview(task_ids: tuple[str, ...]) -> str:
    visible_ids = task_ids[:_TASK_ID_PREVIEW_LIMIT]
    preview = ", ".join(_terminal_safe(task_id, preserve_newlines=False) for task_id in visible_ids)
    omitted_count = len(task_ids) - len(visible_ids)
    return f"{preview} (+{omitted_count} more)" if omitted_count else preview


def group_task_errors(task_errors: Mapping[str, str]) -> list[tuple[str, tuple[str, ...]]]:
    """Group task IDs by identical raw messages in deterministic display order."""
    grouped: defaultdict[str, list[str]] = defaultdict(list)
    for task_id, message in task_errors.items():
        grouped[message].append(task_id)

    groups = [(message, tuple(sorted(task_ids))) for message, task_ids in grouped.items()]
    return sorted(groups, key=lambda group: (-len(group[1]), group[1][0]))


def build_run_errors_payload(
    response: FinalViewResponse,
    *,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Build a versioned allowlist containing only run-error diagnostics."""
    task_errors = dict(sorted((response.task_errors or {}).items()))
    return {
        "schema_version": 1,
        "kind": "run_errors",
        "observed_at": _utc_isoformat(observed_at or datetime.now(timezone.utc)),
        "run_id": str(response.benchmark_id),
        "benchmark_name": response.benchmark_name,
        "status": response.status.value,
        "error_message": response.error_message,
        "task_error_count": len(task_errors),
        "task_errors": task_errors,
    }


def format_run_errors_json(response: FinalViewResponse) -> str:
    """Serialize one compact, machine-readable run-errors document."""
    return json.dumps(
        build_run_errors_payload(response),
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def format_run_errors_text(response: FinalViewResponse) -> None:
    """Render stored run and current task errors for a human reader."""
    task_errors = response.task_errors or {}
    groups = group_task_errors(task_errors)

    click.echo(click.style("Run Errors", bold=True))
    click.echo(f"{'Run ID:':<12}{response.benchmark_id}")
    click.echo(f"{'Benchmark:':<12}{_terminal_safe(response.benchmark_name, preserve_newlines=False)}")
    click.echo(f"{'Status:':<12}{response.status.value.replace('_', ' ').title()}")

    if response.error_message is not None:
        click.echo()
        click.echo(click.style("Stored run error", bold=True))
        click.echo(_indent_message(_display_error_message(response.error_message)))

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
    elif response.error_message is None:
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
def errors(run_id: UUID, output_format: str) -> None:
    """Show stored run and current task error messages."""
    try:
        with TrackerService() as tracker:
            response = tracker.retrieve_results(run_id, s3=False)
        if not isinstance(response, FinalViewResponse):
            raise TrackerServiceError("Tracker returned an unexpected response while fetching run errors")
    except TrackerServiceError as error:
        safe_error = _terminal_safe(str(error), preserve_newlines=False)
        raise click.ClickException(safe_error) from error

    if output_format == "json":
        click.echo(format_run_errors_json(response))
    else:
        format_run_errors_text(response)
