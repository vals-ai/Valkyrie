"""Fetch and follow provider-neutral benchmark logs."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

import click  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.client import ValkyrieClient  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.errors import ValkyrieSDKError  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.models.logs import LogEvent  # pyright: ignore[reportMissingImports]


@click.command(help="Fetch or follow logs for a run or one task.")
@click.argument("run_id", type=UUID)
@click.option("--task-id", help="Limit logs to one task's current stream; IDs may contain '/'.")
@click.option("--query", help="Match literal text in log messages.")
@click.option("--since", "since_value", help="Inclusive RFC 3339 start time with a timezone offset.")
@click.option("--until", "until_value", help="Inclusive RFC 3339 end time with a timezone offset.")
@click.option("--follow", is_flag=True, help="Follow one task stream until interrupted.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "jsonl"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
def logs(
    run_id: UUID,
    task_id: str | None,
    query: str | None,
    since_value: str | None,
    until_value: str | None,
    follow: bool,
    output_format: str,
) -> None:
    """Fetch logs for a run, or select one task with ``--task-id``."""
    if follow and task_id is None:
        raise click.UsageError("--follow requires --task-id")
    if query is not None and not query.strip():
        raise click.BadParameter("must not be blank", param_hint="--query")

    start_time = _parse_timestamp(since_value, "--since")
    end_time = _parse_timestamp(until_value, "--until")
    if start_time is not None and end_time is not None and end_time <= start_time:
        raise click.UsageError("--until must be later than --since")

    try:
        asyncio.run(
            _run_logs(
                run_id,
                task_id=task_id,
                query=query,
                start_time=start_time,
                end_time=end_time,
                follow=follow,
                output_format=output_format,
            )
        )
    except KeyboardInterrupt:
        return
    except ValkyrieSDKError as error:
        raise click.ClickException(str(error)) from error


async def _run_logs(
    run_id: UUID,
    *,
    task_id: str | None,
    query: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    follow: bool,
    output_format: str,
) -> None:
    client = ValkyrieClient.from_config()
    async with client:
        if follow:
            if task_id is None:
                raise ValkyrieSDKError("follow requires a task ID")
            async for event in client.logs.stream_task(
                run_id,
                task_id,
                query=query,
                start_time=start_time,
                end_time=end_time,
            ):
                _write_event(event, output_format)
            return

        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            if task_id is not None:
                page = await client.logs.page_task(
                    run_id,
                    task_id,
                    query=query,
                    start_time=start_time,
                    end_time=end_time,
                    cursor=cursor,
                )
            else:
                page = await client.logs.page_run(
                    run_id,
                    query=query,
                    start_time=start_time,
                    end_time=end_time,
                    cursor=cursor,
                )
            for event in page.events:
                _write_event(event, output_format)

            cursor = page.next_cursor
            if cursor is None:
                return
            if cursor in seen_cursors:
                raise ValkyrieSDKError("Valkyrie log pagination returned a repeated cursor")
            seen_cursors.add(cursor)


def _parse_timestamp(value: str | None, parameter: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise click.BadParameter("must be an RFC 3339 timestamp", param_hint=parameter) from error
    if parsed.utcoffset() is None:
        raise click.BadParameter("must include a timezone offset", param_hint=parameter)
    return parsed


def _write_event(event: LogEvent, output_format: str) -> None:
    if output_format == "jsonl":
        click.echo(event.model_dump_json())
        return

    timestamp = event.timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    task = f" [{_safe_text(event.task_id)}]" if event.task_id is not None else ""
    click.echo(f"{timestamp}{task} {_safe_text(event.message)}")


def _safe_text(value: str) -> str:
    escaped: list[str] = []
    short_escapes = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}
    for character in value:
        codepoint = ord(character)
        if character in short_escapes:
            escaped.append(short_escapes[character])
        elif codepoint < 32 or 127 <= codepoint <= 159:
            escaped.append(f"\\u{codepoint:04x}")
        else:
            escaped.append(character)
    return "".join(escaped)
