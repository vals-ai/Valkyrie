"""Shared CLI display helpers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, TypeVar

import click

if TYPE_CHECKING:
    from valkyrie.sdk.models import FailureCategory

Page = TypeVar("Page")


def terminal_safe(value: str, *, preserve_newlines: bool) -> str:
    """Escape terminal control characters in user-controlled text."""
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


def group_task_errors(
    task_errors: Mapping[str, str],
    task_failure_categories: Mapping[str, FailureCategory] | None = None,
) -> list[tuple[FailureCategory | None, str, tuple[str, ...]]]:
    """Group task IDs that share a category and raw message, as `(category, message, task_ids)`.

    Task IDs are sorted within each group; groups come largest first, ties broken by first task ID.
    """
    categories = task_failure_categories or {}
    grouped: defaultdict[tuple[FailureCategory | None, str], list[str]] = defaultdict(list)
    for task_id, message in task_errors.items():
        grouped[(categories.get(task_id), message)].append(task_id)

    groups = [(category, message, tuple(sorted(task_ids))) for (category, message), task_ids in grouped.items()]
    return sorted(groups, key=lambda group: (-len(group[2]), group[2][0]))


def utc_isoformat(value: datetime | None = None) -> str:
    """Format a datetime (default: now) as a UTC ISO-8601 string ending in Z."""
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def progress_percent(finished_tasks: int, total_tasks: int) -> float:
    return finished_tasks / total_tasks * 100 if total_tasks > 0 else 0


def local_time(dt: datetime) -> str:
    """Convert UTC time to users local time."""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def short_local_time(dt: datetime, include_date: bool = True) -> str:
    """Convert UTC time to a short local time string."""
    fmt = "%m/%d %H:%M" if include_date else "%H:%M"
    return dt.astimezone().strftime(fmt)


def format_table(
    rows: list[dict[str, str]],
    headers: list[str],
    current_page: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
    item_name: str = "item",
) -> None:
    """Render rows as a padded CLI table."""
    if not rows:
        click.echo(click.style(f"No {item_name}s found.", fg="yellow"))
        return

    if total_count is None:
        total_count = len(rows)

    column_widths = {}
    for header in headers:
        max_width = len(header)
        for row in rows:
            value = str(row.get(header, ""))
            clean_value = click.unstyle(value)
            max_width = max(max_width, len(clean_value))
        column_widths[header] = max_width

    header_line = "  ".join(f"{header:<{column_widths[header]}}" for header in headers)
    separator = "─" * len(header_line)

    click.echo()
    click.echo(click.style(header_line, bold=True))
    click.echo(separator)

    for row in rows:
        cells: list[str] = []
        for header in headers:
            value = str(row.get(header, ""))
            clean_value = click.unstyle(value)
            padding = column_widths[header] - len(clean_value)
            cells.append(f"{value}{' ' * padding}")
        click.echo("  ".join(cells))

    click.echo(separator)

    total_text = f"Total: {total_count} {item_name}(s)"
    if total_pages > 1:
        nav_text_raw = f"[h] ← prev  {current_page}/{total_pages}  next → [l]  [q] quit"
        nav_text = click.style(nav_text_raw, fg="bright_black")
        padding = len(separator) - len(total_text) - len(nav_text_raw)
        click.echo(f"{total_text}{' ' * padding}{nav_text}")
    else:
        click.echo(total_text)


def _clear_pager() -> None:
    click.echo("\033[2J\033[3J\033[1;1H", nl=False, color=True)


def paginate_cli_pages(
    load_page: Callable[[int, int], tuple[int, Page]],
    render_page: Callable[[Page, int, int, int], None],
    *,
    limit: int,
    render_empty: Callable[[], None],
) -> None:
    """Page through CLI rows with h/l/q navigation."""
    current_page = 1

    while True:
        offset = (current_page - 1) * limit
        total_count, page = load_page(offset, limit)
        total_pages = max(1, (total_count + limit - 1) // limit)
        while current_page > total_pages:
            current_page = total_pages
            offset = (current_page - 1) * limit
            total_count, page = load_page(offset, limit)
            total_pages = max(1, (total_count + limit - 1) // limit)

        _clear_pager()

        if total_count == 0:
            render_empty()
            break

        render_page(page, current_page, total_pages, total_count)

        if total_pages <= 1:
            break

        try:
            char = click.getchar()
        except OSError as error:
            raise click.ClickException(str(error)) from error

        if char == "l" and current_page < total_pages:
            current_page += 1
        elif char == "h" and current_page > 1:
            current_page -= 1
        elif char == "q" or char == "\x03":
            break
