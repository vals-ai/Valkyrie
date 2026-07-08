"""Shared CLI display helpers."""

from collections.abc import Callable
from datetime import datetime
from typing import TypeVar

import click

Page = TypeVar("Page")


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

        char = click.getchar()

        if char == "l" and current_page < total_pages:
            current_page += 1
        elif char == "h" and current_page > 1:
            current_page -= 1
        elif char == "q" or char == "\x03":
            break
