"""Shared CLI display helpers."""

from datetime import datetime

import click


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
