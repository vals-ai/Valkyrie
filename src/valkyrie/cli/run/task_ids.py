"""Task ID parsing helpers for run commands."""

import urllib.request
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlparse

import click


def _clean_task_ids(task_ids: Iterable[str]) -> list[str]:
    cleaned = [task_id.strip() for task_id in task_ids]
    return list(dict.fromkeys(task_id for task_id in cleaned if task_id))


def read_task_ids_source(source: str) -> list[str]:
    """Read task IDs, one per line, from a local path or an http(s) URL."""
    try:
        if urlparse(source).scheme in {"http", "https"}:
            with urllib.request.urlopen(source) as response:
                content = response.read().decode("utf-8")
        else:
            content = Path(source).read_text(encoding="utf-8")
    except OSError as exc:
        raise click.ClickException(f"Failed to read task ids from {source}: {exc}") from exc

    task_ids = _clean_task_ids(content.splitlines())
    if not task_ids:
        raise click.ClickException(f"No task ids found in {source}")
    return task_ids


def resolve_task_ids(task_ids: str | None = None, task_ids_file: str | None = None) -> list[str] | None:
    if task_ids is not None and task_ids_file is not None:
        raise click.UsageError("--task-ids and --task-ids-file are mutually exclusive")
    if task_ids_file is not None:
        return read_task_ids_source(task_ids_file)
    if task_ids is not None:
        resolved_task_ids = _clean_task_ids(task_ids.split(","))
        if not resolved_task_ids:
            raise click.UsageError("No task ids provided")
        return resolved_task_ids
    return None
