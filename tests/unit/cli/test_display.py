"""Tests for shared CLI display helpers.

Run: uv run pytest tests/unit/cli/test_display.py
"""

import click
import pytest
from valkyrie.sdk.models import FailureCategory

from valkyrie.cli.display import group_task_errors, paginate_cli_pages


@pytest.mark.parametrize(
    ("task_errors", "categories", "groups"),
    [
        (
            {"task-c": "same", "task-b": "different\x1b", "task-a": "same", "task-d": "different\\x1b"},
            None,
            [
                (None, "same", ("task-a", "task-c")),
                (None, "different\x1b", ("task-b",)),
                (None, "different\\x1b", ("task-d",)),
            ],
        ),
        (
            {"task-a": "same", "task-b": "same", "task-c": "same"},
            {"task-a": FailureCategory.INFRASTRUCTURE, "task-b": FailureCategory.INFRASTRUCTURE},
            [(FailureCategory.INFRASTRUCTURE, "same", ("task-a", "task-b")), (None, "same", ("task-c",))],
        ),
    ],
    ids=["raw-messages-largest-first", "split-by-category"],
)
def test_group_task_errors(
    task_errors: dict[str, str],
    categories: dict[str, FailureCategory] | None,
    groups: list[tuple[FailureCategory | None, str, tuple[str, ...]]],
) -> None:
    assert group_task_errors(task_errors, categories) == groups


def test_paginate_cli_pages_clears_between_pages_and_handles_filtered_totals(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Shared table pagination should clear each render and recover when totals shrink.

    Test cases:
    - Moving forward clears the previous table before rendering the next page.
    - If a filtered/refetched total invalidates the current page, pagination reloads a valid page.
    """
    load_offsets: list[int] = []
    rendered_pages: list[tuple[list[str], int, int, int]] = []
    page_responses = [
        (8, ["one", "two"]),
        (8, ["three", "four"]),
        (8, ["five", "six"]),
        (4, []),
        (1, []),
        (1, ["filtered"]),
    ]
    keys = iter(["l", "l", "l"])

    def load_page(offset: int, _limit: int) -> tuple[int, list[str]]:
        response = page_responses[len(load_offsets)]
        load_offsets.append(offset)

        return response

    def render_page(page: list[str], current_page: int, total_pages: int, total_count: int) -> None:
        rendered_pages.append((page, current_page, total_pages, total_count))
        print(f"page={current_page}/{total_pages}:{','.join(page)}")

    monkeypatch.setattr("valkyrie.cli.display.click.getchar", lambda: next(keys))

    paginate_cli_pages(
        load_page,
        render_page,
        limit=2,
        render_empty=lambda: print("empty"),
    )

    output = capsys.readouterr().out
    assert load_offsets == [0, 2, 4, 6, 2, 0]
    assert rendered_pages == [
        (["one", "two"], 1, 4, 8),
        (["three", "four"], 2, 4, 8),
        (["five", "six"], 3, 4, 8),
        (["filtered"], 1, 1, 1),
    ]
    assert output.count("\033[2J\033[3J\033[1;1H") == 4
    assert "page=2/1" not in output
    assert "empty" not in output


def test_paginate_cli_pages_without_a_terminal_fails_with_a_click_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_terminal() -> str:
        raise OSError(6, "Device not configured", "/dev/tty")

    monkeypatch.setattr("valkyrie.cli.display.click.getchar", no_terminal)

    with pytest.raises(click.ClickException, match="Device not configured"):
        paginate_cli_pages(lambda _offset, _limit: (3, ["row"]), lambda *_: None, limit=1, render_empty=lambda: None)
