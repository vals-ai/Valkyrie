"""Shows a run's task errors grouped by failure category and message, the way `valk run errors` groups them."""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from rich.console import Group, RenderableType
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import Static

from valkyrie.cli.display import group_task_errors
from valkyrie.sdk.models import BenchmarkTableRow, FailureCategory, TaskStatus, TaskSummary
from valkyrie.tui.browse import BrowseScreen, matches
from valkyrie.tui.source import Environment
from valkyrie.tui.theme import palette
from valkyrie.tui.widgets import CATEGORY_LABEL, CommandLine, KeyBar, Table, TopBar, short_id, shown

MESSAGE_WIDTH = 72

ErrorGroup = tuple[FailureCategory | None, str, tuple[str, ...]]


def failed_tasks(tasks: Iterable[TaskSummary]) -> list[TaskSummary]:
    """Return the tasks `valk run errors` lists: errored ones, and any other task with an error message."""
    return [task for task in tasks if task.status == TaskStatus.ERROR or task.error_message]


class ErrorsScreen(BrowseScreen[str]):
    """Lists a run's error groups, largest first, with the highlighted group's message and task IDs beside them.

    Enter dismisses the screen with the group's first task, which the run view then highlights.
    """

    KEYS: ClassVar[list[tuple[str, str]]] = [
        ("j k ↑ ↓", "move"),
        ("g G", "top, bottom"),
        ("enter", "show the group's first task in the run"),
        ("/", "filter by failure category, error message or task id"),
        ("esc", "clear the filter, or go back to the run"),
        ("y", "copy the command on the bottom line"),
        ("t", "light or dark theme"),
        ("?", "these keys"),
        ("q", "quit"),
    ]
    CAN_GO_BACK = True

    def __init__(self, environment: Environment, run: BenchmarkTableRow, failed: list[TaskSummary]) -> None:
        super().__init__()
        self.environment = environment
        self.run = run
        self.groups = group_task_errors(
            {task.task_id: task.error_message or "" for task in failed},
            {task.task_id: task.failure_category for task in failed if task.failure_category},
        )
        self.task_count = len(failed)

    def compose(self) -> ComposeResult:
        yield TopBar(self.environment)
        with Horizontal(id="body"):
            yield Table(id="error-groups")
            with VerticalScroll(id="error-pane", classes="panel"):
                yield Static(id="error-detail")
        yield from self.footer()

    def on_mount(self) -> None:
        self.query_one(TopBar).trail = (
            ("› ", "faint"),
            (short_id(self.run.id), "muted"),
            ("  ", "muted"),
            (shown(self.run.name), "text"),
            (" › errors", "muted"),
        )
        table = self.query_one(Table)
        table.add_column("tasks", key="count")
        table.add_column("category", key="category")
        table.add_column("message", key="message")
        self.query_one("#error-pane").border_title = "Message"
        self.query_one(KeyBar).show(
            ("esc", "back"), ("enter", "show task"), ("/", "filter"), ("y", "copy cmd"), ("?", "keys")
        )
        self.query_one(CommandLine).show(f"valk run errors {self.run.id}")
        self.refilter()

    def refilter(self) -> None:
        table = self.query_one(Table)
        shown = {str(index): _group_cells(group) for index, group in enumerate(self.groups) if self._matches(group)}
        table.show_rows(shown)
        count = f"{len(shown)}/{len(self.groups)}" if self.filter else str(len(self.groups))
        table.border_title = f"Errors · {self.task_count} tasks · {count} messages{self.filter_label()}"
        self._show_group()

    def restyle(self) -> None:
        super().restyle()
        self._show_group()

    def on_data_table_row_highlighted(self, _event: Table.RowHighlighted) -> None:
        self._show_group()

    def on_data_table_row_selected(self, _event: Table.RowSelected) -> None:
        if (key := self.query_one(Table).highlighted_key()) is not None:
            self.dismiss(self.groups[int(key)][2][0])

    def _matches(self, group: ErrorGroup) -> bool:
        category, message, task_ids = group
        return matches(self.filter, category.value if category else None, message, *task_ids)

    def _show_group(self) -> None:
        key = self.query_one(Table).highlighted_key()
        detail = _group_detail(self.groups[int(key)]) if key is not None else Text()
        self.query_one("#error-detail", Static).update(detail)


def _group_cells(group: ErrorGroup) -> list[Text]:
    category, message, task_ids = group
    first_line = shown(message).strip().split("\n", 1)[0] or "(empty error message)"
    if len(first_line) > MESSAGE_WIDTH:
        first_line = first_line[: MESSAGE_WIDTH - 1] + "…"
    return [
        Text(str(len(task_ids)), style=f"bold {palette().text}", justify="right"),
        Text(CATEGORY_LABEL[category] if category else "", style=palette().error),
        Text(first_line, style=palette().text),
    ]


def _group_detail(group: ErrorGroup) -> RenderableType:
    category, message, task_ids = group
    heading = Text(f"{len(task_ids)} {'task' if len(task_ids) == 1 else 'tasks'}", style=f"bold {palette().text}")
    if category:
        heading.append(f" · {category.value}", style=palette().error)
    return Group(
        heading,
        Text(),
        Text(shown(message) or "(empty error message)", style=palette().error),
        Text(),
        *(Text(shown(task_id), style=palette().muted) for task_id in task_ids),
    )
