"""Shows one run, with its progress on top, its tasks (errors first) on the left, and the highlighted task's log and detail on the right."""

from __future__ import annotations

import asyncio
import json
import re
import shlex
import webbrowser
from bisect import bisect_left
from collections import Counter
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urlparse
from uuid import UUID
from typing import TYPE_CHECKING, ClassVar, Self, TypeVar, cast

from rich.ansi import AnsiDecoder
from rich.console import Group, RenderableType
from rich.highlighter import Highlighter
from rich.text import Text
from textual import events, on, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.cache import LRUCache
from textual.containers import Horizontal, VerticalScroll
from textual.markup import escape
from textual.reactive import reactive, var
from textual.strip import Strip
from textual.widget import Widget
from textual.widgets import ContentSwitcher, Input, Log, Static

from valkyrie.sdk.errors import ValkyrieSDKError
from valkyrie.sdk.models import (
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    LogEvent,
    SingleTaskResponse,
    TasksResponse,
    TaskStatus,
    TaskSummary,
)
from valkyrie.tui.theme import palette
from valkyrie.tui.browse import BrowseScreen, matches
from valkyrie.tui.errors import ErrorsScreen, failed_tasks
from valkyrie.tui.source import FINAL_STATUSES, TRIAGE_ORDER, RunSource
from valkyrie.tui.widgets import (
    CATEGORY_LABEL,
    CommandLine,
    KeyBar,
    Table,
    TopBar,
    ago,
    progress_bar,
    run_glyph,
    short_id,
    shown,
    status_label,
    task_glyph,
)

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp

T = TypeVar("T")

ACTIVE_TASKS = {TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING}
TABS = ("logs", "detail")
TASK_REFRESH_SECONDS = 2
RECONNECT_SECONDS = 3
LOG_MAX_LINES = 50_000
ERROR_PREVIEW_LINES = 2
CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
# A task loads once the cursor has rested this long, so moving through tasks loads only the ones stopped on. Each log
# costs at least two CloudWatch reads, and those share one per-account rate limit. Cached tasks show at once.
MOVE_DELAY_SECONDS = 0.25
# Log lines can land a few seconds after a task finishes, so a finished task's log is cached once it's been quiet.
SETTLED_AFTER = timedelta(minutes=1)


@dataclass
class RunCache:
    """Holds each run's last task list and finished tasks' logs and detail, kept on the app so reopening them is instant."""

    tasks: dict[UUID, TasksResponse] = field(default_factory=dict)
    # Keyed by run, task and start time, since a retry reuses the task's ID and moves its start time.
    logs: LRUCache[tuple[UUID, str, datetime], list[str]] = field(default_factory=lambda: LRUCache(50))
    details: LRUCache[tuple[UUID, str, datetime], SingleTaskResponse] = field(default_factory=lambda: LRUCache(50))


class RunSummary(Widget):
    """Shows the run's status, a progress bar split by task state, and per-state counts with errors broken down by category."""

    run: reactive[BenchmarkTableRow | None] = reactive(None)
    categories: reactive[tuple[tuple[str, int], ...]] = reactive(())

    def render(self) -> RenderableType:
        run = self.run
        if run is None:
            return Text()
        glyph, color = run_glyph(run.status)
        status = Text(f"{glyph} {status_label(run.status)}", style=f"bold {color}")
        tail = Text()
        if run.total_tasks:
            percent = round(100 * run.finished_tasks / run.total_tasks)
            tail.append(f"  {run.finished_tasks}/{run.total_tasks}  {percent}%", style=palette().muted)
        if run.final_score is not None:
            tail.append(f"   score {run.final_score:.3f}", style=palette().text)
        width = max(self.content_size.width - status.cell_len - tail.cell_len - 2, 10)
        bar = progress_bar(run.task_state_counts, run.total_tasks, width) if run.total_tasks else Text(" " * width)
        lines = [Text.assemble(status, "  ", bar, tail), self._counts(run)]
        if run.error_message:
            # A run's error can be a whole traceback; the task detail and `e` show the task errors in full.
            message = shown(run.error_message).splitlines()
            lines += [
                Text(line, style=palette().error, no_wrap=True, overflow="ellipsis")
                for line in message[:ERROR_PREVIEW_LINES]
            ]
            if len(message) > ERROR_PREVIEW_LINES:
                hidden = len(message) - ERROR_PREVIEW_LINES
                lines.append(Text(f"… {hidden} more lines", style=palette().muted))
        return Group(*lines)

    def _counts(self, run: BenchmarkTableRow) -> Text:
        line = Text(no_wrap=True, overflow="ellipsis")
        for status in TRIAGE_ORDER:
            count = run.task_state_counts.get(status.value, 0)
            if not count:
                continue
            if line:
                line.append("  ")
            glyph, color = task_glyph(status)
            line.append(f"{glyph} {count} ", style=color)
            line.append(status_label(status), style=palette().muted)
            if status == TaskStatus.ERROR and self.categories:
                breakdown = " · ".join(f"{label} {count}" for label, count in self.categories)
                line.append(f" ({breakdown})", style=palette().error)
        return line


class LogHighlighter(Highlighter):
    """Dims timestamps and agent chatter, colors error lines red, and marks the search term, most strongly on the
    current match's line."""

    def __init__(self) -> None:
        self.term = ""
        self.on_current_line = False

    def highlight(self, text: Text) -> None:
        text.stylize(palette().faint, 0, 8)
        if text.plain[10:].startswith("ERROR"):
            text.stylize(palette().error, 10)
        elif text.plain[10:].startswith("[agent]"):
            text.stylize(palette().muted, 10)
        if self.term:
            colors = palette()
            if self.on_current_line:
                text.highlight_words([self.term], f"bold {colors.background} on {colors.warning}")
            else:
                text.highlight_words([self.term], f"bold {colors.warning} on {colors.panel}")


class TaskLog(Log):
    """Shows the highlighted task's log. Scrolling up pauses following, and `G` jumps to the end and resumes it.

    `/` searches the loaded lines for literal, case-sensitive text, as `valk run logs --query` does, and `n`/`N` step
    through the matching lines. The term stays when another task's log loads.
    """

    BINDINGS = [
        Binding("j", "scroll_down", show=False),
        Binding("k", "scroll_up", show=False),
        Binding("g", "scroll_home", show=False),
        Binding("G", "scroll_end", show=False),
        Binding("slash", "screen.open_search", show=False),
        Binding("n", "next_match(1)", show=False),
        Binding("N", "next_match(-1)", show=False),
    ]

    matches: var[tuple[int, ...]] = var(())
    current: var[int | None] = var(None)

    def __init__(self) -> None:
        super().__init__(highlight=True, max_lines=LOG_MAX_LINES, id="logs")
        self.highlighter = self.marks = LogHighlighter()

    @property
    def search_term(self) -> str:
        return self.marks.term

    def search(self, term: str) -> None:
        """Mark `term` and scroll to its first line from the current match, or from the top of the view."""
        self.marks.term = term
        self.notify_style_update()
        self.refresh()
        self._find_matches()
        self._jump(round(self.scroll_y) if self.current is None else self.current, 0)

    def action_next_match(self, step: int) -> None:
        self._jump(round(self.scroll_y) if self.current is None else self.current, step)

    def render_line(self, y: int) -> Strip:
        # The highlighter sees only the line's text, so it's told whether this is the current match's line.
        self.marks.on_current_line = self.scroll_offset.y + y == self.current
        return super().render_line(y)

    def watch_current(self) -> None:
        self.notify_style_update()
        self.refresh()

    def write_lines(self, lines: Iterable[str], scroll_end: bool | None = None) -> Self:
        super().write_lines(lines, scroll_end)
        self._find_matches()
        return self

    def clear(self) -> Self:
        super().clear()
        self.matches = ()
        self.current = None
        return self

    def _find_matches(self) -> None:
        term = self.search_term
        self.matches = tuple(y for y, line in enumerate(self.lines) if term in line) if term else ()

    def _jump(self, start: int, step: int) -> None:
        """Scroll to the first match at or after `start` (step 0), after it (1) or before it (-1), wrapping around."""
        if not self.matches:
            self.current = None
            return
        if step < 0:
            index = bisect_left(self.matches, start) - 1
        else:
            index = bisect_left(self.matches, start + step) % len(self.matches)
        self.current = self.matches[index]
        region = self.scrollable_content_region
        column = self.lines[self.current].find(self.search_term)
        self.scroll_to(
            x=column - region.width // 3 if column + len(self.search_term) > region.width else 0,
            y=self.current - region.height // 3,
            animate=False,
        )


class SearchInput(Input):
    BINDINGS = [Binding("escape", "screen.clear_search", show=False)]

    def __init__(self) -> None:
        super().__init__(placeholder="type to search the log · enter keeps it · esc clears", id="search")


class RunScreen(BrowseScreen[None]):
    BINDINGS = [
        Binding("left_square_bracket", "switch_tab(-1)", "previous tab"),
        Binding("right_square_bracket", "switch_tab(1)", "next tab"),
        Binding("z", "zoom", "zoom"),
        Binding("e", "errors", "errors"),
        Binding("o", "open_docent", "open docent"),
    ]
    KEYS: ClassVar[list[tuple[str, str]]] = [
        ("j k ↑ ↓", "move through tasks, or scroll the log"),
        ("tab", "switch between the task list and the log"),
        ("[ ]", "logs, detail"),
        ("z", "zoom the log"),
        ("G", "jump to the end of the log and follow it"),
        ("/", "filter by task id, status, failure category or error message, or search the log when it has focus"),
        ("n N", "next or previous search match"),
        ("e", "errors grouped by category and message"),
        ("esc", "clear the search or the filter, or go back to the runs"),
        ("y", "copy the command on the bottom line"),
        ("o", "open the run's Docent reading"),
        ("t", "light or dark theme"),
        ("?", "these keys"),
        ("q", "quit"),
    ]
    CAN_GO_BACK = True

    def __init__(self, source: RunSource, run: BenchmarkTableRow) -> None:
        super().__init__()
        self.source = source
        self.run = run
        self.tasks: dict[str, TaskSummary] = {}
        self.docent_url: str | None = None
        self.tasks_stale = False
        self.shown_summary: TaskSummary | None = None
        self.shown_task: str | None = None
        self.detail_task: SingleTaskResponse | None = None
        self.total_tasks = 0
        self.tab = TABS[0]

    def compose(self) -> ComposeResult:
        yield TopBar(self.source.environment)
        yield RunSummary(id="summary")
        with Horizontal(id="body"):
            yield Table(id="tasks")
            with ContentSwitcher(initial="logs", id="task-pane", classes="panel"):
                yield TaskLog()
                with VerticalScroll(id="detail"):
                    yield Static(id="detail-body")
        with Horizontal(id="search-bar"):
            yield Static("/", id="search-prompt")
            yield SearchInput()
        yield from self.footer()

    def on_mount(self) -> None:
        run = self.run
        model = f" ({shown(run.model)})" if run.model else ""
        self.query_one(TopBar).trail = (
            ("› ", "faint"),
            (short_id(run.id), "muted"),
            ("  ", "muted"),
            (shown(run.name), "text"),
            (f" · {shown(run.agent_name)}{model}", "muted"),
        )
        self.query_one(RunSummary).run = run
        table = self.query_one(Table)
        table.border_title = "Tasks"
        table.add_column("", key="status")
        table.add_column("task", key="task", width=30)
        table.add_column("", key="info")
        self._show_keys()
        self._show_tab()
        log = self.query_one(TaskLog)
        for attribute in ("scroll_y", "matches", "current"):
            self.watch(log, attribute, self._show_log_state)
        if cached := self.cache.tasks.get(run.id):
            self._show_tasks(cached)
        self.watch_run()
        self.load_tasks()
        self.set_interval(TASK_REFRESH_SECONDS, self._refresh_if_stale)

    @property
    def cache(self) -> RunCache:
        return cast("ValkyrieApp", self.app).cache

    @work(exclusive=True, group="stream")
    async def watch_run(self) -> None:
        """Follow the run's state, reconnecting after a pause if the stream ends before the run does."""
        try:
            async for snapshot in self.source.watch_run(self.run.id):
                self._apply_snapshot(snapshot)
        except ValkyrieSDKError as error:
            self.query_one(TopBar).status = ("⚠ run stream lost, reconnecting", "warning")
            self.notify(str(error), title="Tracker error", severity="error")
        if self.run.status not in FINAL_STATUSES:
            self.set_timer(RECONNECT_SECONDS, self.watch_run)

    def _apply_snapshot(self, snapshot: FetchBenchmarkResponse) -> None:
        details = snapshot.details
        counts = {status.value: count for status, count in details.task_breakdown.items()}
        if counts != self.run.task_state_counts:
            self.tasks_stale = True
        self.run = self.run.model_copy(
            update={
                "status": details.status,
                "total_tasks": details.total_tasks,
                "finished_tasks": details.finished_tasks,
                "task_state_counts": counts,
                "final_score": snapshot.final_score,
                "error_message": snapshot.error_message,
            }
        )
        self.docent_url = details.docent_reading_url
        self.query_one(RunSummary).run = self.run
        self.query_one(TopBar).status = ("● live", "accent")

    def _refresh_if_stale(self) -> None:
        if self.tasks_stale:
            self.load_tasks()

    @work(exclusive=True, group="tasks")
    async def load_tasks(self) -> None:
        self.tasks_stale = False
        try:
            page = await self.source.tasks(self.run.id)
        except ValkyrieSDKError as error:
            self.tasks_stale = True
            self.notify(str(error), title="Tracker error", severity="error")
            return
        self.cache.tasks[self.run.id] = page
        self._show_tasks(page)
        self._reload_shown_task()

    def _show_tasks(self, page: TasksResponse) -> None:
        ordered = sorted(page.tasks, key=_triage_key)
        self.tasks = {task.task_id: task for task in ordered}
        self.total_tasks = page.total_count
        errors = Counter(
            CATEGORY_LABEL[task.failure_category]
            for task in ordered
            if task.status == TaskStatus.ERROR and task.failure_category
        )
        self.query_one(RunSummary).categories = tuple(errors.most_common())
        self.refilter()

    def refilter(self) -> None:
        visible = [task for task in self.tasks.values() if self._matches(task)]
        now = cast("ValkyrieApp", self.app).now()
        self.query_one(Table).show_rows({task.task_id: _task_cells(task, now) for task in visible})
        loaded = len(self.tasks)
        count = f"{loaded} of {self.total_tasks}" if self.total_tasks > loaded else str(loaded)
        if self.filter:
            count = f"{len(visible)}/{count}"
        self.query_one(Table).border_title = f"Tasks · {count}{self.filter_label()}"
        if not visible:
            self._clear_task()

    def _clear_task(self) -> None:
        """Empty the task pane when the filter hides every task, so it doesn't describe a task that isn't listed."""
        if self.shown_task is None:
            return
        self.shown_task = None
        self.workers.cancel_group(self, "logs")
        self.workers.cancel_group(self, "detail")
        log = self.query_one(TaskLog)
        log.clear()
        log.border_title = ""
        self.detail_task = None
        self.query_one("#detail-body", Static).update("")
        self._show_log_state()
        self._show_command()

    def _matches(self, task: TaskSummary) -> bool:
        category = task.failure_category.value if task.failure_category else None
        return matches(self.filter, task.task_id, status_label(task.status), category, task.error_message)

    def on_data_table_row_highlighted(self, _event: Table.RowHighlighted) -> None:
        task_id = self.query_one(Table).highlighted_key()
        if task_id is None:
            return
        # A refresh that reorders the table highlights the same task again; only a different task reloads.
        if task_id != self.shown_task:
            delay = 0 if self.shown_task is None else MOVE_DELAY_SECONDS
            self.shown_task = task_id
            self.shown_summary = self.tasks[task_id]
            self.load_logs(task_id, delay)
            self.load_detail(task_id, delay)
        self._show_log_state()
        self._show_command()

    def _reload_shown_task(self) -> None:
        """Reload the shown task's panes when a refresh shows it changed status or started a new attempt.

        A retry is a new attempt with a later start time, so it reloads the log too; otherwise the log reloads only
        when no stream is following it.
        """
        task = self.tasks.get(self.shown_task) if self.shown_task else None
        shown = self.shown_summary
        if task is None or shown is None or (task.status, task.started_at) == (shown.status, shown.started_at):
            return
        if task.started_at != shown.started_at or shown.status not in ACTIVE_TASKS:
            self.load_logs(task.task_id, 0)
        self.load_detail(task.task_id, 0)
        self.shown_summary = task

    @work(exclusive=True, group="logs")
    async def load_logs(self, task_id: str, delay: float) -> None:
        """Follow a running task's log, reconnecting if it drops, or fetch a finished one's, caching it once settled."""
        log = self.query_one(TaskLog)
        log.clear()
        log.border_title = escape(shown(task_id))
        task = self.tasks[task_id]
        key = (self.run.id, task_id, task.started_at)
        lines = None if task.status in ACTIVE_TASKS else self.cache.logs.get(key)
        if lines is None:
            await asyncio.sleep(delay)
        try:
            if task.status in ACTIVE_TASKS:
                async for events in _batched(self.source.stream_logs(self.run.id, task_id)):
                    log.write_lines([log_line(event) for event in events])
            else:
                if lines is None:
                    lines = [log_line(event) for event in await self.source.fetch_logs(self.run.id, task_id)]
                    if self._settled(task):
                        self.cache.logs.set(key, lines)
                log.write_lines(lines)
        except ValkyrieSDKError as error:
            if task.status not in ACTIVE_TASKS:
                log.write_line(f"⚠ couldn't load the log: {shown(str(error))}")
            else:
                log.write_line(f"⚠ log stream lost, reconnecting: {shown(str(error))}")
                self.set_timer(RECONNECT_SECONDS, lambda: self._reconnect_log(task_id))
        if not log.line_count:
            log.write_line("No log lines yet.")

    def _reconnect_log(self, task_id: str) -> None:
        if task_id == self.shown_task:
            self.load_logs(task_id, 0)

    @work(exclusive=True, group="detail")
    async def load_detail(self, task_id: str, delay: float) -> None:
        key = (self.run.id, task_id, self.tasks[task_id].started_at)
        body = self.query_one("#detail-body", Static)
        task = self.cache.details.get(key)
        if task is None:
            await asyncio.sleep(delay)
            try:
                task = await self.source.task(self.run.id, task_id)
            except ValkyrieSDKError as error:
                body.update(Text(shown(str(error)), style=palette().error))
                return
            if self._settled(self.tasks[task_id]):
                self.cache.details.set(key, task)
        self.detail_task = task
        self._show_detail()

    def _show_detail(self) -> None:
        if self.detail_task:
            self.query_one("#detail-body", Static).update(
                _detail(self.detail_task, cast("ValkyrieApp", self.app).now())
            )

    def restyle(self) -> None:
        super().restyle()
        self._show_detail()

    def _settled(self, task: TaskSummary) -> bool:
        now = cast("ValkyrieApp", self.app).now()
        return task.finished_at is not None and now - task.finished_at > SETTLED_AFTER

    def action_switch_tab(self, step: int) -> None:
        self.tab = TABS[(TABS.index(self.tab) + step) % len(TABS)]
        self._show_tab()

    def action_open_search(self) -> None:
        self.add_class("-searching")
        self.query_one(SearchInput).focus()

    def action_clear_search(self) -> None:
        self.query_one(SearchInput).value = ""
        self.query_one(TaskLog).search("")
        self._close_search()

    @on(Input.Changed, "#search")
    def _search_changed(self, event: Input.Changed) -> None:
        self.query_one(TaskLog).search(event.value)
        self._show_command()

    @on(Input.Submitted, "#search")
    def _close_search(self) -> None:
        self.remove_class("-searching")
        self.query_one(TaskLog).focus()
        self._show_command()

    def action_back(self) -> None:
        if self.query_one(TaskLog).search_term:
            self.action_clear_search()
        else:
            super().action_back()

    def on_descendant_focus(self, _event: events.DescendantFocus) -> None:
        self._show_keys()

    def _show_keys(self) -> None:
        if isinstance(self.focused, TaskLog):
            keys = [("/", "search"), ("n N", "matches"), ("G", "follow")]
        else:
            keys = [("/", "filter"), ("e", "errors")]
        self.query_one(KeyBar).show(
            ("esc", "back"), *keys, ("[ ]", "tabs"), ("z", "zoom"), ("y", "copy cmd"), ("?", "keys")
        )

    def action_zoom(self) -> None:
        self.toggle_class("-zoomed")

    def action_errors(self) -> None:
        if failed := failed_tasks(self.tasks.values()):
            self.app.push_screen(ErrorsScreen(self.source.environment, self.run, failed), self.show_task)
        else:
            self.notify("No task errors in this run.")

    def show_task(self, task_id: str | None) -> None:
        """Put the cursor on a task, clearing the filter if it hides the task."""
        table = self.query_one(Table)
        if task_id is None or task_id not in self.tasks:
            return
        if task_id not in table.rows:
            self.action_clear_filter()
            self.filter = ""
            self.refilter()
        table.move_cursor(row=table.get_row_index(task_id))
        table.focus()

    def action_open_docent(self) -> None:
        url = self.docent_url
        if not url:
            self.notify("No Docent reading for this run yet.")
            return
        # The Tracker may be self-hosted, so only a web link with a host is handed to the system to open.
        try:
            parts = urlparse(url)
            web = parts.scheme in ("http", "https") and bool(parts.hostname)
        except ValueError:  # urlparse rejects some malformed URLs, such as `https://[`
            web = False
        if web:
            webbrowser.open(url)
        else:
            self.notify(f"Not opening {url}: only web links open.", severity="warning")

    def _show_tab(self) -> None:
        self.query_one(ContentSwitcher).current = self.tab
        # Both tabs set a color, since the panel's title turns accent when it has focus.
        titles = [
            f"[bold $accent]{tab.title()}[/]" if tab == self.tab else f"[$text-muted]{tab.title()}[/]" for tab in TABS
        ]
        self.query_one(ContentSwitcher).border_title = " [$text-muted]│[/] ".join(titles)
        self._show_log_state()
        self._show_command()

    def _show_log_state(self) -> None:
        log = self.query_one(TaskLog)
        task_id = self.query_one(Table).highlighted_key()
        task = self.tasks.get(task_id) if task_id else None
        if self.tab != "logs" or task is None:
            subtitle = ""
        elif task.status not in ACTIVE_TASKS:
            subtitle = "[$text-muted]end of log[/]"
        elif log.is_vertical_scroll_end:
            subtitle = "[$accent]following[/]"
        else:
            subtitle = "[$warning]paused[/] [$text-muted]· G to follow[/]"
        if subtitle and log.search_term:
            subtitle = f"{_search_state(log)} [$text-muted]·[/] {subtitle}"
        self.query_one(ContentSwitcher).border_subtitle = subtitle

    def _show_command(self) -> None:
        task_id = self.query_one(Table).highlighted_key()
        task = self.tasks.get(task_id) if task_id else None
        if task is None:
            command = f"valk run fetch {self.run.id} --connect"
        elif self.tab == "detail":
            command = f"valk run task {self.run.id} {shlex.quote(task.task_id)}"
        else:
            term = self.query_one(TaskLog).search_term
            query = f" --query {shlex.quote(term)}" if term else ""
            follow = " --follow" if task.status in ACTIVE_TASKS else ""
            command = f"valk run logs {self.run.id} --task-id {shlex.quote(task.task_id)}{query}{follow}"
        self.query_one(CommandLine).show(command)


def _search_state(log: TaskLog) -> str:
    term = escape(log.search_term)
    if not log.matches:
        return f"[$error]/{term} not found[/]"
    if log.current is None:
        position = f"{len(log.matches)} matches"
    else:
        position = f"{bisect_left(log.matches, log.current) + 1}/{len(log.matches)}"
    return f"[$warning]/{term}[/] [$text-muted]{position}[/]"


def _triage_key(task: TaskSummary) -> tuple[int, str, str, str]:
    category = CATEGORY_LABEL[task.failure_category] if task.failure_category else ""
    return (TRIAGE_ORDER.index(task.status), category, task.error_message or "", task.task_id)


def _task_cells(task: TaskSummary, now: datetime) -> list[Text]:
    glyph, color = task_glyph(task.status)
    if task.failure_category:
        info = Text(CATEGORY_LABEL[task.failure_category], style=palette().error)
    elif task.status == TaskStatus.PENDING:
        info = Text()
    else:
        info = Text(ago(task.started_at, task.finished_at or now, compact=True), style=palette().faint)
    return [Text(glyph, style=color), Text(shown(task.task_id), style=palette().text), info]


def log_line(event: LogEvent) -> str:
    """Format a log event the way a terminal would show it, with continuation lines indented under the message.

    Messages are raw terminal output, and Textual's Log passes escape codes through, so the real terminal would act on
    them and move its cursor. Each line keeps only the text after its last carriage return, as a progress bar ends up,
    without escape codes or other control characters.
    """
    lines = (
        CONTROL_CHARACTERS.sub("", AnsiDecoder().decode_line(line.rstrip("\r")).plain)
        for line in event.message.rstrip("\r\n").split("\n")
    )
    message = ("\n" + " " * 10).join(lines)
    return f"{event.timestamp.astimezone():%H:%M:%S}  {message}"


def _detail(task: SingleTaskResponse, now: datetime) -> Text:
    glyph, color = task_glyph(task.status)
    status = Text(f"{glyph} {status_label(task.status)}", style=f"bold {color}")
    if task.failure_category:
        status.append(f" · {task.failure_category.value}", style=palette().error)
    timing = f"started {task.started_at.astimezone():%H:%M:%S}"
    if task.status != TaskStatus.PENDING:
        timing += f" · ran {ago(task.started_at, task.finished_at or now)}"
    lines = [
        Text(shown(task.task_id), style=f"bold {palette().text}"),
        status,
        Text(timing, style=palette().faint),
    ]
    if task.error_message:
        lines += [Text(), Text(shown(task.error_message), style=palette().error)]
    fields = [
        ("exit reason", task.agent_caused_exit_reason),
        ("evaluation", json.dumps(task.evaluation_result) if task.evaluation_result else None),
        ("id", str(task.id)),
    ]
    lines.append(Text())
    for label, value in fields:
        if value:
            lines.append(Text.assemble((f"{label:<12}", palette().muted), (shown(value), palette().text)))
    # One Text rather than a Group, so the detail can be selected and copied.
    return Text("\n").join(lines)


async def _batched(events: AsyncIterator[T], size: int = 500, wait: float = 0.05) -> AsyncIterator[list[T]]:
    """Group a stream into batches of up to `size` items, yielding early whenever it goes quiet for `wait` seconds."""
    batch: list[T] = []
    pending: asyncio.Task[T] | None = None
    iterator = aiter(events)
    try:
        while True:
            pending = pending or asyncio.ensure_future(anext(iterator))
            done, _ = await asyncio.wait({pending}, timeout=wait if batch else None)
            if not done:
                yield batch
                batch = []
                continue
            try:
                batch.append(pending.result())
            except StopAsyncIteration:
                break
            finally:
                pending = None
            if len(batch) >= size:
                yield batch
                batch = []
    finally:
        if pending is not None:
            pending.cancel()
    if batch:
        yield batch
