"""Shows recent runs with live status, and a preview of the highlighted one."""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, ClassVar, cast

from rich.console import Group, RenderableType
from rich.text import Text
from textual import events, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.screen import ModalScreen
from textual.widgets import OptionList
from textual.widgets.option_list import Option
from textual.reactive import reactive
from textual.markup import escape
from textual.widget import Widget

from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieSDKError
from valkyrie.sdk.models import BenchmarkStatusEntry, BenchmarkTableRow, TaskStatus
from valkyrie.tui.theme import palette
from valkyrie.tui.browse import BrowseScreen, matches
from valkyrie.tui.run import RunScreen
from valkyrie.tui.source import FINAL_STATUSES, Environment, RunSource
from valkyrie.tui.widgets import (
    CommandLine,
    KeyBar,
    Table,
    TopBar,
    ago,
    progress_bar,
    run_glyph,
    run_status,
    short_id,
    shown,
    status_label,
    task_glyph,
)

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp

RUN_LIMIT = 50
STATUS_POLL_SECONDS = 3
LIST_REFRESH_SECONDS = 30
# Same order as the progress bar's segments.
COUNT_ORDER = [
    TaskStatus.FINISHED,
    TaskStatus.ERROR,
    TaskStatus.EVALUATING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.BUILDING,
    TaskStatus.STOPPED,
    TaskStatus.PENDING,
]


class RunPreview(Widget):
    """Shows the highlighted run's details, progress and per-state counts."""

    run: reactive[BenchmarkTableRow | None] = reactive(None)

    def render(self) -> RenderableType:
        run = self.run
        if run is None:
            return Text("No runs yet.", style=palette().muted)
        now = cast("ValkyrieApp", self.app).now()
        title = _line((shown(run.name), f"bold {palette().text}"))
        if run.label:
            title.append(f"  {shown(run.label)}", style=palette().accent)
        started = shown(" · ".join(part for part in (f"{ago(run.started_at, now)} ago", run.started_by_email) if part))
        glyph, color = run_glyph(run.status)
        status = _line((f"{glyph} {status_label(run.status)}", f"bold {color}"))
        if run.final_score is not None:
            status.append(f"   score {run.final_score:.3f}", style=palette().text)

        lines: list[RenderableType] = [
            title,
            _line((shown(" · ".join(part for part in (run.agent_name, run.model) if part)), palette().muted)),
            _line((started, palette().faint)),
            Text(),
            status,
        ]
        if run.total_tasks:
            percent = round(100 * run.finished_tasks / run.total_tasks)
            summary = f"  {run.finished_tasks}/{run.total_tasks}  {percent}%"
            width = max(self.content_size.width - len(summary), 8)
            lines += [
                progress_bar(run.task_state_counts, run.total_tasks, width) + Text(summary, style=palette().muted)
            ]
            lines += [Text(), *_counts(run)]
        if run.error_message:
            lines += [Text(), Text(shown(run.error_message), style=palette().error)]
        return Group(*lines)


def _line(*parts: tuple[str, str]) -> Text:
    return Text.assemble(*parts, no_wrap=True, overflow="ellipsis")


def _counts(run: BenchmarkTableRow) -> list[Text]:
    lines = []
    for status in COUNT_ORDER:
        count = run.task_state_counts.get(status.value, 0)
        if count:
            glyph, color = task_glyph(status)
            lines.append(
                Text.assemble((f"{glyph} ", color), (f"{status_label(status):<12}", palette().muted), f"{count:>5}")
            )
    return lines


class EnvironmentScreen(ModalScreen[str | None]):
    """Lists the Trackers to switch to by URL. Dismisses with the picked URL."""

    BINDINGS = [Binding("escape,q,c", "dismiss", show=False)]

    def __init__(self, choices: list[Environment], current: str) -> None:
        super().__init__()
        self.choices = choices
        self.current = current

    def compose(self) -> ComposeResult:
        colors = palette()
        options = [
            Option(
                Text.assemble(
                    ("● " if choice.url == self.current else "  ", colors.accent), (choice.url, f"bold {colors.text}")
                ),
                id=choice.url,
            )
            for choice in self.choices
        ]
        yield OptionList(*options, id="environments")

    def on_mount(self) -> None:
        menu = self.query_one(OptionList)
        menu.border_title = "Tracker"
        menu.border_subtitle = "enter switches · esc closes"
        menu.highlighted = next(index for index, choice in enumerate(self.choices) if choice.url == self.current)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.dismiss(event.option.id)

    def on_click(self, event: events.Click) -> None:
        if event.widget is self:
            self.dismiss(None)


class RunsScreen(BrowseScreen[None]):
    BINDINGS = [Binding("r", "reload", "reload"), Binding("c", "switch_environment", "tracker")]
    KEYS: ClassVar[list[tuple[str, str]]] = [
        ("j k ↑ ↓", "move"),
        ("g G", "top, bottom"),
        ("enter", "open the run"),
        ("/", "filter by benchmark, agent, model, label, status or who started it"),
        ("esc", "clear the filter"),
        ("y", "copy the command on the bottom line"),
        ("r", "reload the list"),
        ("c", "switch to another Tracker you've logged in to; clicking its name does too"),
        ("t", "light or dark theme"),
        ("?", "these keys"),
        ("q", "quit"),
    ]

    def __init__(self, source: RunSource) -> None:
        super().__init__()
        self.source = source
        self.rows: dict[str, BenchmarkTableRow] = {}

    def compose(self) -> ComposeResult:
        yield TopBar(self.source.environment)
        with Horizontal(id="body"):
            yield Table(id="runs")
            yield RunPreview(id="preview", classes="panel")
        yield from self.footer()

    def on_mount(self) -> None:
        table = self.query_one(Table)
        table.border_title = "Runs"
        for label, key in [("", "status"), ("id", "id"), ("benchmark", "benchmark"), ("agent", "agent")]:
            table.add_column(label, key=key)
        for label, key in [("model", "model"), ("progress", "progress"), ("", "errors"), ("age", "age")]:
            table.add_column(label, key=key)
        self.query_one(KeyBar).show(
            ("enter", "open"), ("/", "filter"), ("c", "tracker"), ("y", "copy cmd"), ("?", "keys"), ("q", "quit")
        )
        self.query_one(CommandLine).show("valk run list")
        self.load_runs()
        self.set_interval(STATUS_POLL_SECONDS, self.poll_statuses)
        self.set_interval(LIST_REFRESH_SECONDS, self.load_runs)

    def action_switch_environment(self) -> None:
        app = cast("ValkyrieApp", self.app)
        if app.switcher is None:
            self.notify("Sample data has no Trackers to switch to.")
            return
        current = self.source.environment
        choices = app.switcher.choices()
        if current.url not in {choice.url for choice in choices}:
            choices = [current, *choices]
        if len(choices) < 2:
            self.notify("Only this Tracker has a saved login. `VALKYRIE_URL=<url> valk login` adds another.")
            return
        app.push_screen(EnvironmentScreen(choices, current.url), self._switch_to)

    def on_top_bar_badge_clicked(self) -> None:
        self.action_switch_environment()

    def _switch_to(self, url: str | None) -> None:
        if url is not None and url != self.source.environment.url:
            cast("ValkyrieApp", self.app).switch_environment(url)

    @work(exclusive=True, group="runs")
    async def load_runs(self) -> None:
        if self.rows and not self.is_current:
            return
        try:
            runs = await self.source.runs(RUN_LIMIT)
        except ValkyrieSDKError as error:
            self._report(error)
            return
        self.rows = {str(run.id): run for run in runs}
        self.refilter()
        self._fresh()

    def refilter(self) -> None:
        table = self.query_one(Table)
        visible = {key: run for key, run in self.rows.items() if self._matches(run)}
        table.show_rows({key: self._cells(run) for key, run in visible.items()})
        count = f"{len(visible)}/{len(self.rows)}" if self.filter else str(len(self.rows))
        table.border_title = f"Runs · {count}{self.filter_label()}"
        self._show_highlighted()

    def _matches(self, run: BenchmarkTableRow) -> bool:
        fields = (run.name, run.agent_name, run.model, run.label, status_label(run.status), short_id(run.id))
        return matches(self.filter, *fields, run.started_by_email)

    @work(exclusive=True, group="statuses")
    async def poll_statuses(self) -> None:
        live = [run.id for run in self.rows.values() if run.status not in FINAL_STATUSES]
        if not live or not self.is_current:
            return
        try:
            entries = await self.source.statuses(live)
        except ValkyrieSDKError as error:
            self._report(error)
            return
        for entry in entries:
            self._apply_status(entry)
        self.refilter()
        self._fresh()

    def _apply_status(self, entry: BenchmarkStatusEntry) -> None:
        key = str(entry.id)
        if key not in self.rows:
            return
        run = self.rows[key].model_copy(
            update={
                "status": entry.status,
                "finished_at": entry.finished_at,
                "total_tasks": entry.total_tasks,
                "finished_tasks": entry.finished_tasks,
                "task_state_counts": entry.task_state_counts,
            }
        )
        self.rows[key] = run

    def _cells(self, run: BenchmarkTableRow) -> list[Text]:
        now = cast("ValkyrieApp", self.app).now()
        if run.total_tasks:
            progress = progress_bar(run.task_state_counts, run.total_tasks, 10)
            progress.append(f" {run.finished_tasks}/{run.total_tasks}", style=palette().muted)
        else:
            progress = Text(status_label(run.status), style=run_glyph(run.status)[1])
        errors = run.task_state_counts.get(TaskStatus.ERROR.value, 0)
        return [
            run_status(run.status),
            Text(short_id(run.id), style=palette().muted),
            Text(shown(run.name), style=palette().text),
            Text(shown(run.agent_name), style=palette().muted),
            Text(shown(run.model or ""), style=palette().muted),
            progress,
            Text(f"✗ {errors}" if errors else "", style=palette().error),
            Text(ago(run.started_at, now, compact=True), style=palette().faint, justify="right"),
        ]

    def on_data_table_row_highlighted(self, _event: Table.RowHighlighted) -> None:
        self._show_highlighted()

    def on_data_table_row_selected(self, event: Table.RowSelected) -> None:
        self.app.push_screen(RunScreen(self.source, self.rows[str(event.row_key.value)]))

    def action_reload(self) -> None:
        self.load_runs()

    def _show_highlighted(self) -> None:
        key = self.query_one(Table).highlighted_key()
        run = self.rows.get(key) if key else None
        preview = self.query_one(RunPreview)
        preview.run = run
        preview.border_title = f"{short_id(run.id)} · {escape(shown(run.name))}" if run else ""
        self.query_one(CommandLine).show(f"valk run fetch {run.id} --connect" if run else "valk run list")

    def _fresh(self) -> None:
        self.query_one(TopBar).status = ("● live", "accent")

    def _report(self, error: ValkyrieSDKError) -> None:
        # A local Tracker can run without authentication, so the Tracker, not the UI, says when a login is needed.
        if isinstance(error, ValkyrieAPIError) and error.status_code == 401:
            url = shlex.quote(self.source.environment.url)
            self.query_one(TopBar).status = ("⚠ not logged in", "warning")
            message = f"{error}\nLog in with `VALKYRIE_URL={url} valk login`, or set VALKYRIE_API_KEY."
            self.notify(message, title="Not logged in", severity="error")
            return
        self.query_one(TopBar).status = ("⚠ tracker unreachable, retrying", "warning")
        self.notify(str(error), title="Tracker error", severity="error")
