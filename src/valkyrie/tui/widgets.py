"""Provides the small renderers and one-line bars every screen shares."""

from __future__ import annotations

import shlex
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, cast

from rich.text import Text
from textual import events
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.binding import Binding
from textual.widgets import DataTable

from valkyrie.cli.display import terminal_safe
from valkyrie.sdk.models import BenchmarkStatus, FailureCategory, TaskStatus
from valkyrie.tui.theme import BRAND, palette
from valkyrie.tui.source import Environment

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp


def shown(text: str) -> str:
    """Escape terminal control characters in text from the Tracker, at the point it's drawn.

    Textual draws text as given, so an escape sequence in a run name or an error message would otherwise reach the
    terminal, where it can move the cursor or set the clipboard (OSC 52). The models keep the Tracker's text, since
    IDs go back to it in requests.
    """
    return terminal_safe(text, preserve_newlines=True)


# Each status's glyph and the palette role that colors it.
RUN_STATUS = {
    BenchmarkStatus.IN_PROGRESS: ("●", "running"),
    BenchmarkStatus.STOPPING: ("◍", "warning"),
    BenchmarkStatus.STOPPED: ("■", "stopped"),
    BenchmarkStatus.FINISHED: ("✓", "finished"),
    BenchmarkStatus.ERROR: ("✗", "error"),
}
TASK_STATUS = {
    TaskStatus.PENDING: ("◌", "faint"),
    TaskStatus.BUILDING: ("◔", "running"),
    TaskStatus.IN_PROGRESS: ("●", "running"),
    TaskStatus.EVALUATING: ("◑", "warning"),
    TaskStatus.FINISHED: ("✓", "finished"),
    TaskStatus.ERROR: ("✗", "error"),
    TaskStatus.STOPPED: ("■", "stopped"),
}
CATEGORY_LABEL = {
    FailureCategory.INFRASTRUCTURE: "infra",
    FailureCategory.BENCHMARK_SERVICE: "service",
    FailureCategory.AGENT: "agent",
    FailureCategory.CANCELLED: "cancelled",
    FailureCategory.UNKNOWN: "unknown",
}
# Bar segments, left to right: what's done, what failed, what's running, then the empty track.
BAR_SEGMENTS = [
    (("FINISHED",), "finished"),
    (("ERROR",), "error"),
    (("EVALUATING", "IN_PROGRESS", "BUILDING"), "running"),
    (("STOPPED",), "faint"),
]


def run_glyph(status: BenchmarkStatus) -> tuple[str, str]:
    glyph, role = RUN_STATUS[status]
    return glyph, getattr(palette(), role)


def task_glyph(status: TaskStatus) -> tuple[str, str]:
    glyph, role = TASK_STATUS[status]
    return glyph, getattr(palette(), role)


def short_id(value: object) -> str:
    return str(value)[:8]


def run_status(status: BenchmarkStatus) -> Text:
    glyph, color = run_glyph(status)
    return Text(glyph, style=color)


def status_label(status: BenchmarkStatus | TaskStatus) -> str:
    return status.value.replace("_", " ").lower()


def progress_bar(counts: Mapping[str, int], total: int, width: int) -> Text:
    """Render a bar split by task state, so failures show up in the bar and not only in a count."""
    bar = Text()
    used = 0
    for names, role in BAR_SEGMENTS:
        color = getattr(palette(), role)
        count = sum(counts.get(name, 0) for name in names)
        cells = round(width * count / total) if total else 0
        cells = min(cells, width - used)
        bar.append("━" * cells, style=color)
        used += cells
    bar.append("━" * (width - used), style=palette().track)
    return bar


def ago(moment: datetime, now: datetime, *, compact: bool = False) -> str:
    seconds = max(int((now - moment).total_seconds()), 0)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h" if compact else f"{seconds // 3600}h {seconds % 3600 // 60:02}m"
    return f"{seconds // 86400}d"


def keys_text(*pairs: tuple[str, str]) -> Text:
    text = Text()
    for index, (key, description) in enumerate(pairs):
        if index:
            text.append("   ")
        text.append(key, style=f"bold {palette().accent}")
        text.append(f" {description}", style=palette().muted)
    return text


class TopBar(Widget):
    """Shows the environment badge, where you are, and whether the data is live. `trail` and `status` hold (text, palette role) pairs."""

    trail: reactive[tuple[tuple[str, str], ...]] = reactive(())
    status: reactive[tuple[str, str]] = reactive(("", "muted"))

    class BadgeClicked(Message):
        """Posted when the environment badge is clicked."""

    def __init__(self, environment: Environment) -> None:
        super().__init__()
        self.environment = environment

    def on_click(self, event: events.Click) -> None:
        start = len("valkyrie  ")
        if start <= event.x < start + len(self.environment.name) + 2:
            self.post_message(self.BadgeClicked())

    def render(self) -> Text:
        name = self.environment.name
        colors = palette()
        badge_style = f"bold {colors.background} on {colors.warning}" if name == "fake" else f"bold #FFFFFF on {BRAND}"
        trail = [(text, getattr(colors, role)) for text, role in self.trail]
        left = Text.assemble(("valkyrie", f"bold {colors.accent}"), "  ", (f" {name} ", badge_style), "  ", *trail)
        status = Text(self.status[0], style=getattr(colors, self.status[1]))
        gap = max(self.content_size.width - left.cell_len - status.cell_len, 1)
        return left + Text(" " * gap) + status


class Table(DataTable[Text]):
    """Extends DataTable with a row cursor that vim keys (j, k, g, G) and the mouse wheel move.

    The wheel moves the cursor rather than the view, since the highlighted row is what the rest of the screen shows.
    """

    BINDINGS = [
        Binding("j", "cursor_down", show=False),
        Binding("k", "cursor_up", show=False),
        Binding("g", "scroll_top", show=False),
        Binding("G", "scroll_bottom", show=False),
    ]

    def __init__(self, *, id: str) -> None:
        super().__init__(id=id, classes="panel", cursor_type="row", cursor_foreground_priority="renderable")

    def on_mouse_scroll_down(self, event: events.MouseScrollDown) -> None:
        event.prevent_default()
        event.stop()
        self.action_cursor_down()

    def on_mouse_scroll_up(self, event: events.MouseScrollUp) -> None:
        event.prevent_default()
        event.stop()
        self.action_cursor_up()

    def highlighted_key(self) -> str | None:
        """Return the key of the row under the cursor, or None when the table is empty."""
        if not self.row_count:
            return None
        row_key, _ = self.coordinate_to_cell_key(self.cursor_coordinate)
        return str(row_key.value)

    def show_rows(self, rows: Mapping[str, list[Text]]) -> None:
        """Show these rows, keeping the cursor on the same key.

        When the keys are unchanged the cells update in place, so the cursor doesn't move and no highlight event fires.
        """
        with self.app.batch_update():
            if list(rows) == [row.value for row in self.rows]:
                for key, cells in rows.items():
                    for column, cell in zip(self.columns, cells, strict=True):
                        self.update_cell(key, column, cell)
                return
            highlighted = self.highlighted_key()
            self.clear()
            for key, cells in rows.items():
                self.add_row(*cells, key=key)
            if highlighted is not None and highlighted in rows:
                self.move_cursor(row=self.get_row_index(highlighted))


class KeyBar(Widget):
    pairs: reactive[tuple[tuple[str, str], ...]] = reactive(())

    def show(self, *pairs: tuple[str, str]) -> None:
        self.pairs = pairs

    def render(self) -> Text:
        return keys_text(*self.pairs)


class CommandLine(Widget):
    """Shows the `valk` command for what's on screen, which `y` copies."""

    command: reactive[str] = reactive("")
    copied: reactive[bool] = reactive(False)

    def show(self, command: str) -> None:
        """Show `command`, pointed at the Tracker on screen when it isn't the one the shell's `valk` reaches."""
        app = cast("ValkyrieApp", self.app)
        url = app.source.environment.url
        self.command = shown(command if url == app.launch_url else f"VALKYRIE_URL={shlex.quote(url)} {command}")

    def flash_copied(self) -> None:
        self.copied = True
        self.set_timer(1.5, lambda: setattr(self, "copied", False))

    def render(self) -> Text:
        colors = palette()
        if self.copied:
            return Text.assemble(("✓ ", f"bold {colors.accent}"), (f"copied  {self.command}", colors.text))
        return Text.assemble(("$ ", f"bold {colors.accent}"), (self.command, colors.muted))
