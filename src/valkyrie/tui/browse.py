"""Provides the `/` filter, the `?` keys overlay, Esc and command copying that every list screen shares."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, TypeVar, cast

from rich.text import Text
from textual import events, on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.screen import ModalScreen, Screen
from textual.widgets import Input, Static

from valkyrie.tui.theme import palette
from valkyrie.tui.widgets import CommandLine, KeyBar, Table

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp

ResultT = TypeVar("ResultT")


def matches(query: str, *fields: str | None) -> bool:
    """Return whether every term in the query appears in one of the fields, ignoring case."""
    haystack = " ".join(field for field in fields if field).lower()
    return all(term in haystack for term in query.lower().split())


class FilterInput(Input):
    BINDINGS = [Binding("escape", "screen.clear_filter", show=False)]

    def __init__(self) -> None:
        super().__init__(placeholder="type to filter · enter keeps it · esc clears", id="filter")


class BrowseScreen(Screen[ResultT]):
    """Base class for screens built around one table. Subclasses set `KEYS` and implement `refilter()`."""

    BINDINGS = [
        Binding("slash", "open_filter", "filter"),
        Binding("question_mark", "keys", "keys"),
        Binding("y", "copy_command", "copy command"),
        Binding("escape", "back", "back"),
    ]
    KEYS: ClassVar[list[tuple[str, str]]] = []
    CAN_GO_BACK: ClassVar[bool] = False

    filter = ""

    def footer(self) -> ComposeResult:
        """Yield the screen's bottom two lines, the key bar (or the filter while it's open) and the command line."""
        with Horizontal(id="filter-bar"):
            yield Static("/", id="filter-prompt")
            yield FilterInput()
        yield KeyBar()
        yield CommandLine()

    def refilter(self) -> None:
        """Redraw the table with only the rows matching `self.filter`."""
        raise NotImplementedError

    def restyle(self) -> None:
        """Redraw everything after a theme switch, since table cells hold text colored with the old palette."""
        self.refilter()
        for widget in self.walk_children():
            widget.refresh()

    def filter_label(self) -> str:
        return f" · /{self.filter}" if self.filter else ""

    def action_open_filter(self) -> None:
        self.add_class("-filtering")
        self.query_one(FilterInput).focus()

    def action_clear_filter(self) -> None:
        self.query_one(FilterInput).value = ""
        self._close_filter()

    def action_back(self) -> None:
        if self.filter:
            self.action_clear_filter()
        elif self.CAN_GO_BACK:
            self.app.pop_screen()

    @on(Input.Changed, "#filter")
    def _filter_changed(self, event: Input.Changed) -> None:
        self.filter = event.value
        self.refilter()

    @on(Input.Submitted, "#filter")
    def _close_filter(self) -> None:
        self.remove_class("-filtering")
        self.query_one(Table).focus()

    def action_keys(self) -> None:
        self.app.push_screen(KeysScreen(self.KEYS))

    def action_copy_command(self) -> None:
        command_line = self.query_one(CommandLine)
        cast("ValkyrieApp", self.app).copy(command_line.command)
        command_line.flash_copied()


class KeysScreen(ModalScreen[None]):
    """Lists the screen's keys. Any key or click closes it, and the key does nothing else."""

    def __init__(self, keys: list[tuple[str, str]]) -> None:
        super().__init__()
        self.keys = keys

    def compose(self) -> ComposeResult:
        width = max(len(key) for key, _ in self.keys)
        lines = Text("\n").join(
            Text.assemble((f"{key:>{width}}", f"bold {palette().accent}"), "   ", (description, palette().text))
            for key, description in self.keys
        )
        yield Static(lines, id="keys", classes="panel")

    def on_mount(self) -> None:
        keys = self.query_one("#keys")
        keys.border_title = "Keys"
        keys.border_subtitle = "any key closes"

    def on_key(self, event: events.Key) -> None:
        event.stop()
        self.dismiss()

    def on_click(self) -> None:
        self.dismiss()
