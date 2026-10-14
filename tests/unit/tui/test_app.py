"""Tests for `valk ui` on the fake data source.

Run: uv run pytest tests/unit/tui/test_app.py

Covers the run list, run view and errors rendering, live status refresh, triage order, the command line, and the
UI's import boundary.
"""

import subprocess
import sys
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timezone
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from rich.text import Text
from textual.pilot import Pilot
from textual.widgets import DataTable, Log, Static

from valkyrie.tui.app import ValkyrieApp
from valkyrie.tui.browse import KeysScreen
from valkyrie.tui.errors import ErrorsScreen
from valkyrie.tui.fake import FakeSource
from valkyrie.sdk import ValkyrieAPIError, ValkyrieClient, ValkyrieConfig, ValkyrieSDKError
from valkyrie.sdk.models import BenchmarkTableRow, LogEvent, SingleTaskResponse, TaskStatus
from valkyrie.tui.run import RunScreen, TaskLog, log_line
from valkyrie.tui.runs import RunsScreen
from valkyrie.tui.source import Environment, RunSource, TrackerSource
from valkyrie.tui.widgets import TopBar

from tests.unit.tui.conftest import MockClock

_SIZE = (140, 36)
_LIVE_RUN = "3f2a91c4-7b1e-4d0a-9c55-8e2f1a6b0d37"


@asynccontextmanager
async def _open_app(clock: MockClock) -> AsyncGenerator[Pilot[None]]:
    app = ValkyrieApp(FakeSource(clock=clock, follow=False), now=clock)
    async with app.run_test(size=_SIZE) as pilot:
        await _settle(pilot)
        yield pilot


async def _settle(pilot: Pilot[None]) -> None:
    """Wait out workers, including the log and detail workers a cursor move starts."""
    for _ in range(3):
        await pilot.app.workers.wait_for_complete()
        await pilot.pause()


class TestRunsScreen:
    """The run list and the preview of the highlighted run."""

    async def test_highlighted_run_is_previewed_and_its_command_copied(
        self, clock: MockClock, assert_snapshot: Callable[[str, str], None]
    ) -> None:
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j")

            assert_snapshot(pilot.app.export_screenshot(), "runs")
            await pilot.press("y")
            assert pilot.app.clipboard == f"valk run fetch {_LIVE_RUN} --connect"

    async def test_status_poll_updates_live_runs(self, clock: MockClock) -> None:
        async with _open_app(clock) as pilot:
            screen = cast(RunsScreen, pilot.app.screen)
            before = screen.rows[_LIVE_RUN]

            clock.advance(seconds=5)
            screen.poll_statuses()
            await pilot.app.workers.wait_for_complete()

            after = screen.rows[_LIVE_RUN]
            assert after.task_state_counts != before.task_state_counts

            progress = cast(Text, screen.query_one(DataTable).get_cell(_LIVE_RUN, "progress"))
            assert f"{after.finished_tasks}/{after.total_tasks}" in progress.plain

    async def test_c_switches_to_a_tracker_with_a_saved_login_and_reloads_the_runs(self, clock: MockClock) -> None:
        urls = ("https://benchmark-tracker.test", "http://localhost:8000")
        sources = {url: FakeSource(clock=clock, follow=False) for url in urls}
        for url, source in sources.items():
            source.environment = Environment.of(url)

        class Switcher:
            def choices(self) -> list[Environment]:
                return [source.environment for source in sources.values()]

            def open(self, url: str) -> RunSource:
                return sources[url]

        app = ValkyrieApp(sources[urls[0]], now=clock, switcher=Switcher())
        async with app.run_test(size=_SIZE) as pilot:
            await _settle(pilot)
            await pilot.press("c", "down", "enter")
            await _settle(pilot)

            assert app.source is sources[urls[1]]
            assert pilot.app.screen.query_one(TopBar).environment.name == "localhost:8000"
            assert pilot.app.screen.query_one(DataTable).row_count > 0

            await pilot.press("y")
            assert app.clipboard.startswith("VALKYRIE_URL=http://localhost:8000 valk run ")

    @pytest.mark.parametrize("close", ["x", "q", "click"])
    async def test_keys_overlay_closes_on_any_key_or_click_without_acting_on_it(
        self, clock: MockClock, close: str
    ) -> None:
        async with _open_app(clock) as pilot:
            runs = pilot.app.screen
            await pilot.press("question_mark")
            assert isinstance(pilot.app.screen, KeysScreen)

            await (pilot.click() if close == "click" else pilot.press(close))

            assert pilot.app.screen is runs
            assert pilot.app.is_running


class TestRunScreen:
    """A run's summary, its tasks errors first, and the highlighted task's logs and detail."""

    async def test_the_shown_task_reloads_its_log_and_detail_when_it_starts(self, clock: MockClock) -> None:
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)
            screen = cast(RunScreen, pilot.app.screen)
            table = screen.query_one(DataTable)
            log = screen.query_one(Log)
            pending = next(task for task in screen.tasks.values() if task.status == TaskStatus.PENDING)
            table.move_cursor(row=table.get_row_index(pending.task_id))
            await _settle(pilot)
            assert log.lines == ["No log lines yet."]

            source = cast(FakeSource, screen.source)
            started = next(task for task in source._run(screen.run.id).tasks if task.task_id == pending.task_id)
            started.status = TaskStatus.BUILDING
            screen.load_tasks()
            await _settle(pilot)

            assert log.line_count > 1
            assert "building" in str(screen.query_one("#detail-body", Static).content)

    @pytest.mark.parametrize("failed_again", [False, True], ids=["pending", "failed-again"])
    async def test_a_retried_task_shows_its_new_attempt_not_the_cached_one(
        self, clock: MockClock, failed_again: bool
    ) -> None:
        """A retry that fails again before the next refresh keeps the ERROR status but has a later start time."""
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)
            screen = cast(RunScreen, pilot.app.screen)
            log = screen.query_one(Log)
            failed = next(iter(screen.tasks.values()))
            assert failed.status == TaskStatus.ERROR and log.line_count > 1

            source = cast(FakeSource, screen.source)
            retried = next(task for task in source._run(screen.run.id).tasks if task.task_id == failed.task_id)
            before = list(log.lines)
            retried.started_at = clock()
            if failed_again:
                assert retried.error is not None
                retried.finished_at = clock()
                retried.error = (retried.error[0], "second attempt failed")
            else:
                retried.status = TaskStatus.PENDING
                retried.finished_at = retried.error = None
            screen.load_tasks()
            await _settle(pilot)

            assert log.lines != before
            detail = str(screen.query_one("#detail-body", Static).content)
            assert ("second attempt failed" if failed_again else "pending") in detail

    async def test_a_dropped_log_stream_reconnects(self, clock: MockClock, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("valkyrie.tui.run.RECONNECT_SECONDS", 0.01)
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)
            screen = cast(RunScreen, pilot.app.screen)
            source = cast(FakeSource, screen.source)
            stream = source.stream_logs
            attempts = 0

            async def drops_once(run_id: UUID, task_id: str) -> AsyncIterator[LogEvent]:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise ValkyrieSDKError("connection reset")
                async for event in stream(run_id, task_id):
                    yield event

            monkeypatch.setattr(source, "stream_logs", drops_once)
            table = screen.query_one(DataTable)
            running = next(task for task in screen.tasks.values() if task.status == TaskStatus.IN_PROGRESS)
            table.move_cursor(row=table.get_row_index(running.task_id))
            await _settle(pilot)
            await pilot.pause(0.05)
            await _settle(pilot)

            log = screen.query_one(Log)
            assert attempts == 2
            assert log.line_count > 1
            assert not any("reconnecting" in line for line in log.lines)

    async def test_errors_come_first_and_command_follows_the_pane(
        self, clock: MockClock, assert_snapshot: Callable[[str, str], None]
    ) -> None:
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)

            assert_snapshot(pilot.app.export_screenshot(), "run")
            screen = cast(RunScreen, pilot.app.screen)
            first = next(iter(screen.tasks.values()))
            assert first.status == TaskStatus.ERROR

            await pilot.press("y")
            assert pilot.app.clipboard == f"valk run logs {_LIVE_RUN} --task-id {first.task_id}"

            await pilot.press("right_square_bracket", "y")
            assert pilot.app.clipboard == f"valk run task {_LIVE_RUN} {first.task_id}"

            await pilot.press("escape")
            runs = cast(RunsScreen, pilot.app.screen)
            assert runs.query_one(DataTable).cursor_row == 3

    async def test_filter_narrows_tasks_and_esc_clears_it_before_going_back(self, clock: MockClock) -> None:
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)
            screen = cast(RunScreen, pilot.app.screen)
            table = screen.query_one(DataTable)
            everything = table.row_count

            await pilot.press("slash", *"dockerd", "enter")

            shown = [screen.tasks[str(row.value)] for row in table.rows]
            assert 0 < len(shown) < everything
            assert all("dockerd" in (task.error_message or "") for task in shown)

            await pilot.press("slash", *"-nothing", "enter", "y")
            log = screen.query_one(Log)
            assert (table.row_count, log.line_count) == (0, 0)
            assert pilot.app.clipboard == f"valk run fetch {_LIVE_RUN} --connect"

            await pilot.press("escape")
            await _settle(pilot)
            assert table.row_count == everything
            assert log.line_count > 0
            assert pilot.app.screen is screen

    async def test_slash_in_the_log_searches_it_and_n_steps_through_the_matches(self, clock: MockClock) -> None:
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)
            screen = cast(RunScreen, pilot.app.screen)
            table = screen.query_one(DataTable)
            everything = table.row_count
            log = screen.query_one(TaskLog)

            # Every fake log opens with "Creating sandbox" and "Sandbox sb-… ready", so the term matches lines 0 and 1.
            await pilot.press("tab", "slash", *"andbox", "enter")

            assert table.row_count == everything
            assert log.matches == (0, 1)
            visited = [log.current]
            for key in ("n", "n", "N"):
                await pilot.press(key)
                visited.append(log.current)
            assert visited == [0, 1, 0, 1]

            await pilot.press("y")
            assert pilot.app.clipboard.endswith(" --query andbox")

            await pilot.press("escape")
            assert log.search_term == ""
            assert pilot.app.screen is screen


class TestErrorsScreen:
    """A run's errors grouped by category and message."""

    async def test_enter_shows_the_groups_first_task_even_when_the_run_filter_hides_it(
        self, clock: MockClock, assert_snapshot: Callable[[str, str], None]
    ) -> None:
        async with _open_app(clock) as pilot:
            await pilot.press("j", "j", "j", "enter")
            await _settle(pilot)
            run = cast(RunScreen, pilot.app.screen)
            await pilot.press("slash", *"agent", "enter", "e")

            assert_snapshot(pilot.app.export_screenshot(), "errors")
            errors = cast(ErrorsScreen, pilot.app.screen)
            first_task = errors.groups[0][2][0]
            assert first_task not in run.query_one(DataTable).rows

            await pilot.press("y")
            assert pilot.app.clipboard == f"valk run errors {_LIVE_RUN}"

            await pilot.press("enter")
            await _settle(pilot)
            assert pilot.app.screen is run
            assert run.filter == ""
            assert run.shown_task == first_task


@pytest.mark.parametrize(
    ("message", "shown"),
    [
        # uv redraws progress with escape codes and carriage returns; left in, they moved the real terminal's cursor.
        (
            "\x1b[2K\x1b[1A\x1b[1mBuilding\x1b[0m mini-swe-agent\rPreparing packages... (0/112)",
            "Preparing packages... (0/112)",
        ),
        ("Cloning into 'repo'...\r\n", "Cloning into 'repo'..."),
        (
            "Traceback (most recent call last):\nRuntimeError: boom",
            "Traceback (most recent call last):\n" + " " * 10 + "RuntimeError: boom",
        ),
    ],
)
def test_log_lines_show_what_a_terminal_would(message: str, shown: str) -> None:
    event = LogEvent(timestamp=datetime(2026, 10, 12, 18, 0, tzinfo=timezone.utc), message=message)

    assert log_line(event)[10:] == shown


async def test_a_tracker_without_credentials_opens(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A local Tracker can run without authentication, so a missing login isn't refused before any request."""
    monkeypatch.setenv("HOME", str(tmp_path))
    for name in ("VALKYRIE_URL", "VALKYRIE_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    source = TrackerSource.from_config(ValkyrieConfig(url="http://127.0.0.1:8000"))

    assert source.environment.name == "127.0.0.1:8000"
    await source.close()


async def test_a_tracker_that_needs_a_login_says_how_to_log_in(
    clock: MockClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    class LoggedOut(FakeSource):
        async def runs(self, limit: int) -> list[BenchmarkTableRow]:
            raise ValkyrieAPIError(401, "Not authenticated")

    app = ValkyrieApp(LoggedOut(clock=clock, follow=False), now=clock)
    notified: list[str] = []
    monkeypatch.setattr(app, "notify", lambda message, **_: notified.append(str(message)))
    async with app.run_test(size=_SIZE) as pilot:
        await _settle(pilot)

    assert any("valk login" in message for message in notified)


async def test_tracker_source_reports_a_task_id_the_tracker_cant_address_as_an_sdk_error() -> None:
    """Screens catch only SDK errors, so the SDK's ValueError for IDs like `jobshoph/rep1` used to crash the app."""
    client = ValkyrieClient(ValkyrieConfig(), transport=httpx.MockTransport(lambda _request: httpx.Response(500)))
    source = TrackerSource(client, Environment("dev", "https://tracker.test"))

    with pytest.raises(ValkyrieSDKError, match="must not contain '/'"):
        await source.task(uuid4(), "jobshoph/rep1")


@pytest.mark.parametrize(
    ("url", "opened"),
    [
        ("https://docent.test/r/1", True),
        ("file:///etc/passwd", False),
        ("https://[", False),
        ("https:///no-host", False),
    ],
)
async def test_o_opens_only_web_docent_links(
    clock: MockClock, monkeypatch: pytest.MonkeyPatch, url: str, opened: bool
) -> None:
    calls: list[str] = []
    monkeypatch.setattr("webbrowser.open", calls.append)
    async with _open_app(clock) as pilot:
        await pilot.press("j", "j", "j", "enter")
        await _settle(pilot)
        screen = cast(RunScreen, pilot.app.screen)
        screen.docent_url = url

        await pilot.press("o")

    assert calls == ([url] if opened else [])


_POISON = "\x1b]52;c;ZXZpbA==\x07\x1b[2J"


def _poison(source: FakeSource) -> None:
    """Append escape sequences to every string the fake Tracker sends: run fields, task IDs and error messages."""
    fields = ("name", "agent_name", "model", "label", "started_by_email", "error_message")
    for run in source._runs:
        run.row = run.row.model_copy(update={name: (getattr(run.row, name) or "") + _POISON for name in fields})
        for task in run.tasks:
            task.task_id += _POISON
            if task.error:
                task.error = (task.error[0], task.error[1] + _POISON)


async def test_tracker_text_reaches_the_terminal_escaped(clock: MockClock) -> None:
    """The models keep the Tracker's text, so every screen escapes it when drawing; here it sets the clipboard."""
    source = FakeSource(clock=clock, follow=False)
    _poison(source)
    app = ValkyrieApp(source, now=clock)
    screenshots: list[str] = []
    async with app.run_test(size=_SIZE) as pilot:
        await _settle(pilot)
        screenshots.append(app.export_screenshot())
        for keys in (("j", "j", "j", "enter"), ("right_square_bracket",), ("e",)):
            await pilot.press(*keys)
            await _settle(pilot)
            screenshots.append(app.export_screenshot())
        app.notify("toast" + _POISON)
        await pilot.pause()
        screenshots.append(app.export_screenshot())

    assert not [index for index, screenshot in enumerate(screenshots) if "\x1b" in screenshot]


async def test_tracker_errors_reach_the_terminal_escaped(clock: MockClock) -> None:
    class Failing(FakeSource):
        async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse:
            raise ValkyrieAPIError(500, "detail failed" + _POISON)

        async def fetch_logs(self, run_id: UUID, task_id: str) -> list[LogEvent]:
            raise ValkyrieAPIError(500, "log failed" + _POISON)

    async with ValkyrieApp(Failing(clock=clock, follow=False), now=clock).run_test(size=_SIZE) as pilot:
        await pilot.press("j", "j", "j", "enter")
        await _settle(pilot)
        screen = cast(RunScreen, pilot.app.screen)
        log = "\n".join(screen.query_one(Log).lines)
        detail = str(screen.query_one("#detail-body", Static).content)

    assert "log failed" in log and "detail failed" in detail
    assert "\x1b" not in log + detail


def test_ui_reaches_the_tracker_only_through_the_sdk() -> None:
    """Runs in a subprocess because this test process has already imported the server."""
    code = (
        "import sys, valkyrie.tui.app, valkyrie.tui.fake, valkyrie.tui.source\n"
        "print(sorted(name for name in ('tracker', 'fastapi', 'daytona', 'modal') if name in sys.modules))"
    )

    loaded = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout

    assert loaded.strip() == "[]"
