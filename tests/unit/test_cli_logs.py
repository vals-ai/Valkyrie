from uuid import uuid4

from click.testing import CliRunner

from valkyrie.cli import main
from valkyrie.cli.main import cli


def test_run_logs_lists_tasks_when_non_interactive(monkeypatch):
    """The logs command should list task IDs when a task is not selected.

    Test cases:
    - Health check is bypassed in the unit test
    - Non-interactive output includes the task IDs and a copyable command
    """

    class FakeTracker:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def health_check(self):
            return None

        def fetch_benchmark_logs(self, *_args, **_kwargs):
            return {
                "benchmark_id": "bench-1",
                "tasks": [
                    {"task_id": "task-1", "status": "IN_PROGRESS"},
                    {"task_id": "task-2", "status": "EVALUATING"},
                ],
            }

    monkeypatch.setattr("valkyrie.cli.main.TrackerService", lambda: FakeTracker())
    monkeypatch.setattr("valkyrie.cli.main.check_tracker_service_health", lambda _tracker: True)
    monkeypatch.setattr("valkyrie.cli.main.sys.stdin.isatty", lambda: False)

    run_id = uuid4()
    result = CliRunner().invoke(cli, ["run", "logs", str(run_id)])

    assert result.exit_code == 0
    assert "task-1" in result.output
    assert "IN_PROGRESS" in result.output
    assert "task-2" in result.output
    assert "EVALUATING" in result.output
    assert f"valkyrie run logs {run_id} --task-id task-1 --follow" in result.output


def test_run_logs_prints_selected_task_events(monkeypatch):
    """The logs command should print events for a selected task.

    Test cases:
    - Fetches logs for the requested task ID
    - Prints event messages without requiring follow mode
    """
    observed: dict[str, object] = {}

    class FakeTracker:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def health_check(self):
            return None

        def fetch_benchmark_logs(self, run_id, **kwargs):
            observed["run_id"] = run_id
            observed.update(kwargs)
            return {
                "benchmark_id": str(run_id),
                "task_id": "task-1",
                "events": [
                    {"timestamp": 123, "message": "hello\n", "log_stream_name": "task-1_abc"},
                    {"timestamp": 124, "message": "world", "log_stream_name": "task-1_abc"},
                ],
                "next_token": "next",
            }

    monkeypatch.setattr("valkyrie.cli.main.TrackerService", lambda: FakeTracker())
    monkeypatch.setattr("valkyrie.cli.main.check_tracker_service_health", lambda _tracker: True)

    run_id = uuid4()
    result = CliRunner().invoke(cli, ["run", "logs", str(run_id), "--task-id", "task-1", "--limit", "25"])

    assert result.exit_code == 0
    assert observed == {"run_id": run_id, "task_id": "task-1", "next_token": None, "limit": 25}
    assert "hello" in result.output
    assert "world" in result.output


def test_run_logs_opens_interactive_picker_for_tty(monkeypatch):
    """The logs command should open the curses picker when run from a TTY.

    Test cases:
    - Task list is fetched before opening the picker
    - curses.wrapper receives the tracker, run ID, tasks, interval, and limit
    """
    observed: dict[str, object] = {}

    class FakeTracker:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def health_check(self):
            return None

        def fetch_benchmark_logs(self, *_args, **_kwargs):
            return {
                "benchmark_id": "bench-1",
                "tasks": [{"task_id": "task-1", "status": "IN_PROGRESS"}],
            }

    def fake_wrapper(func, tracker, run_id, tasks, **kwargs):
        observed["func"] = func.__name__
        observed["tracker"] = tracker
        observed["run_id"] = run_id
        observed["tasks"] = tasks
        observed.update(kwargs)

    monkeypatch.setattr("valkyrie.cli.main.TrackerService", lambda: FakeTracker())
    monkeypatch.setattr("valkyrie.cli.main.check_tracker_service_health", lambda _tracker: True)
    monkeypatch.setattr("valkyrie.cli.main._stdin_is_interactive", lambda: True)
    monkeypatch.setattr("valkyrie.cli.main.curses.wrapper", fake_wrapper)

    run_id = uuid4()
    result = CliRunner().invoke(cli, ["run", "logs", str(run_id), "--interval", "0.5", "--limit", "25"])

    assert result.exit_code == 0
    assert observed["func"] == "_interactive_log_view"
    assert observed["run_id"] == run_id
    assert observed["tasks"] == [{"task_id": "task-1", "status": "IN_PROGRESS"}]
    assert observed["interval"] == 0.5
    assert observed["limit"] == 25


def test_run_logs_reports_when_no_tasks_have_logs(monkeypatch):
    """The logs command should explain empty filtered task lists.

    Test cases:
    - The tracker can return no log-ready tasks for a run
    - The user-facing message distinguishes this from a missing run
    """

    class FakeTracker:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def health_check(self):
            return None

        def fetch_benchmark_logs(self, *_args, **_kwargs):
            return {"benchmark_id": "bench-1", "tasks": []}

    monkeypatch.setattr("valkyrie.cli.main.TrackerService", lambda: FakeTracker())
    monkeypatch.setattr("valkyrie.cli.main.check_tracker_service_health", lambda _tracker: True)
    monkeypatch.setattr("valkyrie.cli.main._stdin_is_interactive", lambda: False)

    result = CliRunner().invoke(cli, ["run", "logs", str(uuid4())])

    assert result.exit_code == 0
    assert "No tasks have logs yet" in result.output


def test_task_picker_draws_clear_header_and_footer():
    """The interactive picker should explain the screen without cryptic wording.

    Test cases:
    - Header names the run log task picker
    - Footer shows navigation keys separately from the task table
    """

    class FakeScreen:
        def __init__(self):
            self.text: list[str] = []
            self.calls: list[tuple[int, int, str]] = []

        def getmaxyx(self):
            return (20, 100)

        def erase(self):
            pass

        def addnstr(self, *_args):
            self.calls.append((int(_args[0]), int(_args[1]), str(_args[2])))
            self.text.append(str(_args[2]))

        def refresh(self):
            pass

    screen = FakeScreen()

    main._draw_task_picker(
        screen,
        [{"task_id": "task-1", "status": "IN_PROGRESS"}],
        selected=0,
    )

    rendered = "\n".join(screen.text)
    assert "Run Logs" in rendered
    assert "Only tasks with logs are shown" in rendered
    assert "Enter: open logs" in rendered
    assert "Esc: exit" in rendered
    assert "Mouse wheel: scroll logs" in rendered
    status_cells = [(row, col) for row, col, text in screen.calls if text == "IN_PROGRESS"]
    assert status_cells == [(4, 2)]
    task_cells = [(row, col) for row, col, text in screen.calls if text == "task-1"]
    assert task_cells == [(4, 16)]
    assert any(text == ">" for _row, _col, text in screen.calls)


def test_log_view_supports_scroll_offset():
    """The live log view should be able to render older lines.

    Test cases:
    - Default view stays pinned to the newest lines
    - A positive scroll offset renders older log lines instead
    """

    class FakeScreen:
        def __init__(self):
            self.text: list[str] = []

        def getmaxyx(self):
            return (6, 100)

        def erase(self):
            self.text.clear()

        def addnstr(self, *_args):
            self.text.append(str(_args[2]))

        def refresh(self):
            pass

    screen = FakeScreen()
    lines = [f"line-{index}" for index in range(1, 8)]

    main._draw_log_view(screen, "task-1", lines, scroll_offset=0)
    newest_render = "\n".join(screen.text)

    main._draw_log_view(screen, "task-1", lines, scroll_offset=2)
    older_render = "\n".join(screen.text)

    assert "line-7" in newest_render
    assert "line-7" not in older_render
    assert "line-5" in older_render


def test_mouse_wheel_updates_log_scroll_offset(monkeypatch):
    """Mouse wheel events should map to log scroll changes.

    Test cases:
    - Wheel up scrolls toward older lines
    - Wheel down scrolls back toward the live tail
    """
    monkeypatch.setattr(main.curses, "BUTTON4_PRESSED", 1, raising=False)
    monkeypatch.setattr(main.curses, "BUTTON5_PRESSED", 2, raising=False)
    monkeypatch.setattr(main.curses, "BUTTON4_RELEASED", 4, raising=False)

    assert main._log_scroll_offset_after_mouse(0, line_count=20, view_height=5, button_state=1) == 3
    assert main._log_scroll_offset_after_mouse(3, line_count=20, view_height=5, button_state=2) == 0
    assert main._log_scroll_offset_after_mouse(3, line_count=20, view_height=5, button_state=4) == 0


def test_keyboard_scroll_offset_stays_inside_renderable_range():
    """Keyboard scrolling should not overshoot the visible log range.

    Test cases:
    - Scrolling toward older logs caps at the renderable maximum
    - Scrolling back down immediately moves toward the live tail
    """

    assert main._log_scroll_offset_after_keyboard(0, line_count=20, view_height=5, delta=99) == 15
    assert main._log_scroll_offset_after_keyboard(15, line_count=20, view_height=5, delta=-1) == 14


def test_missing_mouse_event_keeps_log_scroll_offset(monkeypatch):
    """Terminals can report KEY_MOUSE without a queued mouse event.

    Test cases:
    - getmouse ERR does not crash the log view
    - Existing scroll position is preserved when the mouse event cannot be read
    """
    monkeypatch.setattr(main.curses, "getmouse", lambda: (_ for _ in ()).throw(main.curses.error))

    assert main._log_scroll_offset_after_curses_mouse(4, line_count=20, view_height=5) == 4


def test_sgr_mouse_escape_sequence_updates_log_scroll_offset():
    """Raw terminal mouse sequences should update log scroll.

    Test cases:
    - SGR wheel up scrolls toward older lines
    - SGR wheel down scrolls back toward the live tail
    """

    assert main._log_scroll_offset_after_escape_sequence("[<64;1;1M", 0, line_count=20, view_height=5) == 3
    assert main._log_scroll_offset_after_escape_sequence("[<65;1;1M", 3, line_count=20, view_height=5) == 0


def test_x10_mouse_escape_sequence_updates_log_scroll_offset():
    """Legacy terminal mouse sequences should update log scroll.

    Test cases:
    - X10 wheel up scrolls toward older lines
    - X10 wheel down scrolls back toward the live tail
    """

    assert main._log_scroll_offset_after_escape_sequence("[M`!!", 0, line_count=20, view_height=5) == 3
    assert main._log_scroll_offset_after_escape_sequence("[Ma!!", 3, line_count=20, view_height=5) == 0


def test_mouse_reporting_sequences_enable_and_disable_sgr_mode():
    """The curses view should request terminal mouse reporting.

    Test cases:
    - Enable sequence turns on basic mouse reporting
    - Disable sequence turns off the same mode when leaving curses
    """

    assert "\x1b[?1000h" in main._mouse_reporting_sequence(enabled=True)
    assert "\x1b[?1006h" in main._mouse_reporting_sequence(enabled=True)
    assert "\x1b[?1000l" in main._mouse_reporting_sequence(enabled=False)
    assert "\x1b[?1006l" in main._mouse_reporting_sequence(enabled=False)


def test_escape_sequence_does_not_exit_log_view():
    """Esc should exit only when it is not part of a terminal sequence.

    Test cases:
    - Standalone Esc exits the log view
    - Esc followed by more bytes is treated as a terminal/mouse sequence prefix
    """

    class FakeScreen:
        def __init__(self, keys: list[int]):
            self.keys = keys
            self.timeouts: list[int] = []

        def timeout(self, delay: int):
            self.timeouts.append(delay)

        def getch(self):
            if not self.keys:
                return -1
            return self.keys.pop(0)

    assert main._escape_key_should_exit(FakeScreen([])) is True
    assert main._escape_key_should_exit(FakeScreen([ord("[")])) is False
