"""Run with `uv run pytest tests/unit/executor/test_dispatch_recovery.py`."""

from collections.abc import Callable
from threading import Event
from unittest.mock import Mock

from pytest import MonkeyPatch

from tracker.executor import dispatch_recovery


def test_recovery_loop_retries_after_a_failed_pass(monkeypatch: MonkeyPatch) -> None:
    stop_event = Event()
    attempts = 0

    def reconcile() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("database unavailable")
        stop_event.set()
        return 1

    logger = Mock()
    monkeypatch.setattr(dispatch_recovery, "reconcile_expired_dispatches_once", reconcile)
    monkeypatch.setattr(dispatch_recovery, "_logger", logger)

    dispatch_recovery.run_dispatch_recovery_loop(stop_event, interval_seconds=0)

    assert attempts == 2
    logger.exception.assert_called_once_with(
        "executor_dispatch_recovery_failed",
        extra={"event": "automatic_dispatch_recovery_failed"},
    )


def test_automatic_recovery_starts_and_stops_owned_thread(monkeypatch: MonkeyPatch) -> None:
    events: list[str] = []

    class FakeThread:
        def __init__(
            self,
            *,
            target: Callable[..., None],
            args: tuple[Event],
            kwargs: dict[str, float],
            name: str,
            daemon: bool,
        ) -> None:
            assert target is dispatch_recovery.run_dispatch_recovery_loop
            assert kwargs == {"interval_seconds": 60}
            assert name == "executor-dispatch-recovery"
            assert daemon
            self.stop_event = args[0]

        def start(self) -> None:
            events.append("started")

        def join(self, timeout: float) -> None:
            assert timeout == 5
            assert self.stop_event.is_set()
            events.append("joined")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(dispatch_recovery, "Thread", FakeThread)
    recovery = dispatch_recovery.AutomaticDispatchRecovery(interval_seconds=60)

    recovery.start()
    recovery.stop()

    assert events == ["started", "joined"]
