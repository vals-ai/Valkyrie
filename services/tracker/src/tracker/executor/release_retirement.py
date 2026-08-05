"""Automatic retirement for drained executor releases."""

from __future__ import annotations

import logging
from threading import Event, Thread

from sqlmodel import Session

from tracker.database.session import engine
from tracker.executor.release_control import retire_drained_releases

_RECONCILIATION_INTERVAL_SECONDS = 60.0
_SHUTDOWN_TIMEOUT_SECONDS = 5.0
_logger = logging.getLogger(__name__)


def retire_drained_releases_once() -> list[str]:
    """Run one atomic retirement pass in a fresh database session."""
    with Session(engine) as session:
        try:
            retired_ids = retire_drained_releases(session)
            session.commit()
        except Exception:
            session.rollback()
            raise
    return retired_ids


def run_release_retirement_loop(
    stop_event: Event,
    *,
    interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
) -> None:
    """Reconcile immediately and keep retrying until shutdown."""
    while not stop_event.is_set():
        try:
            retire_drained_releases_once()
        except Exception:
            _logger.exception(
                "executor_release_retirement_failed",
                extra={"event": "automatic_retirement_failed"},
            )
        stop_event.wait(interval_seconds)


class AutomaticReleaseRetirement:
    """Own the Tracker process's bounded retirement thread lifecycle."""

    def __init__(
        self,
        *,
        interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
        shutdown_timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._stop_event = Event()
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread = Thread(
            target=run_release_retirement_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="executor-release-retirement",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(self._shutdown_timeout_seconds)
        if self._thread.is_alive():
            _logger.error(
                "executor_release_retirement_shutdown_timeout",
                extra={"event": "automatic_retirement_shutdown_timeout"},
            )
