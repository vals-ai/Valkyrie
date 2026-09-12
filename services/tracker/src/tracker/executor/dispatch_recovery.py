"""Recover executor dispatches whose owner stopped renewing its lease."""

from __future__ import annotations

import logging
from threading import Event, Thread

from sqlmodel import Session

from tracker.database.session import engine
from tracker.executor.dispatch_control import reconcile_expired_dispatches

_RECONCILIATION_INTERVAL_SECONDS = 60.0
_SHUTDOWN_TIMEOUT_SECONDS = 5.0
_logger = logging.getLogger(__name__)


def reconcile_expired_dispatches_once() -> int:
    """Run one atomic lease-reconciliation pass in a fresh database session."""
    with Session(engine) as session:
        try:
            recovered_count = reconcile_expired_dispatches(session)
            session.commit()
        except Exception:
            session.rollback()
            raise
    return recovered_count


def run_dispatch_recovery_loop(
    stop_event: Event,
    *,
    interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
) -> None:
    """Reconcile immediately and keep retrying until shutdown."""
    while not stop_event.is_set():
        try:
            reconcile_expired_dispatches_once()
        except Exception:
            _logger.exception(
                "executor_dispatch_recovery_failed",
                extra={"event": "automatic_dispatch_recovery_failed"},
            )
        stop_event.wait(interval_seconds)


class AutomaticDispatchRecovery:
    """Own the Tracker process's bounded dispatch-recovery thread lifecycle."""

    def __init__(
        self,
        *,
        interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
        shutdown_timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._stop_event = Event()
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread = Thread(
            target=run_dispatch_recovery_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="executor-dispatch-recovery",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(self._shutdown_timeout_seconds)
        if self._thread.is_alive():
            _logger.error(
                "executor_dispatch_recovery_shutdown_timeout",
                extra={"event": "automatic_dispatch_recovery_shutdown_timeout"},
            )
