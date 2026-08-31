"""Slack webhook notifications for benchmark progress."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID

import httpx
from pydantic import BaseModel

from tracker.runtime.secrets import SecretStore
from tracker.database.models import BenchmarkStatus
from tracker.exceptions import SecretsError
from tracker.logging import get_logger

if TYPE_CHECKING:
    from tracker.database.models import Benchmark, Org
    from sqlmodel import Session

logger = get_logger(__name__)

WEBHOOK_TIMEOUT = 5.0


class NotificationContext(BaseModel):
    """Common fields for all notification messages."""

    model_config = {"frozen": True}

    benchmark_name: str
    agent_name: str
    benchmark_id: UUID
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    model: str | None = None

    @classmethod
    def from_benchmark(cls, benchmark_row: "Benchmark", session: "Session", org: "Org") -> NotificationContext:
        from tracker.utils import BenchmarkContext

        details = BenchmarkContext(benchmark_row, session, org).benchmark_details
        return cls(
            benchmark_name=benchmark_row.name,
            agent_name=benchmark_row.arguments.contract.name,
            benchmark_id=benchmark_row.id,
            started_at=benchmark_row.started_at,
            total_tasks=details.total_tasks,
            finished_tasks=details.finished_tasks,
            model=benchmark_row.arguments.contract.model,
        )


def _format_duration(started_at: datetime) -> str:
    """Format elapsed time as human-readable string."""
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    seconds = int((datetime.now(timezone.utc) - started_at).total_seconds())

    if seconds < 60:
        return f"{seconds}s"

    minutes, _ = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _build_progress_message(context: NotificationContext, percent: int) -> str:
    elapsed = _format_duration(context.started_at)
    lines = [
        f"*Benchmark Update* — {context.benchmark_name}",
        f"Agent: {context.agent_name} | Run: {context.benchmark_id}",
        f"Model: {context.model or 'N/A'}",
        f"Status: In Progress — {percent}% ({context.finished_tasks}/{context.total_tasks} tasks)",
        f"Elapsed: {elapsed}",
    ]
    return "\n".join(lines)


def _build_terminal_message(
    context: NotificationContext,
    status: BenchmarkStatus,
    final_score: float | None = None,
    error_message: str | None = None,
) -> str:
    duration = _format_duration(context.started_at)
    percent = int((context.finished_tasks / context.total_tasks) * 100) if context.total_tasks > 0 else 0

    status_labels = {
        BenchmarkStatus.FINISHED: "Benchmark Complete",
        BenchmarkStatus.ERROR: "Benchmark Error",
        BenchmarkStatus.STOPPED: "Benchmark Stopped",
    }
    header = status_labels.get(status, f"Benchmark {status.value}")

    lines = [
        f"*{header}* — {context.benchmark_name}",
        f"Agent: {context.agent_name} | Run: {context.benchmark_id}",
        f"Model: {context.model or 'N/A'}",
        f"Status: {status.value} — {percent}% ({context.finished_tasks}/{context.total_tasks} tasks)",
    ]

    if final_score is not None:
        lines.append(f"Final Score: {final_score}")
    if error_message:
        truncated = error_message[:200] + "..." if len(error_message) > 200 else error_message
        lines.append(f"Error: {truncated}")

    lines.append(f"Duration: {duration}")
    return "\n".join(lines)


class SlackNotifier:
    def __init__(self, secret_name: str, secret_store: SecretStore, intervals: list[int]):
        self._secret_name = secret_name
        self._secret_store = secret_store
        self._intervals = set(intervals)
        self._fired: set[int] = set()

    async def _send_webhook(self, text: str) -> None:
        """Fire and forget — exceptions are caught and logged, never raised."""
        try:
            secret_value = self._secret_store.get(self._secret_name)
            if not isinstance(secret_value, str):
                logger.warning(
                    f"Webhook secret '{self._secret_name}' returned a dict, expected a plain string URL. Skipping notification."
                )
                return

            async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as client:
                response = await client.post(
                    secret_value,
                    json={"text": text},
                )
                if response.status_code != 200:
                    logger.warning(f"Slack webhook returned {response.status_code}: {response.text[:200]}")
        except SecretsError as e:
            logger.warning(f"Failed to resolve webhook secret '{self._secret_name}': {e}")
        except Exception as e:
            logger.warning(f"Slack webhook failed: {e}")

    async def check_and_notify(self, context: NotificationContext) -> None:
        """Check if any un-fired threshold was crossed and send notifications."""
        if context.total_tasks == 0:
            return

        percent = int((context.finished_tasks / context.total_tasks) * 100)

        # Skip progress notifications when run is complete — terminal notification handles this
        if percent >= 100:
            return

        crossed = sorted(t for t in self._intervals if t <= percent and t not in self._fired)

        for threshold in crossed:
            self._fired.add(threshold)
            message = _build_progress_message(context, percent=threshold)
            await self._send_webhook(message)

    async def send_terminal_notification(
        self,
        context: NotificationContext,
        status: BenchmarkStatus,
        final_score: float | None = None,
        error_message: str | None = None,
    ) -> None:
        """Send notification for terminal states. Always fires regardless of intervals."""
        message = _build_terminal_message(
            context,
            status=status,
            final_score=final_score,
            error_message=error_message,
        )
        await self._send_webhook(message)
