from datetime import datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest

from tracker.database.models import BenchmarkStatus
from tracker.notifications import NotificationContext, SlackNotifier


def _make_context(**overrides: object) -> NotificationContext:
    """Helper to build a NotificationContext with sensible defaults."""
    defaults = {
        "benchmark_name": "swebench",
        "agent_name": "claude_code",
        "benchmark_id": uuid4(),
        "started_at": datetime.now(ZoneInfo("UTC")),
        "total_tasks": 100,
        "finished_tasks": 0,
    }
    defaults.update(overrides)
    return NotificationContext(**defaults)


class TestSlackNotifierThresholds:
    @pytest.fixture
    def notifier(self) -> SlackNotifier:
        return SlackNotifier(
            webhook_url="https://hooks.slack.com/test",
            intervals=[25, 50, 100],
        )

    async def test_fires_when_threshold_crossed(self, notifier: SlackNotifier) -> None:
        """Notification fires when progress crosses a defined threshold."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.check_and_notify(_make_context(finished_tasks=25))
            mock_send.assert_called_once()

    async def test_does_not_fire_below_threshold(self, notifier: SlackNotifier) -> None:
        """No notification when progress hasn't reached any threshold."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.check_and_notify(_make_context(finished_tasks=24))
            mock_send.assert_not_called()

    async def test_does_not_refire_same_threshold(self, notifier: SlackNotifier) -> None:
        """Same threshold should not fire twice."""
        context = _make_context(finished_tasks=25)

        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.check_and_notify(context)
            await notifier.check_and_notify(
                _make_context(
                    benchmark_id=context.benchmark_id,
                    started_at=context.started_at,
                    finished_tasks=26,
                )
            )
            assert mock_send.call_count == 1

    async def test_fires_multiple_thresholds_at_once(self, notifier: SlackNotifier) -> None:
        """If progress jumps past multiple thresholds, all crossed thresholds fire in order."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.check_and_notify(_make_context(finished_tasks=60))
            # Should fire for 25 and 50 (both crossed), so 2 calls
            assert mock_send.call_count == 2

    async def test_zero_tasks_does_not_crash(self, notifier: SlackNotifier) -> None:
        """Edge case: 0 total tasks should not raise."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.check_and_notify(_make_context(total_tasks=0, finished_tasks=0))
            mock_send.assert_not_called()


class TestSlackNotifierTerminal:
    @pytest.fixture
    def notifier(self) -> SlackNotifier:
        return SlackNotifier(
            webhook_url="https://hooks.slack.com/test",
            intervals=[100],
        )

    async def test_terminal_finished_includes_score(self, notifier: SlackNotifier) -> None:
        """Finished notification includes final score."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.send_terminal_notification(
                _make_context(finished_tasks=100),
                status=BenchmarkStatus.FINISHED,
                final_score=0.42,
            )
            mock_send.assert_called_once()
            message = mock_send.call_args[0][0]
            assert "Final Score: 0.42" in message
            assert "Benchmark Complete" in message

    async def test_terminal_error_includes_message(self, notifier: SlackNotifier) -> None:
        """Error notification includes error message."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.send_terminal_notification(
                _make_context(finished_tasks=50),
                status=BenchmarkStatus.ERROR,
                error_message="Connection timeout",
            )
            mock_send.assert_called_once()
            message = mock_send.call_args[0][0]
            assert "Connection timeout" in message
            assert "Benchmark Error" in message

    async def test_terminal_stopped(self, notifier: SlackNotifier) -> None:
        """Stopped notification has correct header."""
        with patch.object(notifier, "_send_webhook", new_callable=AsyncMock) as mock_send:
            await notifier.send_terminal_notification(
                _make_context(finished_tasks=30),
                status=BenchmarkStatus.STOPPED,
            )
            mock_send.assert_called_once()
            message = mock_send.call_args[0][0]
            assert "Benchmark Stopped" in message


class TestSlackNotifierFireAndForget:
    async def test_webhook_timeout_does_not_raise(self) -> None:
        """Webhook timeout should be caught and logged, not raised."""
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/test",
            intervals=[50],
        )
        with patch("tracker.notifications.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            # Should not raise
            await notifier.check_and_notify(_make_context(finished_tasks=50))

    async def test_webhook_connection_error_does_not_raise(self) -> None:
        """Webhook connection error should be caught and logged, not raised."""
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/test",
            intervals=[50],
        )
        with patch("tracker.notifications.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            # Should not raise
            await notifier.check_and_notify(_make_context(finished_tasks=50))
