"""Unit tests for tracker notification behavior.

Run: uv run pytest tests/unit/test_notifications.py
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest

import tracker.notifications as notifications_module
from tracker.database.models import BenchmarkStatus
from tracker.exceptions import SecretsError
from tracker.notifications import NotificationContext, SlackNotifier
from tracker.types import AWSCredentials

_build_progress_message = getattr(notifications_module, "_build_progress_message")
_build_terminal_message = getattr(notifications_module, "_build_terminal_message")


def _make_context(**overrides: object) -> NotificationContext:
    """Helper to build a NotificationContext with sensible defaults."""
    context = NotificationContext(
        benchmark_name="swebench",
        agent_name="claude_code",
        benchmark_id=uuid4(),
        started_at=datetime.now(ZoneInfo("UTC")),
        total_tasks=100,
        finished_tasks=0,
        model="anthropic/claude-sonnet-4-20250514",
    )
    return NotificationContext.model_validate({**context.model_dump(), **overrides})


def _make_notifier(
    aws_credentials: AWSCredentials,
    *,
    secret_name: str = "test/webhook",
    intervals: tuple[int, ...] = (50,),
) -> SlackNotifier:
    """Build a notifier with deterministic credentials and thresholds."""
    return SlackNotifier(
        secret_name=secret_name,
        aws=aws_credentials,
        intervals=list(intervals),
    )


@pytest.fixture
def notifier(aws_credentials: AWSCredentials) -> SlackNotifier:
    """Provide a notifier with the progress thresholds used by behavior tests."""
    return _make_notifier(aws_credentials, intervals=(25, 50, 100))


@pytest.fixture
def mock_send(notifier: SlackNotifier, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Capture notification delivery at the outbound webhook boundary."""
    send = AsyncMock()
    monkeypatch.setattr(notifier, "_send_webhook", send)

    return send


@pytest.fixture
def mock_http_client(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Provide the async HTTP client used by webhook delivery tests."""
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = False
    monkeypatch.setattr(notifications_module.httpx, "AsyncClient", MagicMock(return_value=client))

    return client


class TestSlackNotifierThresholds:
    """Progress notifications at configured completion thresholds."""

    async def test_fires_when_threshold_crossed(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """Notification fires when progress crosses a defined threshold."""
        await notifier.check_and_notify(_make_context(finished_tasks=25))

        mock_send.assert_called_once()

    async def test_does_not_fire_below_threshold(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """No notification when progress hasn't reached any threshold."""
        await notifier.check_and_notify(_make_context(finished_tasks=24))

        mock_send.assert_not_called()

    async def test_does_not_refire_same_threshold(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """Same threshold should not fire twice."""
        context = _make_context(finished_tasks=25)

        await notifier.check_and_notify(context)
        await notifier.check_and_notify(
            _make_context(
                benchmark_id=context.benchmark_id,
                started_at=context.started_at,
                finished_tasks=26,
            )
        )

        assert mock_send.call_count == 1

    async def test_fires_multiple_thresholds_at_once(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """If progress jumps past multiple thresholds, all crossed thresholds fire in order."""
        await notifier.check_and_notify(_make_context(finished_tasks=60))

        assert mock_send.call_count == 2

    async def test_zero_tasks_does_not_crash(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """Edge case: 0 total tasks should not raise."""
        await notifier.check_and_notify(_make_context(total_tasks=0, finished_tasks=0))

        mock_send.assert_not_called()


class TestSlackNotifierTerminal:
    """Terminal benchmark notifications for finished, error, and stopped runs."""

    async def test_terminal_finished_includes_score(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """Finished notification includes final score."""
        await notifier.send_terminal_notification(
            _make_context(finished_tasks=100),
            status=BenchmarkStatus.FINISHED,
            final_score=0.42,
        )

        mock_send.assert_called_once()
        message = mock_send.call_args[0][0]
        assert "Final Score: 0.42" in message
        assert "Benchmark Complete" in message

    async def test_terminal_error_includes_message(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """Error notification includes error message."""
        await notifier.send_terminal_notification(
            _make_context(finished_tasks=50),
            status=BenchmarkStatus.ERROR,
            error_message="Connection timeout",
        )

        mock_send.assert_called_once()
        message = mock_send.call_args[0][0]
        assert "Connection timeout" in message
        assert "Benchmark Error" in message

    async def test_terminal_stopped(self, notifier: SlackNotifier, mock_send: AsyncMock) -> None:
        """Stopped notification has correct header."""
        await notifier.send_terminal_notification(
            _make_context(finished_tasks=30),
            status=BenchmarkStatus.STOPPED,
        )

        mock_send.assert_called_once()
        message = mock_send.call_args[0][0]
        assert "Benchmark Stopped" in message


class TestMessageContent:
    """Progress and terminal notification message fields."""

    def test_progress_message_contains_all_fields(self) -> None:
        context = _make_context(finished_tasks=25, model="anthropic/claude-sonnet-4-20250514")
        message = _build_progress_message(context, percent=25)
        assert "Benchmark Update" in message
        assert context.benchmark_name in message
        assert context.agent_name in message
        assert str(context.benchmark_id) in message
        assert "Model: anthropic/claude-sonnet-4-20250514" in message
        assert "25% (25/100 tasks)" in message

    def test_progress_message_shows_na_when_model_is_none(self) -> None:
        context = _make_context(finished_tasks=25, model=None)
        message = _build_progress_message(context, percent=25)
        assert "Model: N/A" in message

    def test_terminal_message_contains_all_fields(self) -> None:
        context = _make_context(finished_tasks=100, model="anthropic/claude-sonnet-4-20250514")
        message = _build_terminal_message(context, status=BenchmarkStatus.FINISHED, final_score=0.42)
        assert "Benchmark Complete" in message
        assert context.benchmark_name in message
        assert context.agent_name in message
        assert str(context.benchmark_id) in message
        assert "Model: anthropic/claude-sonnet-4-20250514" in message
        assert "100% (100/100 tasks)" in message
        assert "Final Score: 0.42" in message

    def test_terminal_message_shows_na_when_model_is_none(self) -> None:
        context = _make_context(finished_tasks=100, model=None)
        message = _build_terminal_message(context, status=BenchmarkStatus.FINISHED, final_score=0.42)
        assert "Model: N/A" in message


class TestSlackNotifierFireAndForget:
    """Webhook failures that must not interrupt benchmark work."""

    @pytest.mark.parametrize(
        "webhook_error",
        [httpx.TimeoutException("timeout"), httpx.ConnectError("refused")],
        ids=["timeout", "connection-error"],
    )
    async def test_webhook_delivery_error_does_not_raise(
        self,
        aws_credentials: AWSCredentials,
        monkeypatch: pytest.MonkeyPatch,
        mock_http_client: AsyncMock,
        webhook_error: httpx.HTTPError,
    ) -> None:
        """Webhook transport failures must not interrupt benchmark work.

        Test cases:
        - A request timeout is caught after the threshold is crossed.
        - A connection failure is caught after the threshold is crossed.
        """

        def fetch_secret(_secret_name: str, _aws: AWSCredentials) -> str:
            return "https://hooks.slack.com/test"

        notifier = _make_notifier(aws_credentials)
        mock_http_client.post = AsyncMock(side_effect=webhook_error)
        monkeypatch.setattr(notifications_module, "fetch_aws_secret", fetch_secret)

        await notifier.check_and_notify(_make_context(finished_tasks=50))


class TestSlackNotifierSecretResolution:
    """Webhook secret resolution and invalid-secret handling."""

    async def test_resolves_secret_before_sending(
        self,
        aws_credentials: AWSCredentials,
        monkeypatch: pytest.MonkeyPatch,
        mock_http_client: AsyncMock,
    ) -> None:
        """SlackNotifier calls fetch_aws_secret to resolve the webhook URL."""
        notifier = _make_notifier(aws_credentials, secret_name="my/webhook/secret")
        mock_fetch = MagicMock(return_value="https://hooks.slack.com/resolved")
        mock_http_client.post = AsyncMock(return_value=MagicMock(status_code=200))
        monkeypatch.setattr(notifications_module, "fetch_aws_secret", mock_fetch)

        await notifier.check_and_notify(_make_context(finished_tasks=50))

        mock_fetch.assert_called_once_with("my/webhook/secret", aws_credentials)
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        assert call_args[0][0] == "https://hooks.slack.com/resolved"

    @pytest.mark.parametrize(
        "secret_result",
        [SecretsError("not found"), {"url": "https://hooks.slack.com/test"}],
        ids=["fetch-error", "non-string-secret"],
    )
    async def test_skips_notification_when_secret_is_unusable(
        self,
        aws_credentials: AWSCredentials,
        monkeypatch: pytest.MonkeyPatch,
        mock_http_client: AsyncMock,
        secret_result: Exception | dict[str, str],
    ) -> None:
        """Skip delivery when the configured secret cannot provide a webhook URL.

        Test cases:
        - Secret retrieval raises an expected service error.
        - Secret retrieval returns structured data instead of a URL string.
        """
        notifier = _make_notifier(aws_credentials, secret_name="invalid/secret")
        fetch_secret = (
            MagicMock(side_effect=secret_result)
            if isinstance(secret_result, Exception)
            else MagicMock(return_value=secret_result)
        )
        monkeypatch.setattr(notifications_module, "fetch_aws_secret", fetch_secret)

        await notifier.check_and_notify(_make_context(finished_tasks=50))

        mock_http_client.post.assert_not_called()
