"""Unit tests for tracker logging configuration and context.

Run: uv run pytest tests/unit/logging/test_logging.py
"""

import json
import logging
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from tracker.logging import (
    ContextFilter,
    DevFormatter,
    benchmark_id_var,
    configure_logging,
    request_id_var,
    task_id_var,
)
from tracker.middleware import LoggingContextMiddleware


class ContextLogRecord(logging.LogRecord):
    """Log record fields added by the tracker context filter."""

    request_id: str
    benchmark_id: str
    task_id: str


def _log_record(
    *,
    name: str = "test",
    level: int = logging.INFO,
    message: str = "hello",
) -> ContextLogRecord:
    return ContextLogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )


def _configure_test_logging(monkeypatch: pytest.MonkeyPatch, environment: str) -> None:
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    configure_logging()


class TestContextFilter:
    """Log record enrichment from request context."""

    def test_context_filter_injects_current_values(self) -> None:
        """ContextFilter copies empty and populated ContextVar values onto log records.

        Test cases:
        - Unset context variables produce empty record fields.
        - Set context variables are copied onto the record.
        """
        context_filter = ContextFilter()
        record = _log_record()

        context_filter.filter(record)

        assert record.request_id == ""
        assert record.benchmark_id == ""
        assert record.task_id == ""

        request_token = request_id_var.set("req-123")
        benchmark_token = benchmark_id_var.set("bench-456")
        task_token = task_id_var.set("task-789")
        try:
            context_filter.filter(record)

            assert record.request_id == "req-123"
            assert record.benchmark_id == "bench-456"
            assert record.task_id == "task-789"
        finally:
            request_id_var.reset(request_token)
            benchmark_id_var.reset(benchmark_token)
            task_id_var.reset(task_token)


class TestConfigureLogging:
    """Logging configuration across deployment environments."""

    @pytest.mark.parametrize(
        ("environment", "logger_name", "message", "structured", "expects_request_id"),
        [
            ("development", "tracker.test_dev", "hello dev", False, False),
            ("release-test", "tracker.test_release_test", "hello release-test", False, False),
            ("production", "tracker.test_prod", "hello prod", True, True),
            ("dev", "tracker.test_deployed_dev", "hello deployed dev", True, False),
        ],
    )
    def test_configure_logging_formats_environment(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
        environment: str,
        logger_name: str,
        message: str,
        structured: bool,
        expects_request_id: bool,
    ) -> None:
        """Each supported environment selects its expected human or JSON format."""
        _configure_test_logging(monkeypatch, environment)

        logging.getLogger(logger_name).info(message)
        output = capfd.readouterr().out

        if not structured:
            assert message in output
            assert logger_name in output
            return

        log_payload = json.loads(output.strip())
        assert log_payload["message"] == message
        assert "timestamp" in log_payload
        assert "level" in log_payload
        if expects_request_id:
            assert "request_id" in log_payload

    def test_configure_logging_includes_context_vars(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
    ) -> None:
        """Context variables appear in structured JSON output."""
        _configure_test_logging(monkeypatch, "production")

        token = request_id_var.set("req-abc")
        try:
            logging.getLogger("tracker.test_ctx").info("with context")

            log_payload = json.loads(capfd.readouterr().out.strip())
            assert log_payload["request_id"] == "req-abc"
        finally:
            request_id_var.reset(token)

    def test_configure_logging_rejects_unknown_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown ENVIRONMENT values raise ValueError."""
        monkeypatch.setenv("ENVIRONMENT", "staging")

        with pytest.raises(ValueError, match="staging"):
            configure_logging()


class TestDevelopmentFormatter:
    """Development log formatting with optional context."""

    def test_dev_formatter_handles_optional_context(self) -> None:
        """DevFormatter includes populated context and omits empty context.

        Test cases:
        - Populated request and task IDs appear without an empty benchmark ID.
        - A record without context omits the context bracket.
        """
        formatter = DevFormatter("%(message)s")
        context_record = _log_record(name="tracker.test", message="test msg")
        context_record.request_id = "req-1"
        context_record.benchmark_id = ""
        context_record.task_id = "task-2"

        context_output = formatter.format(context_record)

        assert "request_id=req-1" in context_output
        assert "task_id=task-2" in context_output
        assert "benchmark_id" not in context_output

        plain_record = _log_record(name="tracker.test", level=logging.WARNING, message="no ctx")
        plain_record.request_id = ""
        plain_record.benchmark_id = ""
        plain_record.task_id = ""

        plain_output = formatter.format(plain_record)

        assert "no ctx" in plain_output
        assert " [" not in plain_output


class TestLoggingContextMiddleware:
    """Benchmark logging context setup and cleanup."""

    async def test_logging_context_middleware_pre_execute(self) -> None:
        """Taskiq middleware binds benchmark_id and request_id from message."""
        middleware = LoggingContextMiddleware()

        message = MagicMock()
        message.kwargs = {"benchmark_id_str": "bench-123"}
        message.labels = {"request_id": "req-456"}

        result = await middleware.pre_execute(message)

        assert benchmark_id_var.get() == "bench-123"
        assert request_id_var.get() == "req-456"
        assert result is message

    async def test_logging_context_middleware_post_execute_clears(self) -> None:
        """post_execute clears all context vars."""
        middleware = LoggingContextMiddleware()
        benchmark_id_var.set("leftover")
        request_id_var.set("leftover")
        task_id_var.set("leftover")

        await middleware.post_execute(MagicMock(), MagicMock())

        assert benchmark_id_var.get() == ""
        assert request_id_var.get() == ""
        assert task_id_var.get() == ""

    async def test_logging_context_middleware_on_error_clears(self) -> None:
        """on_error clears context vars so failed jobs don't leak."""
        middleware = LoggingContextMiddleware()
        benchmark_id_var.set("leaked")

        await middleware.on_error(MagicMock(), MagicMock(), RuntimeError("boom"))

        assert benchmark_id_var.get() == ""


async def test_request_context_middleware_sets_request_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Middleware generates request_id and returns it in response header."""
    monkeypatch.setattr("main.check_database_connection", lambda: True)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    request_id = response.headers.get("x-request-id")
    assert request_id is not None

    # Middleware uses the first 12 characters of a UUID.
    assert len(request_id) == 12
