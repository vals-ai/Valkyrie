"""Unit tests for tracker logging configuration and context.

Run: uv run pytest tests/unit/logging/test_logging.py
"""

import json
import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tracker.logging import (
    ContextFilter,
    DevFormatter,
    benchmark_id_var,
    configure_logging,
    request_id_var,
    task_id_var,
)


class ContextLogRecord(logging.LogRecord):
    """Log record fields added by the tracker context filter."""

    request_id: str
    benchmark_id: str
    task_id: str


class TestContextFilter:
    """Log record enrichment from request context."""

    def test_context_filter_injects_vars(self) -> None:
        """ContextFilter copies ContextVar values onto log records."""
        f = ContextFilter()
        record = ContextLogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=(),
            exc_info=None,
        )

        # Default values (empty string)
        f.filter(record)
        assert record.request_id == ""
        assert record.benchmark_id == ""
        assert record.task_id == ""

    def test_context_filter_picks_up_set_values(self) -> None:
        """ContextFilter reads values that were set on ContextVars."""
        f = ContextFilter()
        record = ContextLogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=(),
            exc_info=None,
        )

        token_r = request_id_var.set("req-123")
        token_b = benchmark_id_var.set("bench-456")
        token_t = task_id_var.set("task-789")
        try:
            f.filter(record)
            assert record.request_id == "req-123"
            assert record.benchmark_id == "bench-456"
            assert record.task_id == "task-789"
        finally:
            request_id_var.reset(token_r)
            benchmark_id_var.reset(token_b)
            task_id_var.reset(token_t)


class TestConfigureLogging:
    """Logging configuration across deployment environments."""

    def test_configure_logging_development_format(self, capfd: pytest.CaptureFixture[str]) -> None:
        """Development mode produces colored human-readable output."""
        with patch.dict(os.environ, {"ENVIRONMENT": "development", "LOG_LEVEL": "INFO"}):
            configure_logging()

        logger = logging.getLogger("tracker.test_dev")
        logger.info("hello dev")
        captured = capfd.readouterr()
        assert "hello dev" in captured.out
        assert "tracker.test_dev" in captured.out

    def test_configure_logging_production_json(self, capfd: pytest.CaptureFixture[str]) -> None:
        """Production mode produces JSON output with renamed fields."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production", "LOG_LEVEL": "INFO"}):
            configure_logging()

        logger = logging.getLogger("tracker.test_prod")
        logger.info("hello prod")
        captured = capfd.readouterr()
        line = json.loads(captured.out.strip())
        assert line["message"] == "hello prod"
        assert "timestamp" in line
        assert "level" in line
        assert "request_id" in line

    def test_configure_logging_dev_stage_json(self, capfd: pytest.CaptureFixture[str]) -> None:
        """Deployed dev stage uses structured logs."""
        with patch.dict(os.environ, {"ENVIRONMENT": "dev", "LOG_LEVEL": "INFO"}):
            configure_logging()

        logger = logging.getLogger("tracker.test_deployed_dev")
        logger.info("hello deployed dev")
        captured = capfd.readouterr()
        line = json.loads(captured.out.strip())
        assert line["message"] == "hello deployed dev"
        assert "timestamp" in line
        assert "level" in line

    def test_configure_logging_includes_context_vars(self, capfd: pytest.CaptureFixture[str]) -> None:
        """Context variables appear in structured JSON output."""
        with patch.dict(os.environ, {"ENVIRONMENT": "production", "LOG_LEVEL": "INFO"}):
            configure_logging()

        token = request_id_var.set("req-abc")
        try:
            logger = logging.getLogger("tracker.test_ctx")
            logger.info("with context")
            captured = capfd.readouterr()
            line = json.loads(captured.out.strip())
            assert line["request_id"] == "req-abc"
        finally:
            request_id_var.reset(token)

    def test_configure_logging_rejects_unknown_environment(self) -> None:
        """Unknown ENVIRONMENT values raise ValueError."""
        with patch.dict(os.environ, {"ENVIRONMENT": "staging"}):
            try:
                configure_logging()
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "staging" in str(e)


class TestDevelopmentFormatter:
    """Development log formatting with optional context."""

    def test_dev_formatter_shows_context(self) -> None:
        """DevFormatter includes non-empty context fields."""
        fmt = DevFormatter("%(message)s")
        record = ContextLogRecord(
            name="tracker.test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test msg",
            args=(),
            exc_info=None,
        )
        record.request_id = "req-1"
        record.benchmark_id = ""
        record.task_id = "task-2"
        output = fmt.format(record)
        assert "request_id=req-1" in output
        assert "task_id=task-2" in output

        # Empty context fields are omitted.
        assert "benchmark_id" not in output

    def test_dev_formatter_no_context(self) -> None:
        """DevFormatter works cleanly with no context set."""
        fmt = DevFormatter("%(message)s")
        record = ContextLogRecord(
            name="tracker.test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="no ctx",
            args=(),
            exc_info=None,
        )
        record.request_id = ""
        record.benchmark_id = ""
        record.task_id = ""
        output = fmt.format(record)
        assert "no ctx" in output

        # Empty context omits the context bracket.
        assert " [" not in output


class TestLoggingContextMiddleware:
    """Benchmark logging context setup and cleanup."""

    @pytest.mark.anyio
    async def test_logging_context_middleware_pre_execute(self) -> None:
        """Taskiq middleware binds benchmark_id and request_id from message."""
        from tracker.middleware import LoggingContextMiddleware

        mw = LoggingContextMiddleware()

        message = MagicMock()
        message.kwargs = {"benchmark_id_str": "bench-123"}
        message.labels = {"request_id": "req-456"}

        result = await mw.pre_execute(message)

        assert benchmark_id_var.get() == "bench-123"
        assert request_id_var.get() == "req-456"
        assert result is message

    @pytest.mark.anyio
    async def test_logging_context_middleware_post_execute_clears(self) -> None:
        """post_execute clears all context vars."""
        from tracker.middleware import LoggingContextMiddleware

        mw = LoggingContextMiddleware()
        benchmark_id_var.set("leftover")
        request_id_var.set("leftover")
        task_id_var.set("leftover")

        await mw.post_execute(MagicMock(), MagicMock())

        assert benchmark_id_var.get() == ""
        assert request_id_var.get() == ""
        assert task_id_var.get() == ""

    @pytest.mark.anyio
    async def test_logging_context_middleware_on_error_clears(self) -> None:
        """on_error clears context vars so failed jobs don't leak."""
        from tracker.middleware import LoggingContextMiddleware

        mw = LoggingContextMiddleware()
        benchmark_id_var.set("leaked")

        await mw.on_error(MagicMock(), MagicMock(), RuntimeError("boom"))

        assert benchmark_id_var.get() == ""


@pytest.mark.anyio
async def test_request_context_middleware_sets_request_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Middleware generates request_id and returns it in response header."""
    from main import app

    monkeypatch.setattr("main.check_database_connection", lambda: True)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    request_id = response.headers.get("x-request-id")
    assert request_id is not None

    # Middleware uses the first 12 characters of a UUID.
    assert len(request_id) == 12
