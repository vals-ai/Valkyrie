"""Context variables for structured logging correlation IDs."""

import contextvars
import logging

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")
benchmark_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("benchmark_id", default="")
task_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="")


class ContextFilter(logging.Filter):
    """Injects context variables into every log record for structured logging."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get("")  # type: ignore[attr-defined]
        record.benchmark_id = benchmark_id_var.get("")  # type: ignore[attr-defined]
        record.task_id = task_id_var.get("")  # type: ignore[attr-defined]
        return True
