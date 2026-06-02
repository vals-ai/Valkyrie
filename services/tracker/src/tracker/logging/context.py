"""Context variables for structured logging correlation IDs."""

import contextvars
import logging

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")
benchmark_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("benchmark_id", default="")
task_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="")


def get_context_tags() -> dict[str, str]:
    """Current request/benchmark/task context vars. Values may be empty strings if unset."""
    return {
        "request_id": request_id_var.get(""),
        "benchmark_id": benchmark_id_var.get(""),
        "task_id": task_id_var.get(""),
    }


class ContextFilter(logging.Filter):
    """Injects context variables into every log record for structured logging."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in get_context_tags().items():
            setattr(record, key, value)
        return True
