"""Context variables for structured logging correlation IDs."""

import contextvars
import logging

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")
run_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="")
# Compatibility alias for existing log consumers and worker code.
benchmark_id_var = run_id_var
task_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="")


def get_context_tags() -> dict[str, str]:
    """Current request/run/task context vars with the legacy run-id alias."""
    return {
        "request_id": request_id_var.get(""),
        "run_id": run_id_var.get(""),
        "benchmark_id": benchmark_id_var.get(""),
        "task_id": task_id_var.get(""),
    }


class ContextFilter(logging.Filter):
    """Injects context variables into every log record for structured logging."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in get_context_tags().items():
            setattr(record, key, value)
        return True
