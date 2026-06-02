"""Structured logging configuration for the tracker service."""

import logging
import logging.config
import os


class DevFormatter(logging.Formatter):
    """Colored human-readable formatter that includes context fields."""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[41m\033[97m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        level = f"{color}{record.levelname:<8}{self.RESET}"
        context_parts: list[str] = []
        for field in ("request_id", "benchmark_id", "task_id"):
            value = getattr(record, field, "")
            if value:
                context_parts.append(f"{field}={value}")
        context = " ".join(context_parts)
        context_str = f" [{context}]" if context else ""
        message = super().format(record)
        return f"{level} {record.name}:{context_str} {message}"


_FORMATTERS = {
    "production": {
        "()": "pythonjsonlogger.json.JsonFormatter",
        "fmt": (
            "%(asctime)s %(levelname)s %(name)s %(module)s %(funcName)s %(message)s "
            "%(request_id)s %(benchmark_id)s %(task_id)s"
        ),
        "rename_fields": {
            "asctime": "timestamp",
            "levelname": "level",
            "name": "logger",
        },
    },
    "development": {
        "()": "tracker.logging.config.DevFormatter",
        "fmt": "%(message)s",
    },
}


def configure_logging() -> None:
    """Configure structured logging. Must be called before any log output."""
    environment = os.getenv("ENVIRONMENT", "development")
    log_level = os.getenv("LOG_LEVEL", "INFO")

    if environment not in _FORMATTERS:
        raise ValueError(f"Unknown ENVIRONMENT={environment!r}. Must be one of: {', '.join(_FORMATTERS)}")

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {
                "context": {
                    "()": "tracker.logging.context.ContextFilter",
                },
            },
            "formatters": {
                "default": _FORMATTERS[environment],
            },
            "handlers": {
                "console": {
                    "level": "DEBUG",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "default",
                    "filters": ["context"],
                },
            },
            # Sentry's LoggingIntegration (enabled in init_sentry with sentry_logs_level=INFO)
            # attaches to the root logger and ships records to Sentry Logs without needing a
            # dictConfig handler entry.
            "root": {"handlers": ["console"], "level": log_level},
            "loggers": {
                "tracker": {"handlers": ["console"], "level": log_level, "propagate": False},
                "uvicorn": {"handlers": ["console"], "level": "INFO", "propagate": False},
                "uvicorn.access": {"handlers": ["console"], "level": "INFO", "propagate": False},
                "uvicorn.error": {"handlers": ["console"], "level": "INFO", "propagate": False},
                "taskiq": {"handlers": ["console"], "level": log_level, "propagate": False},
            },
        }
    )
