"""Compatibility shim — delegates to stdlib logging.

Existing callers import `get_logger` from here. After structured logging is
configured via `configure_logging()`, all loggers get JSON (prod) or colored
console (dev) output automatically through dictConfig.
"""

import logging


def get_logger(name: str, stream: bool = False) -> logging.Logger:
    """Get a configured logger instance."""
    return logging.getLogger(name if not stream else f"{name}.stream")
