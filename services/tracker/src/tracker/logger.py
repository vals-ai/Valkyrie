"""
Basic logger with some formatted colors
"""

import logging
import sys

RESET = "\033[0m"
COLORS = {
    "DEBUG": "\033[36m",  # Cyan
    "INFO": "\033[32m",  # Green
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",  # Red
    "CRITICAL": "\033[41m\033[97m",  # White on Red Background
}


class ColorFormatter(logging.Formatter):
    """
    Custom formatter with colored level names and clean output.
    Example:
        [INFO] main: Something happened!
    """

    def format(self, record: logging.LogRecord) -> str:
        level_color = COLORS.get(record.levelname, "")
        level = f"{level_color}{record.levelname}{RESET}"

        # Align level names (DEBUG/INFO/WARNING/ERROR)
        padded_level = f"{level:<20}"

        # Optional: Show logger name
        logger_name = f"{record.name}"

        message = super().format(record)

        return f"[{padded_level}] {logger_name}: {message}"


def get_logger(name: str, stream: bool = False) -> logging.Logger:
    """Get a configured logger instance with pretty colors."""
    logger = logging.getLogger(name if not stream else f"{name}.stream")
    logger.propagate = False

    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler(sys.stdout)

        if stream:
            console_handler.terminator = ""

        console_handler.setLevel(logging.INFO)

        # Attach formatter
        console_handler.setFormatter(ColorFormatter("%(message)s"))

        logger.addHandler(console_handler)

    return logger
