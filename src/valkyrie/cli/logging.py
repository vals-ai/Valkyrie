"""Logging controls for the Valkyrie CLI."""

import logging
import os
import sys

CLI_LOGS_ENV_VAR = "VALKYRIE_CLI_LOGS"


def configure_cli_logging() -> None:
    """Disable library logs in CLI output unless explicitly enabled."""
    if os.environ.get(CLI_LOGS_ENV_VAR) == "true":
        console_handler = logging.getHandlerByName("console")
        if isinstance(console_handler, logging.StreamHandler):
            console_handler.setStream(sys.stderr)
        logging.disable(logging.NOTSET)
        return

    logging.disable(logging.CRITICAL)
