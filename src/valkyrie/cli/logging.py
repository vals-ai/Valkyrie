"""Logging controls for the Valkyrie CLI."""

import logging
import os

CLI_LOGS_ENV_VAR = "VALKYRIE_CLI_LOGS"


def configure_cli_logging() -> None:
    """Disable library logs in CLI output unless explicitly enabled."""
    if os.environ.get(CLI_LOGS_ENV_VAR) == "true":
        logging.disable(logging.NOTSET)
        return

    logging.disable(logging.CRITICAL)
