"""Runtime environment selection for the Valkyrie CLI."""

import os
from pathlib import Path

PROD_ENVIRONMENT = "prod"
DEV_ENVIRONMENT = "dev"

VALKYRIE_ENV_ENV_VAR = "VALKYRIE_ENV"
TRACKER_SERVICE_URL_ENV_VAR = "TRACKER_SERVICE_URL"
VALKYRIE_CONFIG_PATH_ENV_VAR = "VALKYRIE_CONFIG_PATH"

PROD_TRACKER_URL = "https://benchmark-tracker.vals.ai"
DEV_TRACKER_URL = "https://benchmark-tracker-dev.vals.ai"

PROD_CONFIG_PATH = Path("~/.config/valkyrie/valkyrie.yaml")
DEV_CONFIG_PATH = Path("~/.config/valkyrie/dev.yaml")

_TRACKER_URLS = {
    PROD_ENVIRONMENT: PROD_TRACKER_URL,
    DEV_ENVIRONMENT: DEV_TRACKER_URL,
}
_CONFIG_PATHS = {
    PROD_ENVIRONMENT: PROD_CONFIG_PATH,
    DEV_ENVIRONMENT: DEV_CONFIG_PATH,
}


def selected_environment() -> str:
    """Return the configured Valkyrie CLI environment."""
    environment = os.environ.get(VALKYRIE_ENV_ENV_VAR, PROD_ENVIRONMENT).lower()
    if environment not in _TRACKER_URLS:
        valid_environments = ", ".join(sorted(_TRACKER_URLS))
        raise ValueError(f"Unknown {VALKYRIE_ENV_ENV_VAR}={environment!r}. Must be one of: {valid_environments}")

    return environment


def tracker_service_url() -> str:
    """Return the tracker service URL for the selected environment."""
    if url := os.environ.get(TRACKER_SERVICE_URL_ENV_VAR):
        return url

    return _TRACKER_URLS[selected_environment()]


def config_location() -> Path:
    """Return the CLI config path for the selected environment."""
    if path := os.environ.get(VALKYRIE_CONFIG_PATH_ENV_VAR):
        return Path(path).expanduser()

    return _CONFIG_PATHS[selected_environment()].expanduser()
