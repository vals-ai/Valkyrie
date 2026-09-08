"""Runtime environment selection for the Valkyrie CLI."""

import os
from pathlib import Path

import yaml

BENCH_ENVIRONMENT = "bench"
PRODUCTION_ENVIRONMENT = "prod"
DEV_ENVIRONMENT = "dev"

VALKYRIE_ENV_ENV_VAR = "VALKYRIE_ENV"
TRACKER_SERVICE_URL_ENV_VAR = "TRACKER_SERVICE_URL"
VALKYRIE_CONFIG_PATH_ENV_VAR = "VALKYRIE_CONFIG_PATH"
ENVIRONMENT_CONFIG_KEY = "environment"

BENCH_TRACKER_URL = "https://benchmark-tracker.vals.ai"
PRODUCTION_TRACKER_URL = "https://benchmark-tracker-prod.vals.ai"
DEV_TRACKER_URL = "https://benchmark-tracker-dev.vals.ai"

HOSTED_CONFIG_PATH = Path("~/.config/valkyrie/valkyrie.yaml")
DEV_CONFIG_PATH = Path("~/.config/valkyrie/dev.yaml")

_TRACKER_URLS = {
    BENCH_ENVIRONMENT: BENCH_TRACKER_URL,
    PRODUCTION_ENVIRONMENT: PRODUCTION_TRACKER_URL,
    DEV_ENVIRONMENT: DEV_TRACKER_URL,
}
_ENVIRONMENT_OVERRIDES = {
    "bench": BENCH_ENVIRONMENT,
    "dev": DEV_ENVIRONMENT,
    # "prod" shipped as the selector for the Tracker now named bench.
    "prod": BENCH_ENVIRONMENT,
    "external": PRODUCTION_ENVIRONMENT,
}


def tracker_url_for_environment(environment: str) -> str:
    """Return the Tracker URL for a validated runtime environment."""
    return _TRACKER_URLS[environment]


def _environment_from_override(selector: str) -> str:
    selector = selector.lower()
    try:
        return _ENVIRONMENT_OVERRIDES[selector]
    except KeyError:
        valid_selectors = ", ".join(sorted(_ENVIRONMENT_OVERRIDES))
        raise ValueError(f"Unknown {VALKYRIE_ENV_ENV_VAR}={selector!r}. Must be one of: {valid_selectors}") from None


def selected_environment() -> str:
    """Return the configured Valkyrie CLI environment."""
    environment_override = os.environ.get(VALKYRIE_ENV_ENV_VAR)
    if environment_override is None:
        selection_path = config_location()
        if selection_path.exists():
            payload = yaml.safe_load(selection_path.read_text(encoding="utf-8")) or {}
            environment = payload.get(ENVIRONMENT_CONFIG_KEY, BENCH_ENVIRONMENT)
        else:
            environment = BENCH_ENVIRONMENT
    else:
        environment = _environment_from_override(environment_override)
    environment = environment.lower()
    if environment not in _TRACKER_URLS:
        valid_environments = ", ".join(sorted(_TRACKER_URLS))
        raise ValueError(f"Config key {ENVIRONMENT_CONFIG_KEY!r} must be one of: {valid_environments}")

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

    environment_override = os.environ.get(VALKYRIE_ENV_ENV_VAR)
    if environment_override is not None and _environment_from_override(environment_override) == DEV_ENVIRONMENT:
        return DEV_CONFIG_PATH.expanduser()
    return HOSTED_CONFIG_PATH.expanduser()
