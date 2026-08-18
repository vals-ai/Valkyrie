"""Config file state helpers for CLI commands."""

from enum import Enum
from typing import Any

import click
import yaml

from valkyrie.cli.runtime_config import config_location


class ConfigValue(str, Enum):
    API_KEY = "api_key"
    SLACK_WEBHOOK_SECRET = "webhook"
    AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
    AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
    AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"
    AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"
    S3_BUCKET = "S3_BUCKET"
    LOG_GROUP = "LOG_GROUP"
    LOG_RETENTION_POLICY = "LOG_RETENTION_POLICY"

    @classmethod
    def from_str(cls, key: str) -> "ConfigValue":
        """Convert string value to enum value, raising if value is not an option."""
        for member in cls:
            if member.value.lower() == key.lower():
                return member

        raise ValueError(f"Invalid config key: {key!r}")


def read_config_if_exists() -> dict[str, Any]:
    """Read the config file if present, otherwise return an empty config."""
    config_path = config_location()
    if not config_path.exists():
        return {}

    with open(config_path) as config_file:
        return yaml.safe_load(config_file) or {}


def load_config() -> dict[str, Any]:
    """Load the Valkyrie configuration from YAML file."""
    if not config_location().exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    return read_config_if_exists()


def write_config(config: dict[str, Any], *, sort_keys: bool = True) -> None:
    """Write the Valkyrie configuration to disk."""
    config_path = config_location()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w") as config_file:
        yaml.dump(config, config_file, default_flow_style=False, sort_keys=sort_keys)
