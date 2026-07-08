"""Config file state helpers for CLI commands."""

from enum import Enum
from pathlib import Path
from typing import Any

import click
import yaml

CONFIG_LOCATION: Path = Path("~/.config/valkyrie/valkyrie.yaml").expanduser()


class ConfigValue(str, Enum):
    API_KEY = "api_key"
    SLACK_WEBHOOK_SECRET = "webhook"
    AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
    AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
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
    if not CONFIG_LOCATION.exists():
        return {}

    with open(CONFIG_LOCATION) as config_file:
        return yaml.safe_load(config_file) or {}


def load_config() -> dict[str, Any]:
    """Load the Valkyrie configuration from YAML file."""
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    return read_config_if_exists()


def write_config(config: dict[str, Any], *, sort_keys: bool = True) -> None:
    """Write the Valkyrie configuration to disk."""
    CONFIG_LOCATION.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_LOCATION, "w") as config_file:
        yaml.dump(config, config_file, default_flow_style=False, sort_keys=sort_keys)
