"""Runner for executing agents on benchmarks."""

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from vals import configure_credentials

from agentic_harness.base.types import BaseConfig

logger = logging.getLogger(__name__)


def parse_yaml_config(path: str) -> dict[str, Any]:
    """
    Parses a yaml config file and returns the contents as a dictionary.
    """
    yaml_file = Path(path)
    if not yaml_file.exists():
        raise FileNotFoundError(f"Yaml config file `{path}` does not exist")

    try:
        with open(yaml_file, "r") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing yaml config file `{path}`: {e}") from e


def parse_base_config(config: dict[str, Any]) -> BaseConfig:
    """Parses yaml config into a pydantic object"""
    base_model_keys = BaseConfig.model_fields.keys()
    discovered_config_keys = config.keys()

    excess_keys = discovered_config_keys - base_model_keys
    if excess_keys:
        logger.warning(
            f"Excess keys have been found in the config. These will be ignored at run time: {', '.join(excess_keys)}"
        )

    # NOTE: Bareminimum validation for now

    return BaseConfig(**config)


def create_base_config(config_path: str) -> BaseConfig:
    """Creates a base config object from a yaml config file"""
    config = parse_yaml_config(config_path)

    return parse_base_config(config)


def setup_environment():
    """Sets up the environment for using the vals api"""

    _ = load_dotenv(override=True)

    _vals_api_key = os.getenv("VALS_API_KEY")
    if _vals_api_key is None:
        raise ValueError("VALS_API_KEY is not set")

    # NOTE: Used to check but sourced inside of the sdk already
    _vals_env = os.getenv("VALS_ENV")
    if _vals_env is None:
        raise ValueError("VALS_ENV is not set")

    configure_credentials(api_key=_vals_api_key)

    logger.info("Environment setup complete")
