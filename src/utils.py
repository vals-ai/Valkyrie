"""Runner for executing agents on benchmarks."""

import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import yaml
from dotenv import load_dotenv
from vals import configure_credentials

from src.base.types import BaseConfig
from src.logger import DisableLogging

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


@asynccontextmanager
async def spinner(message: str, stream_logger: logging.Logger):
    """
    Creates a spinner around logs which signify the start of a long running operation

    NOTE: This should not be used with multiple async loggin operations going on at the same time.
    WARNING: Do not include logs inside of this context manager other than the log inside of the spinning message
    """

    if not any(isinstance(handler, logging.StreamHandler) for handler in stream_logger.handlers):
        raise ValueError("Stream logger must be a stream logger with a stream handler")

    filter_handler: DisableLogging | None = None
    for handler in stream_logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            filter_handler = cast(DisableLogging, handler.filters[0])
            break

    if filter_handler is None:
        raise ValueError("Stream logger must have a filter handler to disable file logging while spining in place")

    # Disable logs to file while we are spinning
    filter_handler.enabled = True

    stop = asyncio.Event()
    start = time.time()

    async def spin():
        frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        interval = 0.1
        i = 0
        while not stop.is_set():
            elapsed = time.time() - start
            frame = frames[i % len(frames)]
            sys.stdout.write("\r\033[K")
            stream_logger.info(f"{frame} {message} [{elapsed:.1f}s]")
            sys.stdout.flush()
            i += 1
            await asyncio.sleep(interval)

    task = asyncio.create_task(spin())

    try:
        yield
    finally:
        stop.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        filter_handler.enabled = False

        elapsed = time.time() - start
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

        stream_logger.info(f"✓ {message} [completed in {elapsed:.1f}s]\n")
