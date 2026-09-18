"""Local installation resources selected by the server's configuration file."""

from pathlib import Path

import yaml
from pydantic import BaseModel
from typing import Literal

from tracker.local.resources import LocalResources


class LocalConfiguration(BaseModel):
    execution_environment: Literal["aws", "local"] = "aws"
    local_data_root: Path | None = None
    local_secrets_file: Path | None = None


resources: LocalResources | None = None


def configure(path: Path) -> None:
    global resources
    config = LocalConfiguration.model_validate(yaml.safe_load(path.read_text()))
    if config.execution_environment == "local":
        if config.local_data_root is None:
            raise ValueError("local_data_root is required for local execution")
        if not config.local_data_root.is_absolute():
            raise ValueError("local_data_root must be an absolute path")
        secrets_file = config.local_secrets_file
        if secrets_file is not None:
            if not secrets_file.is_absolute() or not secrets_file.is_file():
                raise ValueError("local_secrets_file must name an existing absolute file")
            secrets_file = secrets_file.resolve()
        resources = LocalResources(data_root=config.local_data_root.resolve(), secrets_file=secrets_file)
    else:
        resources = None
