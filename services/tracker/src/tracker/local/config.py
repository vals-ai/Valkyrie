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
        resources = LocalResources(data_root=config.local_data_root, secrets_file=config.local_secrets_file)
        if resources.secrets_file is not None and not resources.secrets_file.is_file():
            raise ValueError("local_secrets_file must name an existing file")
    else:
        resources = None
