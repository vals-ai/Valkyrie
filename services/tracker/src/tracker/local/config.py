"""Local installation resources selected by the server's configuration file."""

from pathlib import Path

import yaml

from tracker.local.resources import LocalResources


resources: LocalResources | None = None


def configure(path: Path) -> None:
    global resources
    config = LocalResources.model_validate(yaml.safe_load(path.read_text()))
    if config.secrets_file is not None and not config.secrets_file.is_file():
        raise ValueError("secrets_file must name an existing file")
    resources = config
