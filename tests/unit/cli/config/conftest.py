"""Fixtures shared by CLI configuration tests."""

from pathlib import Path

import pytest

from valkyrie.cli.runtime_config import VALKYRIE_CONFIG_PATH_ENV_VAR


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point configuration commands at an isolated YAML file."""
    path = tmp_path / "valkyrie.yaml"
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(path))

    return path
