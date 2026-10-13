from pathlib import Path

import pytest

from tracker.local import config


def test_relative_paths_resolve_from_config_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("KEY=value\n")
    config_path = tmp_path / "local-server.yaml"
    config_path.write_text("data_root: .local/data\nsecrets_file: .env\n")
    monkeypatch.chdir("/")
    monkeypatch.setattr(config, "resources", None)

    config.configure(config_path)

    assert config.resources is not None
    assert config.resources.data_root == tmp_path / ".local/data"
    assert config.resources.secrets_file == tmp_path / ".env"
