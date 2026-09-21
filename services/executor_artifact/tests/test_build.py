import json
import zipfile
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import Mock

import pytest

import services.executor_artifact.build as builder
from services.executor_artifact.build import release_identity, verify_archive


def test_dependency_export_omits_local_projects() -> None:
    builder = (Path(__file__).parents[1] / "build.py").read_text()

    assert '"--no-emit-local"' in builder
    assert '"--no-emit-project"' not in builder


def test_release_identity_includes_source_and_artifact_digest() -> None:
    release_id, key = release_identity("abcdef1234567890", "0123456789abcdef" * 4)

    assert release_id == "git-abcdef123456-0123456789abcdef"
    assert key == f"releases/{release_id}/executor.pex"


def test_verify_archive_requires_executor_entrypoint_and_importable_protocol(tmp_path: Path) -> None:
    artifact = tmp_path / "executor.pex"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(".deps/tracker.whl/tracker/executor/entrypoint.py", "")
        archive.writestr(".deps/tracker.whl/executor_protocol.py", "")

    verify_archive(artifact)

    wrong_protocol_location = tmp_path / "wrong-protocol-location.pex"
    with zipfile.ZipFile(wrong_protocol_location, "w") as archive:
        archive.writestr(".deps/tracker.whl/tracker/executor/entrypoint.py", "")
        archive.writestr(".deps/tracker.whl/tracker/executor_protocol.py", "")

    with pytest.raises(ValueError, match="executor_protocol"):
        verify_archive(wrong_protocol_location)


def test_new_artifact_manifest_requires_protocol_three(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_command(command: Sequence[str], **_kwargs: object) -> None:
        if "export" in command:
            Path(command[command.index("--output-file") + 1]).write_text("requirements")
        elif "--wheel" in command:
            directory = Path(command[command.index("--out-dir") + 1])
            (directory / "tracker-test.whl").write_bytes(b"wheel")
        elif "pex" in command:
            artifact = Path(command[command.index("-o") + 1])
            with zipfile.ZipFile(artifact, "w") as archive:
                archive.writestr("tracker/executor/entrypoint.py", "")
                archive.writestr("executor_protocol.py", "")

    monkeypatch.setattr(builder.platform, "machine", lambda: "arm64")
    commands = Mock(side_effect=fake_command)
    monkeypatch.setattr(builder.subprocess, "run", commands)

    manifest = builder.build(tmp_path, "a" * 40)

    assert manifest["protocol_version"] == "3"
    assert json.loads((tmp_path / "manifest.json").read_text())["protocol_version"] == "3"
