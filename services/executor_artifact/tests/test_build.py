import json
import subprocess
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


@pytest.mark.parametrize(("system", "artifact_protocol"), [("Linux", "4"), ("Darwin", "4"), ("Linux", "3")])
def test_new_artifact_manifest_requires_protocol_four(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, system: str, artifact_protocol: str
) -> None:
    """Publish only a manifest matching the checked artifact and native build platform.

    Test cases:
    - Linux and macOS artifacts report their own operating system.
    - A stale packaged protocol cannot be published with a newer manifest.
    """

    def fake_command(command: Sequence[str], **_kwargs: object) -> subprocess.CompletedProcess[str] | None:
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
        elif "--check" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"protocol_version": artifact_protocol}), "")

        return None

    monkeypatch.setattr(builder.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(builder.platform, "system", lambda: system)
    commands = Mock(side_effect=fake_command)
    monkeypatch.setattr(builder.subprocess, "run", commands)

    if artifact_protocol != "4":
        with pytest.raises(ValueError, match="protocol does not match"):
            builder.build(tmp_path, "a" * 40)
        assert not (tmp_path / "manifest.json").exists()
        return

    manifest = builder.build(tmp_path, "a" * 40)

    assert manifest["protocol_version"] == "4"
    assert manifest["architecture"] == f"{system.lower()}-arm64"
    assert json.loads((tmp_path / "manifest.json").read_text())["protocol_version"] == "4"
