import zipfile
from pathlib import Path

import pytest

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
