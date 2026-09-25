"""Initialize the supplied local executor release for future runs."""

import argparse
import hashlib
import shutil
import tempfile
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel
from sqlmodel import Session

from executor_protocol import (
    SUPPORTED_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    source_executor_artifact_uri,
    validate_executor_digest,
)
from tracker.database.models import ExecutorRelease
from tracker.executor.release_control import (
    ReleaseControlError,
    register_release,
    promote_release,
    lock_executor_admission,
    select_active_release,
)
from tracker.local.storage import local_path


class LocalReleaseManifest(BaseModel):
    """Fields consumed from the existing executor artifact builder's manifest."""

    artifact_path: str
    artifact_digest: str
    protocol_version: str


def _publish_artifact(artifact: Path, root: Path, digest: str) -> Path:
    destination = local_path(root, f"{digest}/executor.pex")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as staging:
        temporary = Path(staging) / "executor.pex"
        shutil.copyfile(artifact, temporary)
        with temporary.open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != digest:
                raise ReleaseControlError("Local executor artifact does not match its manifest digest")
        temporary.chmod(0o444)
        temporary.replace(destination)
    return destination


def initialize_release(
    session: Session,
    manifest_path: Path,
    release_root: Path,
) -> ExecutorRelease:
    """Verify and activate a content-addressed artifact, preserving prior releases."""
    if not release_root.is_absolute():
        raise ValueError("Local executor release root must be absolute")
    manifest = LocalReleaseManifest.model_validate_json(manifest_path.read_bytes())
    digest = validate_executor_digest(manifest.artifact_digest)
    if manifest.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
        raise ReleaseControlError(f"Unsupported executor protocol version: {manifest.protocol_version}")
    artifact = local_path(manifest_path.parent, manifest.artifact_path)
    destination = _publish_artifact(artifact, release_root.resolve(), digest)

    return _activate_or_reuse(
        session,
        artifact_uri=destination.as_uri(),
        artifact_digest=digest,
        protocol_version=manifest.protocol_version,
        id_prefix="local",
    )


def register_source_release(session: Session, source_root: Path) -> ExecutorRelease:
    """Activate a release that runs the executor from this checkout, reusing it until the source changes."""
    root = source_root.resolve()

    return _activate_or_reuse(
        session,
        artifact_uri=source_executor_artifact_uri(root),
        artifact_digest=_source_tree_digest(root),
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        id_prefix="source",
    )


def _source_tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if "__pycache__" in relative.parts or not path.is_file():
            continue
        content_digest = hashlib.sha256(path.read_bytes()).hexdigest()
        digest.update(f"{relative.as_posix()}\0{content_digest}\n".encode())
    return digest.hexdigest()


def _activate_or_reuse(
    session: Session,
    *,
    artifact_uri: str,
    artifact_digest: str,
    protocol_version: str,
    id_prefix: str,
) -> ExecutorRelease:
    admission = lock_executor_admission(session)
    if admission.release_id is not None:
        active = select_active_release(session)
        if (
            active.artifact_uri == artifact_uri
            and active.artifact_digest == artifact_digest
            and active.protocol_version == protocol_version
        ):
            return active

    candidate = ExecutorRelease(
        id=f"{id_prefix}-{uuid4()}",
        artifact_uri=artifact_uri,
        artifact_digest=artifact_digest,
        protocol_version=protocol_version,
        readiness_verified=True,
    )
    register_release(session, candidate)
    return promote_release(session, candidate.id)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--release-root", type=Path, required=True)
    args = parser.parse_args()

    from tracker.database.session import engine

    with Session(engine) as session:
        release = initialize_release(session, args.manifest.resolve(), args.release_root)
        session.commit()
        print(f"Local executor release ready: {release.id}")


if __name__ == "__main__":
    main()
