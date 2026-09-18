"""Initialize local executor releases without replacing an active release on restart."""

import argparse
import hashlib
import os
import shutil
import tempfile
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel
from sqlmodel import Session

from executor_protocol import SUPPORTED_PROTOCOL_VERSIONS, validate_executor_digest
from tracker.database.models import ExecutorRelease
from tracker.executor.release_control import (
    ReleaseControlError,
    register_release,
    promote_release,
    get_executor_admission,
    select_active_release,
)
from tracker.local.executor_artifacts import FilesystemExecutorArtifactReader
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
        try:
            os.link(temporary, destination)
        except FileExistsError:
            # Never overwrite a file that a saved dispatch may still reference.
            with destination.open("rb") as source:
                if hashlib.file_digest(source, "sha256").hexdigest() != digest:
                    raise ReleaseControlError("Existing local executor artifact has an invalid digest") from None
    return destination


def initialize_release(
    session: Session,
    manifest_path: Path,
    release_root: Path,
    *,
    replace_active: bool = False,
) -> ExecutorRelease:
    """Verify and activate a content-addressed artifact, preserving prior releases."""
    reader = FilesystemExecutorArtifactReader(release_root)
    admission = get_executor_admission(session, for_update=True)
    if admission.maintenance_target_sha is not None:
        raise ReleaseControlError("Cannot initialize a local release during executor maintenance")
    if admission.release_id is not None and not replace_active:
        active = select_active_release(session)
        with reader.open(active.artifact_uri) as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != active.artifact_digest:
                raise ReleaseControlError("Active local executor artifact has an invalid digest")
        return active

    manifest = LocalReleaseManifest.model_validate_json(manifest_path.read_bytes())
    digest = validate_executor_digest(manifest.artifact_digest)
    if manifest.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
        raise ReleaseControlError(f"Unsupported executor protocol version: {manifest.protocol_version}")
    artifact = local_path(manifest_path.parent, manifest.artifact_path)
    destination = _publish_artifact(artifact, reader.root, digest)
    candidate = ExecutorRelease(
        id=f"local-{uuid4()}",
        artifact_uri=destination.as_uri(),
        artifact_digest=digest,
        protocol_version=manifest.protocol_version,
    )
    register_release(session, candidate)
    candidate.readiness_verified = True
    session.add(candidate)
    return promote_release(session, candidate.id)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--replace-active", action="store_true", help="Activate a rebuilt executor for future runs")
    args = parser.parse_args()

    from tracker.database.session import engine

    with Session(engine) as session:
        release = initialize_release(
            session, args.manifest.resolve(), args.release_root, replace_active=args.replace_active
        )
        session.commit()
        print(f"Local executor release ready: {release.id}")


if __name__ == "__main__":
    main()
