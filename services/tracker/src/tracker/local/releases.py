"""Register the checkout's executor source as the release for local runs."""

import hashlib
from pathlib import Path
from uuid import uuid4

from sqlmodel import Session

from executor_protocol import SUPPORTED_PROTOCOL_VERSION, source_executor_artifact_uri
from tracker.database.models import ExecutorRelease
from tracker.executor.release_control import (
    register_release,
    promote_release,
    lock_executor_admission,
    select_active_release,
)


def register_source_release(session: Session, source_root: Path) -> ExecutorRelease:
    """Activate the release that always runs this checkout's current executor source."""
    artifact_uri = source_executor_artifact_uri(source_root.resolve())
    # Source releases are not pinned: the digest names the checkout, not its contents, so edits reuse the release.
    artifact_digest = hashlib.sha256(artifact_uri.encode()).hexdigest()

    admission = lock_executor_admission(session)
    if admission.release_id is not None:
        active = select_active_release(session)
        if (
            active.artifact_uri == artifact_uri
            and active.artifact_digest == artifact_digest
            and active.protocol_version == SUPPORTED_PROTOCOL_VERSION
        ):
            return active

    candidate = ExecutorRelease(
        id=f"source-{uuid4()}",
        artifact_uri=artifact_uri,
        artifact_digest=artifact_digest,
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        readiness_verified=True,
    )
    register_release(session, candidate)
    return promote_release(session, candidate.id)
