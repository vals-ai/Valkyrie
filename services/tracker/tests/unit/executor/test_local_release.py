"""Local Compose release registration and runner artifact access."""

import hashlib
from pathlib import Path
from urllib.parse import urlparse

from sqlmodel import Session, select

from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus
from tracker.executor.local_release import LocalArtifactStore, register_local_release


def test_local_release_repeated_registration_is_active_and_downloadable(
    database_session: Session, tmp_path: Path
) -> None:
    bucket = "local"
    prefix = "executor-releases"
    first = register_local_release(database_session, root=tmp_path, bucket=bucket, prefix=prefix)
    database_session.commit()
    second = register_local_release(database_session, root=tmp_path, bucket=bucket, prefix=prefix)
    database_session.commit()

    releases = database_session.exec(select(ExecutorRelease)).all()
    admission = database_session.get(ExecutorAdmission, 1)
    assert len(releases) == 1
    assert first.id == second.id == releases[0].id
    assert releases[0].status == ExecutorReleaseStatus.ACTIVE
    assert releases[0].readiness_verified
    assert admission is not None
    assert admission.release_id == releases[0].id

    uri = urlparse(releases[0].artifact_uri)
    downloaded = tmp_path / "downloaded.pex"
    LocalArtifactStore(tmp_path).download_file(uri.netloc, uri.path.lstrip("/"), str(downloaded))
    assert hashlib.sha256(downloaded.read_bytes()).hexdigest() == releases[0].artifact_digest
