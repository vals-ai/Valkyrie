"""Local Compose release registration and runner artifact access."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest
from sqlmodel import Session, select

from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus
from tracker.executor import local_release
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


def test_local_release_launcher_changes_promote_fresh_releases(
    database_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_launcher = local_release._LAUNCHER
    old = register_local_release(database_session, root=tmp_path, bucket="local", prefix="executor-releases")
    database_session.commit()
    monkeypatch.setattr(local_release, "_LAUNCHER", b"# executor protocol next\n" + original_launcher)
    new = register_local_release(database_session, root=tmp_path, bucket="local", prefix="executor-releases")
    database_session.commit()
    database_session.refresh(old)
    assert new.id != old.id
    assert new.status == ExecutorReleaseStatus.ACTIVE
    assert old.status == ExecutorReleaseStatus.DRAINING

    # Switching back to the original launcher cannot reactivate the drained release.
    monkeypatch.setattr(local_release, "_LAUNCHER", original_launcher)
    back = register_local_release(database_session, root=tmp_path, bucket="local", prefix="executor-releases")
    database_session.commit()
    database_session.refresh(old)
    admission = database_session.get(ExecutorAdmission, 1)
    assert back.id not in (old.id, new.id)
    assert back.artifact_digest == old.artifact_digest
    assert back.status == ExecutorReleaseStatus.ACTIVE
    assert old.status == ExecutorReleaseStatus.DRAINING
    assert admission is not None
    assert admission.release_id == back.id


def test_local_artifact_store_rejects_keys_outside_its_bucket(tmp_path: Path) -> None:
    (tmp_path / "secret").write_bytes(b"not an artifact")
    (tmp_path / "local").mkdir()
    store = LocalArtifactStore(tmp_path)
    escaping_key = "executor-releases/../../secret"

    with pytest.raises(ValueError, match="escapes its bucket"):
        store.download_file("local", escaping_key, str(tmp_path / "copied.pex"))
    with pytest.raises(ValueError, match="escapes its bucket"):
        store.open("local", escaping_key)
    assert not (tmp_path / "copied.pex").exists()


def test_runner_logs_keep_dispatch_context_after_loading_local_artifact_store() -> None:
    # A fresh interpreter matches the runner process; other tests may already have loaded tracker.config.
    script = (
        "import logging\n"
        "from tracker.executor import runner_observability\n"
        "runner_observability.configure_observability()\n"
        "runner_observability.dispatch_id_var.set('dispatch-123')\n"
        "from tracker.executor.local_release import LocalArtifactStore\n"
        "logging.getLogger('tracker.executor.runner').info('after local store import')\n"
    )
    env = {key: value for key, value in os.environ.items() if key != "SENTRY_DSN"}
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True)

    record = json.loads(result.stdout.strip().splitlines()[-1])
    assert record["message"] == "after local store import"
    assert record["executor_dispatch_id"] == "dispatch-123"
