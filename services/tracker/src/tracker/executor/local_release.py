"""Register the installed Tracker executor as a local Compose release."""

import hashlib
import json
import os
import shutil
import subprocess
import sys
from contextlib import AbstractContextManager
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from sqlmodel import Session, col, select

from tracker.database.models import ExecutorRelease, ExecutorReleaseStatus
from tracker.executor.release_control import activate_release

# The launcher runs whatever executor code is installed in this image, so a local release is not
# byte-immutable across rebuilds. Its identity changes only with the executor protocol version.
_LAUNCHER = (
    f"# executor protocol {SUPPORTED_PROTOCOL_VERSION}\nfrom tracker.executor.entrypoint import main\nmain()\n"
).encode()


class LocalArtifactStore:
    """Resolve release artifact keys from the local shared filesystem."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, bucket: str, key: str) -> Path:
        # Release URIs are only prefix-checked, so keep keys like "prefix/../../etc/passwd" inside the bucket.
        bucket_root = (self.root / bucket).resolve()
        path = (bucket_root / key).resolve()
        if not path.is_relative_to(bucket_root):
            raise ValueError(f"Local executor artifact key escapes its bucket: {key!r}")
        return path

    def open(self, bucket: str, key: str) -> AbstractContextManager[BinaryIO]:
        return self._path(bucket, key).open("rb")

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        shutil.copyfile(self._path(bucket, key), filename)


def register_local_release(session: Session, *, root: Path, bucket: str, prefix: str) -> ExecutorRelease:
    """Publish, smoke-test, and activate the local image's executor."""
    digest = hashlib.sha256(_LAUNCHER).hexdigest()
    # Reuse the active release only when its artifact location matches. A drained or retired one cannot
    # be reactivated, so switching back to an older protocol version gets a fresh release ID.
    active = session.exec(
        select(ExecutorRelease).where(
            col(ExecutorRelease.artifact_digest) == digest,
            col(ExecutorRelease.status) == ExecutorReleaseStatus.ACTIVE,
        )
    ).first()
    if active is not None and active.artifact_uri == f"s3://{bucket}/{prefix}/{active.id}/executor.pex":
        release_id = active.id
    else:
        release_id = f"local-{digest[:16]}-{uuid4().hex[:8]}"
    key = f"{prefix}/{release_id}/executor.pex"
    artifact = root / bucket / key
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(_LAUNCHER)
    subprocess.run([sys.executable, str(artifact), "--check"], check=True, stdout=subprocess.PIPE)

    release = ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://{bucket}/{key}",
        artifact_digest=digest,
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
    )
    return activate_release(
        session,
        release,
        expected_bucket=bucket,
        expected_prefix=prefix,
        artifact_reader=LocalArtifactStore(root),
    )


def main() -> None:
    if os.environ["EXECUTOR_LAUNCHER"] != "local":
        raise SystemExit("Local executor release requires EXECUTOR_LAUNCHER=local")
    root = Path(os.environ["EXECUTOR_RELEASE_LOCAL_DIR"])
    bucket = os.environ["EXECUTOR_RELEASE_BUCKET"]
    prefix = os.environ["EXECUTOR_RELEASE_PREFIX"]
    # Imported here because tracker.config reconfigures logging, which would replace the runner's handlers.
    from tracker.database.session import engine

    with Session(engine, expire_on_commit=False) as session:
        release = register_local_release(session, root=root, bucket=bucket, prefix=prefix)
        session.commit()
    print(
        json.dumps(
            {"release_id": release.id, "artifact_uri": release.artifact_uri, "artifact_digest": release.artifact_digest}
        )
    )


if __name__ == "__main__":
    main()
