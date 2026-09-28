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

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from sqlmodel import Session

from tracker.database.models import ExecutorRelease
from tracker.database.session import engine
from tracker.executor.release_control import activate_release

_LAUNCHER = b"from tracker.executor.entrypoint import main\nmain()\n"


class LocalArtifactStore:
    """Resolve release artifact keys from the local shared filesystem."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def open(self, bucket: str, key: str) -> AbstractContextManager[BinaryIO]:
        return (self.root / bucket / key).open("rb")

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        shutil.copyfile(self.root / bucket / key, filename)


def register_local_release(session: Session, *, root: Path, bucket: str, prefix: str) -> ExecutorRelease:
    """Publish, smoke-test, and activate the local image's executor."""
    digest = hashlib.sha256(_LAUNCHER).hexdigest()
    release_id = f"local-{digest[:16]}"
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
