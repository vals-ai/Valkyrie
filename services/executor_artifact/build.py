"""Build the immutable ARM64 executor PEX and provenance manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

from executor_protocol import DEFAULT_EXECUTOR_RELEASE_PREFIX, SUPPORTED_PROTOCOL_VERSION

PEX_VERSION = "2.98.2"
_REQUIRED_ARCHIVE_PATHS = (
    "tracker/executor/entrypoint.py",
    "executor_protocol.py",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def release_identity(source_revision: str, artifact_digest: str) -> tuple[str, str]:
    release_id = f"git-{source_revision[:12]}-{artifact_digest[:16]}"
    return release_id, f"{DEFAULT_EXECUTOR_RELEASE_PREFIX}/{release_id}/executor.pex"


def _contains_importable_member(names: set[str], required: str) -> bool:
    if required in names:
        return True
    return any(
        name.startswith(".deps/") and ".whl/" in name and name.split(".whl/", maxsplit=1)[1] == required
        for name in names
    )


def verify_archive(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        invalid_member = archive.testzip()
        if invalid_member is not None:
            raise ValueError(f"Executor PEX contains an invalid member: {invalid_member}")
        names = set(archive.namelist())
    missing = [required for required in _REQUIRED_ARCHIVE_PATHS if not _contains_importable_member(names, required)]
    if missing:
        raise ValueError(f"Executor PEX is missing required modules: {', '.join(missing)}")


def build(output_directory: Path, source_revision: str) -> dict[str, object]:
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError("Executor artifacts must be built with Python 3.12")
    if platform.machine() not in {"aarch64", "arm64"}:
        raise RuntimeError("Executor artifacts must be built on ARM64")

    executor_project = Path(__file__).resolve().parent
    tracker_directory = executor_project.parent / "tracker"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / "executor.pex"

    with tempfile.TemporaryDirectory(prefix="valkyrie-executor-build-") as temporary_directory:
        temporary = Path(temporary_directory)
        requirements = temporary / "requirements.txt"
        wheel_directory = temporary / "wheel"
        wheel_directory.mkdir()
        artifact = temporary / "executor.pex"
        subprocess.run(
            [
                "uv",
                "export",
                "--project",
                str(executor_project),
                "--locked",
                "--no-dev",
                "--no-hashes",
                "--no-emit-local",
                "--format",
                "requirements.txt",
                "--output-file",
                str(requirements),
            ],
            cwd=tracker_directory,
            check=True,
        )
        subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", str(wheel_directory)],
            cwd=tracker_directory,
            check=True,
        )
        wheels = list(wheel_directory.glob("tracker-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError("Tracker build must produce exactly one wheel")
        subprocess.run(
            [
                "uvx",
                "--python",
                "3.12",
                "--from",
                f"pex=={PEX_VERSION}",
                "pex",
                "--no-transitive",
                "-r",
                str(requirements),
                str(wheels[0]),
                "-m",
                "tracker.executor.entrypoint",
                "-o",
                str(artifact),
            ],
            cwd=tracker_directory,
            check=True,
        )
        verify_archive(artifact)
        subprocess.run([sys.executable, str(artifact), "--check"], cwd=tracker_directory, check=True)
        shutil.copyfile(artifact, output_path)
        requirements_digest = sha256(requirements)
        wheel_digest = sha256(wheels[0])

    artifact_digest = sha256(output_path)
    release_id, key = release_identity(source_revision, artifact_digest)
    manifest: dict[str, object] = {
        "architecture": "linux-arm64",
        "artifact_digest": artifact_digest,
        "artifact_path": output_path.name,
        "artifact_size_bytes": output_path.stat().st_size,
        "key": key,
        "pex_version": PEX_VERSION,
        "protocol_version": SUPPORTED_PROTOCOL_VERSION,
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "release_id": release_id,
        "requirements_digest": requirements_digest,
        "source_revision": source_revision,
        "tracker_wheel_digest": wheel_digest,
    }
    (output_directory / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--source-revision")
    args = parser.parse_args()
    source_revision = args.source_revision or subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    manifest = build(args.output_directory, source_revision)
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
