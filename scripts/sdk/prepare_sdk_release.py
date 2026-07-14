"""Prepare and preflight a hash-bound Valkyrie SDK release."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import zipfile
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from email.parser import Parser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

INDEX_API = {
    "pypi": "https://pypi.org/pypi",
    "testpypi": "https://test.pypi.org/pypi",
}


class ReleaseError(RuntimeError):
    """A release artifact or target failed preflight validation."""


@dataclass(frozen=True)
class ReleaseManifest:
    """Identity and hashes for one wheel/sdist release pair."""

    name: str
    version: str
    files: dict[str, str]


def _wheel_identity(path: Path) -> tuple[str, str]:
    with zipfile.ZipFile(path) as archive:
        metadata_members = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_members) != 1:
            raise ReleaseError(f"expected one wheel METADATA file, found {len(metadata_members)}")
        metadata = Parser().parsestr(archive.read(metadata_members[0]).decode())
    name = metadata["Name"]
    version = metadata["Version"]
    if not name or not version:
        raise ReleaseError("wheel metadata must contain Name and Version")
    return name, version


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        while chunk := artifact.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(dist: Path) -> ReleaseManifest:
    """Build a release manifest from exactly one wheel and one sdist."""
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ReleaseError(f"expected one wheel and one sdist, found {len(wheels)} wheel(s), {len(sdists)} sdist(s)")

    name, version = _wheel_identity(wheels[0])
    normalized = name.replace("-", "_")
    if normalized not in sdists[0].name or version not in sdists[0].name:
        raise ReleaseError("wheel and sdist names or versions do not match")
    files = {path.name: _sha256(path) for path in (wheels[0], sdists[0])}
    return ReleaseManifest(name=name, version=version, files=files)


def write_checksums(manifest: ReleaseManifest, dist: Path, destination: Path) -> None:
    """Write sha256sum-compatible paths relative to the manifest directory."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for filename, digest in sorted(manifest.files.items()):
        relative_path = os.path.relpath(dist / filename, destination.parent)
        lines.append(f"{digest}  {relative_path}")
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def ensure_index_version_available(
    target: str,
    name: str,
    version: str,
    *,
    opener: Callable[[Request], AbstractContextManager[Any]] = urlopen,
) -> None:
    """Fail if a release version already exists on the selected index."""
    try:
        base_url = INDEX_API[target]
    except KeyError as exc:
        raise ReleaseError(f"unsupported package index: {target}") from exc

    url = f"{base_url}/{quote(name)}/{quote(version)}/json"
    request = Request(url, headers={"User-Agent": "vals-ai/Valkyrie SDK release preflight"})
    try:
        with opener(request):
            pass
    except HTTPError as exc:
        if exc.code == 404:
            return
        raise ReleaseError(f"could not check {target} for {name} {version}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise ReleaseError(f"could not check {target} for {name} {version}: {exc.reason}") from exc
    raise ReleaseError(f"{name} {version} already exists on {target}")


def write_output(path: Path, manifest: ReleaseManifest, target: str) -> None:
    data = {
        "name": manifest.name,
        "version": manifest.version,
        "target": target,
        "manifest": json.dumps(manifest.files, sort_keys=True, separators=(",", ":")),
    }
    with path.open("a", encoding="utf-8") as output:
        for key, value in data.items():
            output.write(f"{key}={value}\n")


def write_summary(path: Path, manifest: ReleaseManifest, target: str) -> None:
    source_sha = os.environ.get("GITHUB_SHA", "local")
    lines = [
        "## Valkyrie SDK release",
        "",
        f"- Project: `{manifest.name}`",
        f"- Version: `{manifest.version}`",
        f"- Target: `{target}`",
        f"- Source: `{source_sha}`",
        "",
        "| File | SHA-256 |",
        "| --- | --- |",
    ]
    lines.extend(f"| `{filename}` | `{digest}` |" for filename, digest in sorted(manifest.files.items()))
    with path.open("a", encoding="utf-8") as summary:
        summary.write("\n".join(lines) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Validate release availability and emit hashes for GitHub Actions."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=sorted(INDEX_API), required=True)
    parser.add_argument("--dist", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=os.environ.get("GITHUB_OUTPUT"))
    parser.add_argument("--summary", type=Path, default=os.environ.get("GITHUB_STEP_SUMMARY"))
    args = parser.parse_args(argv)

    manifest = build_manifest(args.dist)
    ensure_index_version_available(args.target, manifest.name, manifest.version)
    write_checksums(manifest, args.dist, args.manifest)
    if args.output is not None:
        write_output(args.output, manifest, args.target)
    if args.summary is not None:
        write_summary(args.summary, manifest, args.target)
    print(f"release preflight passed: {manifest.name} {manifest.version} -> {args.target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
