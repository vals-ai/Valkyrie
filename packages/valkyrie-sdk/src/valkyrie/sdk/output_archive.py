"""Bounded extraction of run output archives into a new directory."""

import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import BinaryIO


def extract_output_archive(stream: BinaryIO, output_dir: Path, *, max_expanded_bytes: int, max_entries: int) -> Path:
    """Extract regular files and directories, including one layer of task tarballs."""
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(f"Output directory already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    expanded = 0
    entries = 0

    def extract(source: BinaryIO, destination: Path) -> None:
        nonlocal expanded, entries
        seen: set[str] = set()
        with tarfile.open(fileobj=source, mode="r|*") as archive:
            for member in archive:
                entries += 1
                expanded += member.size
                if entries > max_entries or expanded > max_expanded_bytes:
                    raise ValueError("Run outputs exceed extraction limits")
                path = PurePosixPath(member.name)
                if path == PurePosixPath(".") and member.isdir():
                    continue
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or "\\" in member.name
                    or ":" in member.name
                    or not (member.isfile() or member.isdir())
                    or str(path).casefold() in seen
                ):
                    raise ValueError(f"Unsafe output archive member: {member.name!r}")
                seen.add(str(path).casefold())
                target = destination / path
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    content = archive.extractfile(member)
                    if content is None:
                        raise ValueError("Output archive member has no content")
                    with content, target.open("xb") as output:
                        while chunk := content.read(1024 * 1024):
                            output.write(chunk)
                    target.chmod(member.mode & 0o777)

    with tempfile.TemporaryDirectory(dir=output_dir.parent) as temporary:
        staging = Path(temporary) / "outputs"
        staging.mkdir()
        extract(stream, staging)
        for nested in list(staging.rglob("*.tar.gz")):
            destination = nested.parent / nested.name.removesuffix(".tar.gz")
            destination.mkdir()
            with nested.open("rb") as source:
                extract(source, destination)
            nested.unlink()
        if output_dir.exists() or output_dir.is_symlink():
            raise FileExistsError(f"Output directory already exists: {output_dir}")
        staging.rename(output_dir)
    return output_dir
