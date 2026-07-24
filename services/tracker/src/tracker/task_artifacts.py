"""Immutable, range-readable task artifact indexes."""

from __future__ import annotations

import base64
import binascii
from bisect import bisect_left
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    download_range_from_s3,
    s3_object_exists,
)

MAX_ARTIFACT_FILES = 50_000
MAX_ARTIFACT_INDEX_BYTES = 32 * 1024 * 1024
MAX_ARTIFACT_PATH_BYTES = 4_000
MAX_ARTIFACT_PATH_COMPONENTS = 24
MAX_ARTIFACT_PACK_BYTES = 8 * 1024 * 1024 * 1024
ARTIFACT_CONTENT_PAGE_BYTES = 64 * 1024
MAX_ARTIFACT_INDEX_CACHE_BYTES = 64 * 1024 * 1024


class ArtifactFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    size_bytes: int
    offset: int


class ArtifactIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    generation: str = Field(pattern=r"^[0-9a-f]{32}$")
    archive_available: bool
    pack_size_bytes: int
    files: list[ArtifactFile]


class ArtifactIndexNotFoundError(Exception):
    pass


class InvalidArtifactIndexError(Exception):
    pass


@dataclass(frozen=True)
class ArtifactTreeEntry:
    kind: Literal["directory", "file"]
    path: str
    size_bytes: int | None


@dataclass(frozen=True)
class ArtifactContent:
    data: bytes
    next_cursor: str | None


_artifact_index_cache: OrderedDict[tuple[str, str], tuple[ArtifactIndex, int]] = OrderedDict()
_artifact_index_cache_bytes = 0


def clear_artifact_index_cache() -> None:
    global _artifact_index_cache_bytes

    _artifact_index_cache.clear()
    _artifact_index_cache_bytes = 0


def _get_cached_artifact_index(key: tuple[str, str]) -> ArtifactIndex | None:
    cached = _artifact_index_cache.get(key)
    if cached is None:
        return None
    _artifact_index_cache.move_to_end(key)
    return cached[0]


def _cache_artifact_index(key: tuple[str, str], index: ArtifactIndex, size_bytes: int) -> None:
    global _artifact_index_cache_bytes

    existing = _artifact_index_cache.pop(key, None)
    if existing is not None:
        _artifact_index_cache_bytes -= existing[1]
    _artifact_index_cache[key] = (index, size_bytes)
    _artifact_index_cache_bytes += size_bytes

    while _artifact_index_cache_bytes > MAX_ARTIFACT_INDEX_CACHE_BYTES:
        _, (_, evicted_bytes) = _artifact_index_cache.popitem(last=False)
        _artifact_index_cache_bytes -= evicted_bytes


def task_artifact_prefix(benchmark_id: str, task_id: str, attempt_id: str) -> str:
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/.valkyrie/artifacts/{attempt_id}"


def task_artifact_index_key(
    benchmark_id: str,
    task_id: str,
    attempt_id: str,
) -> str:
    return f"{task_artifact_prefix(benchmark_id, task_id, attempt_id)}/index.json"


def task_artifact_generation_key(
    benchmark_id: str,
    task_id: str,
    attempt_id: str,
    generation: str,
    name: Literal["agent_output.tar.gz", "content.pack"],
) -> str:
    return f"{task_artifact_prefix(benchmark_id, task_id, attempt_id)}/generations/{generation}/{name}"


def _validate_path(path: str, *, allow_empty: bool = False) -> str:
    if allow_empty and not path:
        return path
    parsed = PurePosixPath(path)
    if parsed.is_absolute() or str(parsed) != path:
        raise ValueError("Artifact paths must be normalized relative paths")
    if ".." in parsed.parts:
        raise ValueError("Artifact paths cannot traverse parent directories")
    if len(parsed.parts) > MAX_ARTIFACT_PATH_COMPONENTS:
        raise ValueError("Artifact path has too many components")
    if len(path.encode()) > MAX_ARTIFACT_PATH_BYTES:
        raise ValueError("Artifact path is too long")
    if ".valkyrie" in parsed.parts:
        raise ValueError("Artifact path uses the reserved .valkyrie directory")
    return path


def build_artifact_index(
    manifest: bytes,
    pack_size_bytes: int,
    generation: str,
    archive_available: bool,
) -> ArtifactIndex:
    """Validate the sandbox manifest and return a path-sorted immutable index."""
    fields = manifest.split(b"\0")
    if fields[-1] != b"" or (len(fields) - 1) % 3:
        raise ValueError("Artifact manifest is malformed")

    files: list[ArtifactFile] = []
    expected_offset = 0
    for index in range(0, len(fields) - 1, 3):
        path = _validate_path(fields[index].decode())
        size_bytes = int(fields[index + 1])
        offset = int(fields[index + 2])
        if size_bytes < 0 or offset != expected_offset:
            raise ValueError("Artifact manifest offsets are invalid")
        files.append(ArtifactFile(path=path, size_bytes=size_bytes, offset=offset))
        expected_offset += size_bytes

    if len(files) > MAX_ARTIFACT_FILES:
        raise ValueError("Artifact index contains too many files")
    if expected_offset != pack_size_bytes or pack_size_bytes > MAX_ARTIFACT_PACK_BYTES:
        raise ValueError("Artifact pack size does not match its index")

    files.sort(key=lambda file: file.path)
    if len({file.path for file in files}) != len(files):
        raise ValueError("Artifact index contains duplicate paths")
    for parent, child in zip(files, files[1:], strict=False):
        if child.path.startswith(f"{parent.path}/"):
            raise ValueError("Artifact index contains a file and its descendant")
    return ArtifactIndex(
        generation=generation,
        archive_available=archive_available,
        pack_size_bytes=pack_size_bytes,
        files=files,
    )


def serialize_artifact_index(index: ArtifactIndex) -> bytes:
    content = index.model_dump_json().encode()
    if len(content) > MAX_ARTIFACT_INDEX_BYTES:
        raise ValueError("Artifact index is too large")
    return content


async def load_artifact_index(
    benchmark_id: str,
    task_id: str,
    attempt_id: str,
    runtime: AWSRuntime,
) -> ArtifactIndex:
    key = task_artifact_index_key(benchmark_id, task_id, attempt_id)
    cache_key = (runtime.resources.s3_bucket, key)
    cached = _get_cached_artifact_index(cache_key)
    if cached is not None:
        return cached

    if not await s3_object_exists(key, runtime):
        raise ArtifactIndexNotFoundError

    content = await download_range_from_s3(
        key,
        0,
        MAX_ARTIFACT_INDEX_BYTES,
        runtime,
    )
    if len(content) > MAX_ARTIFACT_INDEX_BYTES:
        raise InvalidArtifactIndexError("Artifact index is too large")
    try:
        index = ArtifactIndex.model_validate_json(content)
        manifest = bytearray()
        for file in index.files:
            manifest.extend(file.path.encode())
            manifest.extend(b"\0")
            manifest.extend(str(file.size_bytes).encode())
            manifest.extend(b"\0")
            manifest.extend(str(file.offset).encode())
            manifest.extend(b"\0")
        validated = build_artifact_index(
            bytes(manifest),
            index.pack_size_bytes,
            index.generation,
            index.archive_available,
        )
        if validated != index:
            raise ValueError("Artifact index files are not sorted")
        _cache_artifact_index(cache_key, validated, len(content))
        return validated
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidArtifactIndexError("Artifact index is invalid") from exc


def artifact_candidates(index: ArtifactIndex) -> tuple[str | None, str | None]:
    paths = [file.path for file in index.files]
    path_set = set(paths)
    trajectory_names = (
        "vals_format/turns.jsonl",
        "artifacts/turns.jsonl",
        "trajectory.json",
        "trajectory.jsonl",
    )

    trajectory = next((name for name in trajectory_names if name in path_set), None)
    if trajectory is None:
        trajectory = next(
            (path for name in trajectory_names for path in paths if path.endswith(f"/{name}")),
            None,
        )

    top_level = [path for path in paths if "/" not in path]
    diff = next((path for path in top_level if path.endswith(".patch")), None)
    diff = diff or next((path for path in top_level if path.endswith(".diff")), None)
    diff = diff or next((path for path in paths if "/" in path and path.endswith(".patch")), None)
    diff = diff or next((path for path in paths if "/" in path and path.endswith(".diff")), None)
    return trajectory, diff


def list_artifact_files(
    index: ArtifactIndex,
    prefix: str,
    cursor: str | None,
    limit: int,
) -> tuple[list[ArtifactTreeEntry], str | None]:
    prefix = _validate_path(prefix, allow_empty=True)
    children: dict[str, ArtifactTreeEntry] = {}
    prefix_with_slash = f"{prefix}/" if prefix else ""

    start = bisect_left(
        index.files,
        prefix_with_slash,
        key=lambda file: file.path,
    )
    for file in index.files[start:]:
        if not file.path.startswith(prefix_with_slash):
            break
        remainder = file.path[len(prefix_with_slash) :]
        name, separator, _ = remainder.partition("/")
        path = f"{prefix_with_slash}{name}"
        if separator:
            children[path] = ArtifactTreeEntry(kind="directory", path=path, size_bytes=None)
        else:
            children[path] = ArtifactTreeEntry(kind="file", path=path, size_bytes=file.size_bytes)

    items = sorted(children.values(), key=lambda item: (item.kind == "file", item.path))
    offset = decode_artifact_cursor(cursor)
    if offset > len(items):
        raise ValueError("Artifact cursor is out of range")
    page = items[offset : offset + limit]
    next_offset = offset + len(page)
    next_cursor = encode_artifact_cursor(next_offset) if next_offset < len(items) else None
    return page, next_cursor


def find_artifact_file(index: ArtifactIndex, path: str) -> ArtifactFile:
    path = _validate_path(path)
    position = bisect_left(index.files, path, key=lambda file: file.path)
    if position == len(index.files) or index.files[position].path != path:
        raise KeyError(path)
    return index.files[position]


async def read_artifact_content(
    benchmark_id: str,
    task_id: str,
    attempt_id: str,
    generation: str,
    file: ArtifactFile,
    cursor: str | None,
    runtime: AWSRuntime,
) -> ArtifactContent:
    offset = decode_artifact_cursor(cursor)
    if offset > file.size_bytes:
        raise ValueError("Artifact cursor is out of range")
    if offset == file.size_bytes:
        return ArtifactContent(data=b"", next_cursor=None)

    remaining = file.size_bytes - offset
    requested_bytes = min(remaining, ARTIFACT_CONTENT_PAGE_BYTES)
    pack_key = task_artifact_generation_key(
        benchmark_id,
        task_id,
        attempt_id,
        generation,
        "content.pack",
    )
    content = await download_range_from_s3(
        pack_key,
        file.offset + offset,
        file.offset + offset + requested_bytes - 1,
        runtime,
    )
    page = content[:requested_bytes]
    next_offset = offset + len(page)
    next_cursor = encode_artifact_cursor(next_offset) if next_offset < file.size_bytes else None
    return ArtifactContent(data=page, next_cursor=next_cursor)


def encode_artifact_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode().rstrip("=")


def decode_artifact_cursor(cursor: str | None) -> int:
    if cursor is None:
        return 0
    try:
        padding = "=" * (-len(cursor) % 4)
        offset = int(base64.urlsafe_b64decode(cursor + padding))
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Artifact cursor is invalid") from exc
    if offset < 0:
        raise ValueError("Artifact cursor is invalid")
    return offset
