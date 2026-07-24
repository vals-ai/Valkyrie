"""Run with `uv run pytest tests/unit/test_task_artifacts.py`."""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock

import pytest

import tracker.task_artifacts as artifact_module
from tracker.task_artifacts import (
    ArtifactFile,
    ArtifactIndex,
    artifact_candidates,
    build_artifact_index,
    clear_artifact_index_cache,
    decode_artifact_cursor,
    list_artifact_files,
    load_artifact_index,
    read_artifact_content,
    serialize_artifact_index,
)
from tracker.database.models import AgentContractRequest


@pytest.fixture(autouse=True)
def clear_index_cache() -> Iterator[None]:
    clear_artifact_index_cache()
    yield
    clear_artifact_index_cache()


def test_build_index_sorts_paths_and_finds_trajectory_and_diff() -> None:
    manifest = b"\0".join(
        (
            b"agent_output/repo/fix.patch",
            b"2",
            b"0",
            b"agent_output/vals_format/turns.jsonl",
            b"3",
            b"2",
            b"summary.txt",
            b"1",
            b"5",
            b"",
        )
    )

    index = build_artifact_index(manifest, 6, "a" * 32, True)

    assert [file.path for file in index.files] == [
        "agent_output/repo/fix.patch",
        "agent_output/vals_format/turns.jsonl",
        "summary.txt",
    ]
    assert artifact_candidates(index) == (
        "agent_output/vals_format/turns.jsonl",
        "agent_output/repo/fix.patch",
    )
    assert ArtifactIndex.model_validate_json(serialize_artifact_index(index)) == index


@pytest.mark.parametrize(
    "manifest",
    [
        b"\0".join((b"../secret", b"1", b"0", b"")),
        b"\0".join((b".valkyrie/secret", b"1", b"0", b"")),
        b"\0".join((b"file", b"1", b"1", b"")),
    ],
)
def test_build_index_rejects_unsafe_paths_and_offsets(manifest: bytes) -> None:
    with pytest.raises(ValueError):
        build_artifact_index(manifest, 1, "a" * 32, False)


def test_build_index_rejects_file_ancestor_collision() -> None:
    manifest = b"\0".join((b"a", b"1", b"0", b"a/b", b"1", b"1", b""))

    with pytest.raises(ValueError, match="file and its descendant"):
        build_artifact_index(manifest, 2, "a" * 32, False)


async def test_load_index_caches_one_successful_download(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: object,
) -> None:
    content = serialize_artifact_index(
        ArtifactIndex(
            generation="a" * 32,
            archive_available=False,
            pack_size_bytes=1,
            files=[ArtifactFile(path="result.txt", size_bytes=1, offset=0)],
        )
    )
    exists = AsyncMock(return_value=True)
    download = AsyncMock(return_value=content)
    monkeypatch.setattr(artifact_module, "s3_object_exists", exists)
    monkeypatch.setattr(artifact_module, "download_range_from_s3", download)

    first = await load_artifact_index("run", "task", "attempt", aws_runtime)  # type: ignore[arg-type]
    second = await load_artifact_index("run", "task", "attempt", aws_runtime)  # type: ignore[arg-type]

    assert second is first
    exists.assert_awaited_once()
    download.assert_awaited_once()


async def test_load_index_evicts_least_recently_used_entry(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: object,
) -> None:
    content = serialize_artifact_index(
        ArtifactIndex(
            generation="a" * 32,
            archive_available=False,
            pack_size_bytes=1,
            files=[ArtifactFile(path="result.txt", size_bytes=1, offset=0)],
        )
    )
    exists = AsyncMock(return_value=True)
    download = AsyncMock(return_value=content)
    monkeypatch.setattr(artifact_module, "MAX_ARTIFACT_INDEX_CACHE_BYTES", len(content) * 2)
    monkeypatch.setattr(artifact_module, "s3_object_exists", exists)
    monkeypatch.setattr(artifact_module, "download_range_from_s3", download)

    await load_artifact_index("run", "task", "a", aws_runtime)  # type: ignore[arg-type]
    await load_artifact_index("run", "task", "b", aws_runtime)  # type: ignore[arg-type]
    await load_artifact_index("run", "task", "a", aws_runtime)  # type: ignore[arg-type]
    await load_artifact_index("run", "task", "c", aws_runtime)  # type: ignore[arg-type]
    await load_artifact_index("run", "task", "b", aws_runtime)  # type: ignore[arg-type]

    assert exists.await_count == 4
    assert download.await_count == 4


def test_declared_outputs_cannot_use_internal_artifact_namespace() -> None:
    with pytest.raises(ValueError, match="reserved .valkyrie"):
        AgentContractRequest(
            name="agent",
            output_artifacts=[".valkyrie/index.json"],
        )
    with pytest.raises(ValueError, match="reserved agent_output"):
        AgentContractRequest(
            name="agent",
            output_artifacts=["agent_output/result.json"],
        )


def test_list_files_returns_direct_children_with_opaque_pagination() -> None:
    index = ArtifactIndex(
        generation="a" * 32,
        archive_available=False,
        pack_size_bytes=3,
        files=[
            ArtifactFile(path="agent_output/a.txt", size_bytes=1, offset=0),
            ArtifactFile(path="agent_output/nested/b.txt", size_bytes=1, offset=1),
            ArtifactFile(path="declared.json", size_bytes=1, offset=2),
        ],
    )

    first, cursor = list_artifact_files(index, "", None, 1)
    second, next_cursor = list_artifact_files(index, "", cursor, 2)
    nested, _ = list_artifact_files(index, "agent_output", None, 10)

    assert [(item.kind, item.path) for item in first] == [("directory", "agent_output")]
    assert decode_artifact_cursor(cursor) == 1
    assert [(item.kind, item.path) for item in second] == [("file", "declared.json")]
    assert next_cursor is None
    assert [(item.kind, item.path) for item in nested] == [
        ("directory", "agent_output/nested"),
        ("file", "agent_output/a.txt"),
    ]


async def test_read_content_uses_one_exact_bounded_range(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: object,
) -> None:
    file = ArtifactFile(
        path="trajectory.jsonl",
        size_bytes=artifact_module.ARTIFACT_CONTENT_PAGE_BYTES + 4,
        offset=17,
    )
    content = b"a" * (artifact_module.ARTIFACT_CONTENT_PAGE_BYTES - 1) + "€".encode() + b"z"
    download = AsyncMock(return_value=content)
    monkeypatch.setattr(artifact_module, "download_range_from_s3", download)

    page = await read_artifact_content(
        "run",
        "task",
        "abc",
        "a" * 32,
        file,
        None,
        aws_runtime,  # type: ignore[arg-type]
    )

    assert page.data == content[: artifact_module.ARTIFACT_CONTENT_PAGE_BYTES]
    assert decode_artifact_cursor(page.next_cursor) == artifact_module.ARTIFACT_CONTENT_PAGE_BYTES
    download.assert_awaited_once_with(
        f"benchmarks/run/task/.valkyrie/artifacts/abc/generations/{'a' * 32}/content.pack",
        17,
        17 + artifact_module.ARTIFACT_CONTENT_PAGE_BYTES - 1,
        aws_runtime,
    )


async def test_read_content_returns_binary_bytes_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: object,
) -> None:
    file = ArtifactFile(path="image.png", size_bytes=3, offset=4)
    download = AsyncMock(return_value=b"\x89\x00\xff")
    monkeypatch.setattr(artifact_module, "download_range_from_s3", download)

    page = await read_artifact_content(
        "run",
        "task",
        "abc",
        "a" * 32,
        file,
        None,
        aws_runtime,  # type: ignore[arg-type]
    )

    assert page.data == b"\x89\x00\xff"
    assert page.next_cursor is None
    download.assert_awaited_once()
