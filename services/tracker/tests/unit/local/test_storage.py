"""Real filesystem coverage for publication, freezing, and cancellation."""

import asyncio
import tempfile
import threading
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.local.storage import FilesystemObjectStore
from tracker.runtime.artifacts import copy_agent_to_benchmark


async def test_publish_list_read_and_locations(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "artifacts")
    await store.put_bytes("agents/example.zip", b"bundle")
    await store.put_bytes("agents/other.zip", b"other")
    async with store.read_session() as reader:
        assert await reader.get_bytes("agents/example.zip") == b"bundle"
        assert [item.key async for item in reader.list_objects("agents/ex")] == ["agents/example.zip"]
    assert len([item async for item in store.list_objects("")]) == 2
    assert store.object_location("agents/example.zip") == str(store.root / "agents/example.zip")
    assert store.prefix_location("agents/") == str(store.root / "agents")
    await store.delete("agents/other.zip")
    assert not await store.exists("agents/other.zip")

    async def keys() -> AsyncIterator[str]:
        yield "agents/example.zip"
        yield "agents/other.zip"

    assert [item async for item in store.get_many(keys())] == [("agents/example.zip", b"bundle")]


async def test_failed_upload_preserves_complete_file(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    await store.put_bytes("output", b"previous")

    async def interrupted() -> AsyncIterator[bytes]:
        yield b"partial"
        assert await store.get_bytes("output") == b"previous"
        raise ConnectionError("upload interrupted")

    with pytest.raises(ConnectionError):
        await store.put_stream("output", interrupted())
    assert await store.get_bytes("output") == b"previous"
    assert not list((tmp_path / ".valkyrie/staging").iterdir())


async def test_cancelled_upload_removes_only_its_staging_file(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    started = asyncio.Event()

    async def interrupted() -> AsyncIterator[bytes]:
        yield b"partial"
        started.set()
        await asyncio.Event().wait()

    upload = asyncio.create_task(store.put_stream("output", interrupted()))
    await started.wait()
    await store.put_bytes("output", b"another writer")
    upload.cancel()
    with pytest.raises(asyncio.CancelledError):
        await upload
    assert await store.get_bytes("output") == b"another writer"
    assert not list((tmp_path / ".valkyrie/staging").iterdir())


async def test_cancellation_during_staging_creation_cleans_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FilesystemObjectStore(tmp_path)
    created = threading.Event()
    release = threading.Event()
    temporary_directory = tempfile.TemporaryDirectory

    def delayed_directory(*, dir: Path) -> tempfile.TemporaryDirectory[str]:
        directory = temporary_directory(dir=dir)
        created.set()
        assert release.wait(timeout=5)
        return directory

    monkeypatch.setattr(tempfile, "TemporaryDirectory", delayed_directory)
    upload = asyncio.create_task(store.put_bytes("output", b"incomplete"))
    try:
        assert await asyncio.to_thread(created.wait, 5)
        upload.cancel()
        await asyncio.sleep(0)
        upload.cancel()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await upload
    assert not await store.exists("output")
    assert not list((tmp_path / ".valkyrie/staging").iterdir())


async def test_revoked_upload_cannot_publish(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    await store.put_bytes("output", b"previous")
    permitted = True

    async def chunks() -> AsyncIterator[bytes]:
        nonlocal permitted
        yield b"new"
        permitted = False

    with pytest.raises(ExecutionAuthorityRevoked):
        await store.put_stream("output", chunks(), should_continue=lambda: permitted)
    assert await store.get_bytes("output") == b"previous"


async def test_frozen_bundle_and_failed_admission_cleanup(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    await store.put_bytes("agents/example.zip", b"first")
    created = await copy_agent_to_benchmark(store, "run", "example")
    assert created is not None
    await store.put_bytes("agents/example.zip", b"second")
    assert await copy_agent_to_benchmark(store, "run", "example") is None
    key = "benchmarks/run/example.zip"
    assert await store.get_bytes(key) == b"first"
    await store.delete(key, deletion_token=created.deletion_token)
    assert not await store.exists(key)


@pytest.mark.parametrize("key", ["../outside", "/absolute", "a/../../outside", ".valkyrie/staging/entry", "", "."])
async def test_rejects_invalid_keys(tmp_path: Path, key: str) -> None:
    store = FilesystemObjectStore(tmp_path)
    with pytest.raises(ValueError):
        await store.put_bytes(key, b"invalid")


async def test_rejects_symlink_escape(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError):
        await FilesystemObjectStore(root).put_bytes("escape/outside", b"invalid")
    assert not (tmp_path / "outside").exists()
