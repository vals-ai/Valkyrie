"""Real filesystem coverage for publication, pagination, and cancellation."""

import asyncio
import tempfile
import threading
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.local.storage import FilesystemObjectStore


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


async def test_metadata_and_pagination(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path)
    for key in ("run/a", "run/b", "run/c"):
        await store.put_bytes(key, key.encode())
    first, cursor = await store.list_objects_page("run/", cursor=None, limit=2)
    assert [entry.key for entry in first] == ["run/a", "run/b"]
    assert [entry.size for entry in first] == [5, 5]
    assert cursor == "run/b"
    last, cursor = await store.list_objects_page("run/", cursor=cursor, limit=2)
    assert [entry.key for entry in last] == ["run/c"]
    assert cursor is None
    assert (await store.stat("run/a")).size == 5
    with pytest.raises(FileNotFoundError):
        await store.stat("missing")
    with pytest.raises(ValueError, match="cursor"):
        await store.list_objects_page("other/", cursor="run/b", limit=2)
