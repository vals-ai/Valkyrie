"""Real filesystem coverage for publication, freezing, and cancellation."""

import asyncio
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


async def test_frozen_bundle_and_conditional_cleanup(tmp_path: Path) -> None:
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

    created = await store.copy("agents/example.zip", key)
    await store.put_bytes(key, b"replacement")
    await store.delete(key, deletion_token=created.deletion_token)
    assert await store.get_bytes(key) == b"replacement"


async def test_concurrent_copies_cannot_delete_the_winning_bundle(tmp_path: Path) -> None:
    stores = [FilesystemObjectStore(tmp_path), FilesystemObjectStore(tmp_path)]
    await stores[0].put_bytes("agents/example.zip", b"bundle")
    results = await asyncio.gather(*(store.copy("agents/example.zip", "frozen.zip") for store in stores))
    tokens = [result.deletion_token for result in results]
    assert tokens.count("existing") == 1
    await stores[0].delete("frozen.zip", deletion_token="existing")
    assert await stores[0].get_bytes("frozen.zip") == b"bundle"


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
