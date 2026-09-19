"""Filesystem artifacts with atomic publication and operation-scoped cleanup."""

import asyncio
import tempfile
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Callable
from contextlib import ExitStack, asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, ParamSpec, TypeVar

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.storage import StoredObject, StoredObjectCopy

P = ParamSpec("P")
T = TypeVar("T")


def local_path(root: Path, key: str, *, prefix: bool = False) -> Path:
    """Resolve a store key without allowing traversal or escaped symlinks."""
    parts = PurePosixPath(key).parts
    if (not key and not prefix) or key.startswith("/") or ".." in parts or "\x00" in key:
        raise ValueError("Local artifact keys must be relative paths without traversal")
    if parts and parts[0] == ".valkyrie":
        raise ValueError("Local artifact key uses the reserved metadata directory")
    root = root.resolve()
    path = (root / key).resolve()
    if not path.is_relative_to(root) or (path == root and not prefix):
        raise ValueError("Local artifact path escapes its storage directory")
    return path


async def _io(operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    # Do not close or unlink a temporary file while a cancelled thread still uses it.
    return await finish_cleanup(asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs)))


class FilesystemObjectStore:
    """Store complete files under stable keys in a shared local directory."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("Local artifact root must be absolute")
        self.root = root.resolve()

    @asynccontextmanager
    async def _staging_file(self) -> AsyncGenerator[tuple[BinaryIO, Path]]:
        stack = ExitStack()

        def create() -> tuple[BinaryIO, Path]:
            staging = self.root / ".valkyrie" / "staging"
            staging.mkdir(parents=True, exist_ok=True)
            directory = stack.enter_context(tempfile.TemporaryDirectory(dir=staging))
            temporary = Path(directory) / "upload"
            return stack.enter_context(temporary.open("wb")), temporary

        try:
            yield await _io(create)
        finally:
            await _io(stack.close)

    def _publish(self, stream: BinaryIO, temporary: Path, key: str, should_continue: Callable[[], bool] | None) -> None:
        stream.close()
        if should_continue is not None and not should_continue():
            raise ExecutionAuthorityRevoked("Local stream upload authority was revoked")
        destination = local_path(self.root, key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary.replace(destination)

    async def put_bytes(self, key: str, content: bytes) -> None:
        async def chunks() -> AsyncIterator[bytes]:
            yield content

        await self.put_stream(key, chunks())

    async def put_stream(
        self,
        key: str,
        chunks: AsyncIterable[bytes],
        *,
        should_continue: Callable[[], bool] | None = None,
    ) -> int:
        await asyncio.to_thread(local_path, self.root, key)
        async with self._staging_file() as (stream, temporary):
            size = 0
            async for chunk in chunks:
                if should_continue is not None and not should_continue():
                    raise ExecutionAuthorityRevoked("Local stream upload authority was revoked")
                await _io(stream.write, chunk)
                size += len(chunk)
            await _io(self._publish, stream, temporary, key, should_continue)
            return size

    async def get_bytes(self, key: str) -> bytes:
        def read() -> bytes:
            return local_path(self.root, key).read_bytes()

        return await asyncio.to_thread(read)

    async def get_many(self, keys: AsyncIterable[str]) -> AsyncIterator[tuple[str, bytes]]:
        async for key in keys:
            try:
                content = await self.get_bytes(key)
            except FileNotFoundError:
                continue
            yield key, content

    async def delete(self, key: str, *, deletion_token: str | None = None) -> None:
        await _io(lambda: local_path(self.root, key).unlink(missing_ok=True))

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        # Admission copies into a new run ID; local files do not have versions.
        await self.put_bytes(destination_key, await self.get_bytes(source_key))
        return StoredObjectCopy(deletion_token=None)

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(lambda: local_path(self.root, key).is_file())

    async def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        def list_files() -> list[StoredObject]:
            local_path(self.root, prefix, prefix=True)
            objects: list[StoredObject] = []
            directory = self.root / prefix.rpartition("/")[0]
            for path in sorted(directory.rglob("*")):
                key = path.relative_to(self.root).as_posix()
                if key == ".valkyrie" or key.startswith(".valkyrie/") or not key.startswith(prefix):
                    continue
                checked = local_path(self.root, key)
                try:
                    if checked.is_file():
                        stat = checked.stat()
                        objects.append(StoredObject(key, datetime.fromtimestamp(stat.st_mtime, UTC), size=stat.st_size))
                except FileNotFoundError:
                    continue
            return objects

        for stored in await asyncio.to_thread(list_files):
            yield stored

    async def stat(self, key: str) -> StoredObject:
        def metadata() -> StoredObject:
            path = local_path(self.root, key)
            if not path.is_file():
                raise FileNotFoundError(key)
            stat = path.stat()
            return StoredObject(key, datetime.fromtimestamp(stat.st_mtime, UTC), size=stat.st_size)

        return await asyncio.to_thread(metadata)

    async def list_objects_page(
        self, prefix: str, *, cursor: str | None, limit: int
    ) -> tuple[list[StoredObject], str | None]:
        if cursor is not None and not cursor.startswith(prefix):
            raise ValueError("Artifact cursor does not match the requested prefix")
        entries: list[StoredObject] = []
        async for stored in self.list_objects(prefix):
            if cursor is not None and stored.key <= cursor:
                continue
            if len(entries) == limit:
                return entries, entries[-1].key
            entries.append(stored)
        return entries, None

    def maximum_download_ttl(self, requested: int) -> int:
        return 0

    async def temporary_download_url(self, key: str, *, expires_in: int) -> None:
        return None

    def object_location(self, key: str) -> str:
        return str(local_path(self.root, key))

    def prefix_location(self, prefix: str) -> str:
        return str(local_path(self.root, prefix, prefix=True))
