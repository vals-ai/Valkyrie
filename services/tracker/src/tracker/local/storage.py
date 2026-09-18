"""Filesystem artifacts with atomic publication and operation-scoped cleanup."""

import asyncio
import fcntl
import os
import shutil
import tempfile
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Callable, Generator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, ParamSpec, TypeVar

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.storage import ObjectReadSession, StoredObject, StoredObjectCopy

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
    async def read_session(self) -> AsyncGenerator[ObjectReadSession]:
        yield self

    @contextmanager
    def _lock(self) -> Generator[None]:
        metadata = self.root / ".valkyrie"
        metadata.mkdir(parents=True, exist_ok=True)
        with (metadata / "write.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def _temporary_file(self) -> tuple[BinaryIO, Path]:
        staging = self.root / ".valkyrie" / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(dir=staging)
        return os.fdopen(descriptor, "wb"), Path(name)

    @asynccontextmanager
    async def _staging_file(self) -> AsyncGenerator[tuple[BinaryIO, Path]]:
        pending = asyncio.create_task(asyncio.to_thread(self._temporary_file))
        try:
            yield await asyncio.shield(pending)
        finally:

            async def cleanup() -> None:
                stream, temporary = await pending
                try:
                    await _io(stream.close)
                finally:
                    await _io(temporary.unlink, missing_ok=True)

            await finish_cleanup(asyncio.create_task(cleanup()))

    def _publish(self, stream: BinaryIO, temporary: Path, key: str, should_continue: Callable[[], bool] | None) -> None:
        stream.close()
        with self._lock():
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
        await _io(local_path, self.root, key)
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

        return await _io(read)

    async def get_many(self, keys: AsyncIterable[str]) -> AsyncIterator[tuple[str, bytes]]:
        async for key in keys:
            try:
                content = await self.get_bytes(key)
            except FileNotFoundError:
                continue
            yield key, content

    @staticmethod
    def _deletion_token(path: Path) -> str:
        stat = path.stat()
        return f"{stat.st_dev}:{stat.st_ino}:{stat.st_mtime_ns}:{stat.st_size}"

    async def delete(self, key: str, *, deletion_token: str | None = None) -> None:
        def remove() -> None:
            with self._lock():
                path = local_path(self.root, key)
                if not path.exists():
                    return
                if deletion_token is None or deletion_token == self._deletion_token(path):
                    path.unlink()

        await _io(remove)

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        """Freeze an agent bundle; concurrent admission retains the first copy."""

        def copy() -> StoredObjectCopy:
            with self._lock():
                source = local_path(self.root, source_key)
                destination = local_path(self.root, destination_key)
                if destination.exists():
                    # A failed admission may only delete the copy it created.
                    return StoredObjectCopy(deletion_token="existing")
                stream, temporary = self._temporary_file()
                try:
                    stream.close()
                    shutil.copyfile(source, temporary)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    temporary.replace(destination)
                    return StoredObjectCopy(deletion_token=self._deletion_token(destination))
                finally:
                    temporary.unlink(missing_ok=True)

        return await _io(copy)

    async def exists(self, key: str) -> bool:
        return await _io(lambda: local_path(self.root, key).is_file())

    async def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        def list_files() -> list[StoredObject]:
            local_path(self.root, prefix, prefix=True)
            objects: list[StoredObject] = []
            for path in sorted(self.root.rglob("*")):
                key = path.relative_to(self.root).as_posix()
                if key == ".valkyrie" or key.startswith(".valkyrie/") or not key.startswith(prefix):
                    continue
                checked = local_path(self.root, key)
                try:
                    if checked.is_file():
                        objects.append(StoredObject(key, datetime.fromtimestamp(checked.stat().st_mtime, UTC)))
                except FileNotFoundError:
                    continue
            return objects

        for stored in await _io(list_files):
            yield stored

    async def temporary_download_url(self, key: str, *, expires_in: int) -> None:
        return None

    def object_location(self, key: str) -> str:
        return str(local_path(self.root, key))

    def prefix_location(self, prefix: str) -> str:
        return str(local_path(self.root, prefix, prefix=True))
