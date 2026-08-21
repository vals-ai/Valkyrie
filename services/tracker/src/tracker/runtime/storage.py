"""Provider-neutral object storage capabilities used by Tracker and the CLI."""

from collections.abc import AsyncIterable, AsyncIterator, Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class StoredObject:
    """Metadata returned while listing stored objects."""

    key: str
    last_modified: datetime | None = None


@dataclass(frozen=True)
class StoredObjectCopy:
    """Opaque provider identity for precisely deleting a copied object."""

    deletion_token: str | None


class ObjectReadSession(Protocol):
    """Related object reads performed within one provider-owned resource scope."""

    def get_bytes(self, key: str) -> Coroutine[Any, Any, bytes]:
        raise NotImplementedError  # pragma: no cover

    def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        raise NotImplementedError  # pragma: no cover


class ObjectStore(Protocol):
    """Object transfers using opaque, store-relative keys and prefixes."""

    def read_session(self) -> AbstractAsyncContextManager[ObjectReadSession]:
        """Open one resource scope for related listings and object reads."""
        raise NotImplementedError  # pragma: no cover

    async def put_bytes(self, key: str, content: bytes) -> None:
        raise NotImplementedError  # pragma: no cover

    async def put_stream(
        self,
        key: str,
        chunks: AsyncIterable[bytes],
        *,
        should_continue: Callable[[], bool] | None = None,
    ) -> int:
        """Store all chunks and return their total byte count."""
        raise NotImplementedError  # pragma: no cover

    async def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError  # pragma: no cover

    def get_many(self, keys: AsyncIterable[str]) -> AsyncIterator[tuple[str, bytes]]:
        raise NotImplementedError  # pragma: no cover

    async def delete(self, key: str, *, deletion_token: str | None = None) -> None:
        raise NotImplementedError  # pragma: no cover

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        raise NotImplementedError  # pragma: no cover

    async def exists(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

    def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        raise NotImplementedError  # pragma: no cover

    async def temporary_download_url(self, key: str, *, expires_in: int) -> str:
        raise NotImplementedError  # pragma: no cover


class ArtifactLocations(Protocol):
    """Provider-native artifact locations for display or navigation."""

    def object_location(self, key: str) -> str:
        raise NotImplementedError  # pragma: no cover

    def prefix_location(self, prefix: str) -> str:
        raise NotImplementedError  # pragma: no cover
