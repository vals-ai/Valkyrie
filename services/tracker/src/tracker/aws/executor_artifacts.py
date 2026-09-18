"""S3 adapter for immutable executor release artifacts."""

from collections.abc import Mapping
from contextlib import AbstractContextManager, closing
from typing import BinaryIO, Protocol, cast


class S3ExecutorArtifactClient(Protocol):
    """The S3 operation needed to retrieve a sealed executor artifact."""

    def get_object(self, *, Bucket: str, Key: str) -> Mapping[str, object]: ...


class S3ExecutorArtifactReader:
    """Read an artifact through an already-selected S3 client."""

    def __init__(self, client: S3ExecutorArtifactClient) -> None:
        self._client = client

    def open(self, bucket: str, key: str) -> AbstractContextManager[BinaryIO]:
        response = self._client.get_object(Bucket=bucket, Key=key)
        return closing(cast(BinaryIO, response["Body"]))
