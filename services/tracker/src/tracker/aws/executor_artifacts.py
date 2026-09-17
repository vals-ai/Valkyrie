"""S3 adapter for immutable executor release artifacts."""

from collections.abc import Mapping
from contextlib import AbstractContextManager, closing
from typing import BinaryIO, Protocol, cast
from urllib.parse import urlparse

from executor_protocol import validate_executor_artifact_uri


class S3ExecutorArtifactClient(Protocol):
    """The S3 operation needed to retrieve a sealed executor artifact."""

    def get_object(self, *, Bucket: str, Key: str) -> Mapping[str, object]: ...


class S3ExecutorArtifactReader:
    """Read an artifact through an already-selected S3 client."""

    def __init__(
        self,
        client: S3ExecutorArtifactClient | None = None,
        *,
        expected_bucket: str | None = None,
        expected_prefix: str | None = None,
    ) -> None:
        self._client = client
        self._expected_bucket = expected_bucket
        self._expected_prefix = expected_prefix

    def validate(self, artifact_uri: str) -> None:
        parsed = urlparse(artifact_uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/") or parsed.query or parsed.fragment:
            raise ValueError("Executor artifact URI must identify an S3 object")
        if self._expected_bucket is not None or self._expected_prefix is not None:
            validate_executor_artifact_uri(artifact_uri, self._expected_bucket or "", self._expected_prefix or "")

    def open(self, artifact_uri: str) -> AbstractContextManager[BinaryIO]:
        self.validate(artifact_uri)
        parsed = urlparse(artifact_uri)
        client = self._client
        if client is None:
            import boto3

            client = cast(S3ExecutorArtifactClient, boto3.client("s3"))  # pyright: ignore[reportUnknownMemberType]
        response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        return closing(cast(BinaryIO, response["Body"]))
