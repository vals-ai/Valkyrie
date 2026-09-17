"""Provider-neutral access to immutable executor artifacts."""

from contextlib import AbstractContextManager
from typing import BinaryIO, Protocol


class ExecutorArtifactReader(Protocol):
    """Open a release artifact as a closed-on-exit binary stream."""

    def validate(self, artifact_uri: str) -> None:
        """Reject locations outside the configured release storage."""
        raise NotImplementedError

    def open(self, artifact_uri: str) -> AbstractContextManager[BinaryIO]:
        """Yield the complete artifact stream and close it on every exit."""
        raise NotImplementedError
