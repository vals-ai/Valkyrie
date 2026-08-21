"""Provider-neutral access to immutable executor artifacts."""

from contextlib import AbstractContextManager
from typing import BinaryIO, Protocol


class ExecutorArtifactReader(Protocol):
    """Open a release artifact as a closed-on-exit binary stream."""

    def open(self, bucket: str, key: str) -> AbstractContextManager[BinaryIO]:
        """Yield the complete artifact stream and close it on every exit."""
        raise NotImplementedError
