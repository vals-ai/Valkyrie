"""Filesystem release reader shared by Tracker and the minimal ExecutorHost."""

from io import BufferedReader
from pathlib import Path
from urllib.parse import unquote, urlparse


class FilesystemExecutorArtifactReader:
    """Open only local executor artifacts inside the installation's release root."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("Local executor release root must be absolute")
        self.root = root.resolve()

    def _path(self, artifact_uri: str) -> Path:
        parsed = urlparse(artifact_uri)
        if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("Local executor artifact URI must use file:///absolute/path")
        path = Path(unquote(parsed.path))
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError("Local executor artifact URI must be absolute without traversal")
        path = path.resolve()
        if not path.is_relative_to(self.root) or path == self.root:
            raise ValueError("Executor artifact URI is outside the configured local release root")
        return path

    def validate(self, artifact_uri: str) -> None:
        self._path(artifact_uri)

    def open(self, artifact_uri: str) -> BufferedReader:
        return self._path(artifact_uri).open("rb")
