"""Run artifact metadata and download links."""

from datetime import datetime

from valkyrie.sdk.models._base import ResponseModel


class RunArtifactEntry(ResponseModel):
    """An object stored beneath one run's artifact directory."""

    path: str
    size: int
    last_modified: datetime | None = None


class RunArtifactsResponse(ResponseModel):
    """One page of run artifacts."""

    artifacts: list[RunArtifactEntry]
    next_cursor: str | None = None


class RunArtifactDownloadResponse(ResponseModel):
    """A temporary URL for a single run artifact."""

    path: str
    download_url: str
    expires_in: int
    size: int
