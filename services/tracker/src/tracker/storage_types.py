"""Wire models for Tracker-mediated storage operations."""

from pydantic import BaseModel


class AgentEntry(BaseModel):
    name: str
    last_modified: str | None = None


class AgentsResponse(BaseModel):
    agents: list[AgentEntry]


class AgentDownloadURLResponse(BaseModel):
    name: str
    download_url: str
    expires_in: int


class AgentUploadURLResponse(BaseModel):
    name: str
    upload_url: str
    expires_in: int


class OutputURLEntry(BaseModel):
    key: str
    download_url: str


class BenchmarkOutputURLsResponse(BaseModel):
    prefix: str
    files: list[OutputURLEntry]
    expires_in: int
