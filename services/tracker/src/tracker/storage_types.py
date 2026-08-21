"""Wire models for Tracker-mediated storage operations."""

from pydantic import BaseModel, Field


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


class BenchmarkOutputKeysResponse(BaseModel):
    prefix: str
    keys: list[str]


class OutputURLsRequest(BaseModel):
    keys: list[str] = Field(min_length=1, max_length=8)


class BenchmarkOutputURLsResponse(BaseModel):
    files: list[OutputURLEntry]
    expires_in: int
