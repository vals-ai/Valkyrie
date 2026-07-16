"""Models for benchmark-service discovery and task selection."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from valkyrie.sdk.models._base import ResponseModel


class BenchmarkServiceEntry(ResponseModel):
    """One benchmark service available through the hosted catalog."""

    name: str
    url: str
    auth_header_name: str | None = None
    auth_secret_name: str | None = None

    @field_validator("url")
    @classmethod
    def normalize_url(cls, value: str) -> str:
        """Remove trailing slashes from the service base URL."""
        return value.rstrip("/")


class BenchmarkServiceHealth(ResponseModel):
    """Health-check result for one benchmark service."""

    name: str
    url: str
    healthy: bool
    latency_ms: int | None
    error: str | None = None


class BenchmarkServicesResponse(ResponseModel):
    """Health-check results for benchmark services."""

    services: list[BenchmarkServiceHealth]


class BenchmarkServiceCatalogResponse(ResponseModel):
    """Benchmark services visible to the configured tenant."""

    services: list[BenchmarkServiceEntry]


class BenchmarkServicesRequest(BaseModel):
    """Benchmark services to health-check through Tracker."""

    services: list[BenchmarkServiceEntry] = Field(default_factory=list)


class FetchBenchmarkTasksRequest(BaseModel):
    """Wire payload for discovering task IDs from a benchmark service."""

    benchmark_name: str
    dataset: str | None = None
    custom_benchmark_service: str | None = None
    service_headers: dict[str, str] = Field(default_factory=dict, repr=False)


class TaskIDsResponse(ResponseModel):
    """Task IDs returned by a benchmark service through Tracker."""

    task_ids: list[str]
