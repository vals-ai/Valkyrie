"""Hosted benchmark and task inspection operations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import quote
from uuid import UUID

from valkyrie.sdk.models import (
    BenchmarkStatusResponse,
    FetchTasksRequest,
    SingleBenchmarkResponse,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TasksResponse,
)

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class BenchmarksResource:
    """Async operations for hosted benchmark and task inspection."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def fetch(self, run_id: UUID) -> SingleBenchmarkResponse:
        """Fetch detailed state for one hosted run."""
        return await self._sdk.request_model("GET", f"/benchmarks/{run_id}", SingleBenchmarkResponse)

    async def statuses(self, run_ids: Sequence[UUID]) -> BenchmarkStatusResponse:
        """Fetch lightweight status and task counts for several runs."""
        return await self._sdk.request_model(
            "GET",
            "/benchmarks/status",
            BenchmarkStatusResponse,
            params={"ids": ",".join(str(run_id) for run_id in run_ids)},
        )

    async def tasks(self, run_id: UUID, request: FetchTasksRequest | None = None) -> TasksResponse:
        """Fetch a filtered and paginated page of tasks for one run."""
        resolved_request = request or FetchTasksRequest()
        params: dict[str, Any] = resolved_request.model_dump(exclude_none=True, mode="json")
        if resolved_request.status is not None:
            params["status"] = ",".join(status.value for status in resolved_request.status)
        return await self._sdk.request_model(
            "GET",
            f"/benchmarks/{run_id}/tasks",
            TasksResponse,
            params=params,
        )

    async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse:
        """Fetch detailed state and evaluation output for one task."""
        task_segment = self._task_segment(task_id)
        return await self._sdk.request_model(
            "GET",
            f"/benchmarks/{run_id}/tasks/{task_segment}",
            SingleTaskResponse,
        )

    async def artifacts(self, run_id: UUID, task_id: str) -> TaskArtifactsResponse:
        """Fetch temporary output and log links for one task."""
        task_segment = self._task_segment(task_id)
        return await self._sdk.request_model(
            "GET",
            f"/benchmarks/{run_id}/tasks/{task_segment}/artifacts",
            TaskArtifactsResponse,
        )

    @staticmethod
    def _task_segment(task_id: str) -> str:
        if not task_id.strip():
            raise ValueError("task_id must not be blank")
        if task_id in {".", ".."}:
            raise ValueError("task_id must not be '.' or '..'")
        if "/" in task_id:
            raise ValueError("task_id must not contain '/'")
        return quote(task_id, safe="")
