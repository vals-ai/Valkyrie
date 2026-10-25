"""Tracker-backed projects that runs are started in."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from valkyrie.sdk.models import ProjectCreateRequest, ProjectResponse, ProjectsResponse, SpendType
from valkyrie.sdk.resources._path import encode_path_segment

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class ProjectsResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def create(
        self,
        handle: str,
        *,
        name: str,
        spend_type: SpendType,
        subcategory: str,
        customer: str | None = None,
        extra_tags: Mapping[str, str] | None = None,
    ) -> ProjectResponse:
        """Create a project that runs can be started in; the ``handle`` is unique within the organization."""
        request = ProjectCreateRequest(
            handle=handle,
            name=name,
            spend_type=spend_type,
            subcategory=subcategory,
            customer=customer,
            extra_tags=dict(extra_tags or {}),
        )
        return await self._sdk.request_model("POST", "/projects", ProjectResponse, json=request.model_dump(mode="json"))

    async def list(self) -> ProjectsResponse:
        """List the organization's projects by handle."""
        return await self._sdk.request_model("GET", "/projects", ProjectsResponse)

    async def update(
        self,
        handle: str,
        *,
        name: str,
        spend_type: SpendType,
        subcategory: str,
        customer: str | None = None,
        extra_tags: Mapping[str, str] | None = None,
    ) -> ProjectResponse:
        """Replace a project's shared spend metadata without changing its handle."""
        request = ProjectCreateRequest(
            handle=handle,
            name=name,
            spend_type=spend_type,
            subcategory=subcategory,
            customer=customer,
            extra_tags=dict(extra_tags or {}),
        )
        return await self._sdk.request_model(
            "PUT",
            f"/projects/{encode_path_segment(handle, label='project handle')}",
            ProjectResponse,
            json=request.model_dump(mode="json"),
        )

    async def delete(self, handle: str) -> ProjectResponse:
        """Delete an unused project; Tracker rejects projects with run history."""
        return await self._sdk.request_model(
            "DELETE",
            f"/projects/{encode_path_segment(handle, label='project handle')}",
            ProjectResponse,
        )
