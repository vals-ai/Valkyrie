"""Read-only sandbox scheduler operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from valkyrie.sdk.models.scheduler import SchedulerOverviewResponse

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class SchedulerResource:
    """Inspect queue positions and active tasks in the authenticated organization."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def overview(
        self,
        *,
        waiting_limit: int = 100,
        active_limit: int = 100,
        waiting_offset: int = 0,
        active_offset: int = 0,
        include_capacity: bool = False,
    ) -> SchedulerOverviewResponse:
        """Fetch live totals and up to 1–200 entries per waiting or active page.

        Offsets are nonnegative integers, defaulting to zero. Advance each list
        with its waiting_next_offset or active_next_offset until null. Capped
        flags indicate more entries after this page. Totals are not capped.
        Pages are live: queue changes between calls can repeat or skip entries.
        Capacity is observational and returned only when requested.
        """
        for name, value in (("waiting_limit", waiting_limit), ("active_limit", active_limit)):
            if type(value) is not int or not 1 <= value <= 200:
                raise ValueError(f"{name} must be an integer from 1 to 200")
        for name, value in (("waiting_offset", waiting_offset), ("active_offset", active_offset)):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if type(include_capacity) is not bool:
            raise ValueError("include_capacity must be a boolean")

        params: dict[str, int | bool] = {
            "waiting_limit": waiting_limit,
            "active_limit": active_limit,
            "waiting_offset": waiting_offset,
            "active_offset": active_offset,
        }
        if include_capacity:
            params["include_capacity"] = True

        return await self._sdk.request_model(
            "GET",
            "/scheduler/overview",
            SchedulerOverviewResponse,
            params=params,
        )
