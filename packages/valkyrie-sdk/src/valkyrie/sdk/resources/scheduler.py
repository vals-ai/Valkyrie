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

    async def overview(self, *, waiting_limit: int = 100, active_limit: int = 100) -> SchedulerOverviewResponse:
        """Fetch totals and up to 1–200 entries per waiting or active list.

        Totals are not capped. Check waiting_capped and active_capped before
        treating the returned entries as a complete inventory.
        """
        for name, value in (("waiting_limit", waiting_limit), ("active_limit", active_limit)):
            if type(value) is not int or not 1 <= value <= 200:
                raise ValueError(f"{name} must be an integer from 1 to 200")

        return await self._sdk.request_model(
            "GET",
            "/scheduler/overview",
            SchedulerOverviewResponse,
            params={"waiting_limit": waiting_limit, "active_limit": active_limit},
        )
