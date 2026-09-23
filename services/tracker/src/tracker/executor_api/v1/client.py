"""Version-one client shipped with each immutable executor package."""

from uuid import UUID
from typing import TypeVar

from pydantic import BaseModel

from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.schemas import (
    AuthorityResponse,
    ClaimRequest,
    DispatchRequest,
    FailRequest,
    LeaseResponse,
    RunStateResponse,
    RunTasksRequest,
    TerminalResponse,
)

Response = TypeVar("Response", bound=BaseModel)


class ExecutorClient:
    """Keep one claimant id for this process, including every retry of its claim."""

    def __init__(self, transport: ExecutorTransport, dispatch_id: UUID, claimant_id: UUID) -> None:
        self._transport = transport
        self._path = f"/internal/executor/v1/dispatches/{dispatch_id}"
        self._claimant_id = claimant_id

    async def _post(self, operation: str, request: BaseModel, response_type: type[Response]) -> Response:
        response = await self._transport.post(f"{self._path}/{operation}", request.model_dump(mode="json"))

        return response_type.model_validate(response.json())

    async def claim(self, request: ClaimRequest) -> LeaseResponse:
        if request.claimant_id != self._claimant_id:
            raise ValueError("Claim request must use this process's claimant id")

        return await self._post("claim", request, LeaseResponse)

    async def authority(self) -> AuthorityResponse:
        return await self._post("authority", DispatchRequest(claimant_id=self._claimant_id), AuthorityResponse)

    async def heartbeat(self) -> LeaseResponse:
        return await self._post("heartbeat", DispatchRequest(claimant_id=self._claimant_id), LeaseResponse)

    async def finish(self) -> TerminalResponse:
        return await self._post("finish", DispatchRequest(claimant_id=self._claimant_id), TerminalResponse)

    async def fail(self, error_message: str) -> TerminalResponse:
        return await self._post(
            "fail", FailRequest(claimant_id=self._claimant_id, error_message=error_message), TerminalResponse
        )

    async def initialize_run_tasks(self, task_ids: list[str]) -> RunStateResponse:
        """Ensure one assigned batch exists without resetting existing task attempts."""
        return await self._post(
            "run/initialize",
            RunTasksRequest(claimant_id=self._claimant_id, task_ids=task_ids, include_eval_resume_state=True),
            RunStateResponse,
        )

    async def run_state(self, task_ids: list[str]) -> RunStateResponse:
        """Read status and attempt timestamps for one assigned batch."""
        return await self._post(
            "run/state", RunTasksRequest(claimant_id=self._claimant_id, task_ids=task_ids), RunStateResponse
        )
