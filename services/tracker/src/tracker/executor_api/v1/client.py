"""Version-one client shipped with each immutable executor package."""

from uuid import UUID
from typing import TypeVar
from datetime import datetime
from collections.abc import Callable
from contextlib import AbstractContextManager

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
from tracker.executor_api.v1.task_schemas import Mutation, TaskAttemptRequest, TaskWriteRequest, TaskWriteResponse
from tracker.executor_api.v1.finalization_schemas import (
    Finalization,
    FinalizationResponse,
    FinalizeRequest,
    FinalizeResponse,
)
from tracker.executor_api.v1.queue_schemas import (
    ReleasePoolRequest,
    ReleasePoolResponse,
    ReservePoolRequest,
    ReservePoolResponse,
)

Response = TypeVar("Response", bound=BaseModel)


class ExecutorClient:
    """Keep one claimant id for this process, including every retry of its claim."""

    def __init__(self, transport: ExecutorTransport, dispatch_id: UUID, claimant_id: UUID) -> None:
        self._transport = transport
        self._path = f"/internal/executor/v1/dispatches/{dispatch_id}"
        self._claimant_id = claimant_id

    def retry_until(self, deadline: Callable[[], float]) -> AbstractContextManager[None]:
        """Keep replay-safe writes pending through outages while this process retains a lease."""
        return self._transport.retry_until(deadline)

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

    async def claim_task(self, task_id: UUID, started_at: datetime, *, command_id: UUID) -> TaskWriteResponse:
        return await self._post(
            f"tasks/{task_id}/claim",
            TaskAttemptRequest(claimant_id=self._claimant_id, command_id=command_id, expected_started_at=started_at),
            TaskWriteResponse,
        )

    async def finalization_state(self) -> FinalizationResponse:
        return await self._post(
            "run/finalization", DispatchRequest(claimant_id=self._claimant_id), FinalizationResponse
        )

    async def finalize_run(
        self, snapshot_digest: str, finalization: Finalization, *, command_id: UUID
    ) -> FinalizeResponse:
        return await self._post(
            "run/finalize",
            FinalizeRequest(
                claimant_id=self._claimant_id,
                command_id=command_id,
                snapshot_digest=snapshot_digest,
                finalization=finalization,
            ),
            FinalizeResponse,
        )

    async def write_task(
        self, task_id: UUID, started_at: datetime, mutation: Mutation, *, command_id: UUID, expected_revision: int
    ) -> TaskWriteResponse:
        return await self._post(
            f"tasks/{task_id}/write",
            TaskWriteRequest(
                claimant_id=self._claimant_id,
                command_id=command_id,
                expected_started_at=started_at,
                expected_revision=expected_revision,
                mutation=mutation,
            ),
            TaskWriteResponse,
        )

    async def reserve_pool(
        self, task_id: UUID, started_at: datetime, *, command_id: UUID, expected_revision: int
    ) -> ReservePoolResponse:
        return await self._post(
            f"tasks/{task_id}/queue/reserve",
            ReservePoolRequest(
                claimant_id=self._claimant_id,
                command_id=command_id,
                expected_started_at=started_at,
                expected_revision=expected_revision,
            ),
            ReservePoolResponse,
        )

    async def release_pool(
        self, task_id: UUID, started_at: datetime, reservation_id: UUID, *, command_id: UUID
    ) -> ReleasePoolResponse:
        """Release only after the provider operation and any required cleanup have settled."""
        return await self._post(
            f"tasks/{task_id}/queue/release",
            ReleasePoolRequest(
                claimant_id=self._claimant_id,
                command_id=command_id,
                expected_started_at=started_at,
                reservation_id=reservation_id,
            ),
            ReleasePoolResponse,
        )
