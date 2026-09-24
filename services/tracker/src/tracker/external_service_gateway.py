"""Local External Service Gateway control client and deadline accounting."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import httpx
from pydantic import BaseModel


class AccountingSessionState(StrEnum):
    OPEN = "OPEN"
    ARBITRATING = "ARBITRATING"
    SEALED = "SEALED"


class ArbitrationDecision(StrEnum):
    RESUME = "RESUME"
    SEAL = "SEAL"


class AccountingSessionSnapshot(BaseModel):
    session_id: str
    state: AccountingSessionState
    cumulative_neutral_overhead_ms: int
    revision: int
    accounting_epoch: int


class CreateAccountingSessionRequest(BaseModel):
    session_id: str
    adapter: str
    model: str
    config: dict[str, str]


class ResolveArbitrationRequest(BaseModel):
    decision: ArbitrationDecision


class ExternalServiceGatewayClient:
    """Minimal unauthenticated client for the local gateway control plane."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 5.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = client

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> AccountingSessionSnapshot:
        if self._client is not None:
            response = await self._client.request(method, f"{self.base_url}{path}", json=json)
        else:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.request(method, f"{self.base_url}{path}", json=json)
        response.raise_for_status()
        return AccountingSessionSnapshot.model_validate(response.json())

    async def create_session(
        self,
        *,
        session_id: str,
        model: str,
        config: dict[str, str],
    ) -> AccountingSessionSnapshot:
        request = CreateAccountingSessionRequest(
            session_id=session_id,
            adapter="model_gateway",
            model=model,
            config=config,
        )
        return await self._request("POST", "/sessions", json=request.model_dump())

    async def read_session(self, session_id: str) -> AccountingSessionSnapshot:
        return await self._request("GET", f"/sessions/{session_id}")

    async def begin_arbitration(self, session_id: str) -> AccountingSessionSnapshot:
        return await self._request("POST", f"/sessions/{session_id}/arbitration/begin")

    async def resolve_arbitration(
        self,
        session_id: str,
        decision: ArbitrationDecision,
    ) -> AccountingSessionSnapshot:
        request = ResolveArbitrationRequest(decision=decision)
        return await self._request(
            "POST",
            f"/sessions/{session_id}/arbitration/resolve",
            json=request.model_dump(mode="json"),
        )


@dataclass(frozen=True)
class ExternalServiceAccountingSummary:
    accounting_session_id: str
    base_generation_allowance_seconds: float
    cumulative_time_credit_cap_seconds: float
    external_service_overhead_seconds: float
    external_service_credit_applied_seconds: float
    effective_generation_allowance_seconds: float
    external_service_credit_revision: int


@dataclass
class ExternalServiceDeadlineController:
    """Tracker-owned deadline policy over gateway-owned cumulative observations."""

    client: ExternalServiceGatewayClient
    snapshot: AccountingSessionSnapshot
    base_allowance_seconds: float
    credit_cap_seconds: float

    @property
    def session_id(self) -> str:
        return self.snapshot.session_id

    def applied_credit_seconds(
        self,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> float:
        observed = snapshot or self.snapshot
        return min(
            observed.cumulative_neutral_overhead_ms / 1000,
            self.credit_cap_seconds,
        )

    def effective_allowance_seconds(
        self,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> float:
        return self.base_allowance_seconds + self.applied_credit_seconds(snapshot)

    def deadline(
        self,
        started_at: float,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> float:
        return started_at + self.effective_allowance_seconds(snapshot)

    def _accept(self, snapshot: AccountingSessionSnapshot) -> AccountingSessionSnapshot:
        if snapshot.session_id != self.session_id:
            raise ValueError("Gateway returned a snapshot for a different accounting session")
        self.snapshot = snapshot
        return snapshot

    async def refresh(self) -> AccountingSessionSnapshot:
        return self._accept(await self.client.read_session(self.session_id))

    async def begin_arbitration(self) -> AccountingSessionSnapshot:
        return self._accept(await self.client.begin_arbitration(self.session_id))

    async def resolve(
        self,
        decision: ArbitrationDecision,
    ) -> AccountingSessionSnapshot:
        return self._accept(
            await self.client.resolve_arbitration(self.session_id, decision)
        )

    def summary(
        self,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> ExternalServiceAccountingSummary:
        observed = snapshot or self.snapshot
        applied = self.applied_credit_seconds(observed)
        return ExternalServiceAccountingSummary(
            accounting_session_id=observed.session_id,
            base_generation_allowance_seconds=self.base_allowance_seconds,
            cumulative_time_credit_cap_seconds=self.credit_cap_seconds,
            external_service_overhead_seconds=(
                observed.cumulative_neutral_overhead_ms / 1000
            ),
            external_service_credit_applied_seconds=applied,
            effective_generation_allowance_seconds=(
                self.base_allowance_seconds + applied
            ),
            external_service_credit_revision=observed.revision,
        )
