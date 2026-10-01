"""Local External Service Gateway control client and deadline accounting."""

from dataclasses import dataclass
from enum import StrEnum
import time
from typing import Any

import httpx

from pydantic import BaseModel, Field


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
    generation_active: bool
    interval_index: int


class CreateAccountingSessionRequest(BaseModel):
    session_id: str


class GenerationIntervalRequest(BaseModel):
    interval_index: int = Field(gt=0)


class ResolveArbitrationRequest(BaseModel):
    decision: ArbitrationDecision


class ExternalServiceGatewayClient:
    """Authenticated Tracker-only client for gateway session controls."""

    def __init__(
        self,
        base_url: str,
        *,
        control_token: str,
        timeout_seconds: float = 5.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        if not control_token:
            raise ValueError("Gateway control token is required")
        self.control_token = control_token
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
            response = await self._client.request(
                method,
                f"{self.base_url}{path}",
                json=json,
                headers={"X-SSP-Control-Token": self.control_token},
            )
        else:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.request(
                    method,
                    f"{self.base_url}{path}",
                    json=json,
                    headers={"X-SSP-Control-Token": self.control_token},
                )
        response.raise_for_status()
        return AccountingSessionSnapshot.model_validate(response.json())

    async def create_session(self, *, session_id: str) -> AccountingSessionSnapshot:
        request = CreateAccountingSessionRequest(session_id=session_id)
        return await self._request("POST", "/sessions", json=request.model_dump())

    async def begin_generation(self, session_id: str, interval_index: int) -> AccountingSessionSnapshot:
        request = GenerationIntervalRequest(interval_index=interval_index)
        return await self._request("POST", f"/sessions/{session_id}/generation/begin", json=request.model_dump())

    async def end_generation(self, session_id: str, interval_index: int) -> AccountingSessionSnapshot:
        request = GenerationIntervalRequest(interval_index=interval_index)
        return await self._request("POST", f"/sessions/{session_id}/generation/end", json=request.model_dump())

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
    """Tracker-owned cumulative active-time policy over optional gateway credit."""

    base_allowance_seconds: float
    credit_cap_seconds: float = 0.0
    client: ExternalServiceGatewayClient | None = None
    snapshot: AccountingSessionSnapshot | None = None
    active_elapsed_seconds: float = 0.0
    active_since: float | None = None
    interval_index: int = 0

    @property
    def session_id(self) -> str:
        if self.snapshot is None:
            raise ValueError("No gateway accounting session is configured")
        return self.snapshot.session_id

    def applied_credit_seconds(
        self,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> float:
        observed = snapshot or self.snapshot
        return min(
            (observed.cumulative_neutral_overhead_ms / 1000) if observed is not None else 0.0,
            self.credit_cap_seconds,
        )

    def effective_allowance_seconds(
        self,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> float:
        return self.base_allowance_seconds + self.applied_credit_seconds(snapshot)

    def elapsed_seconds(self, now: float) -> float:
        return self.active_elapsed_seconds + (
            max(0.0, now - self.active_since) if self.active_since is not None else 0.0
        )

    def deadline(self, now: float, snapshot: AccountingSessionSnapshot | None = None) -> float:
        return now + max(0.0, self.effective_allowance_seconds(snapshot) - self.elapsed_seconds(now))

    async def begin_generation(self, now: float | None = None) -> None:
        if self.active_since is not None:
            raise ValueError("Generation interval already active")
        index = self.interval_index + 1
        if self.client is not None:
            self._accept(await self.client.begin_generation(self.session_id, index))
        self.interval_index = index
        self.active_since = time.monotonic() if now is None else now

    async def end_generation(self, now: float | None = None) -> None:
        if self.active_since is None:
            raise ValueError("No generation interval is active")
        ended_at = time.monotonic() if now is None else now
        self.active_elapsed_seconds += max(0.0, ended_at - self.active_since)
        self.active_since = None
        if (
            self.client is not None
            and self.snapshot is not None
            and self.snapshot.state != AccountingSessionState.SEALED
        ):
            self._accept(await self.client.end_generation(self.session_id, self.interval_index))

    def _accept(self, snapshot: AccountingSessionSnapshot) -> AccountingSessionSnapshot:
        if snapshot.session_id != self.session_id:
            raise ValueError("Gateway returned a snapshot for a different accounting session")
        self.snapshot = snapshot
        return snapshot

    async def refresh(self) -> AccountingSessionSnapshot:
        if self.client is None:
            raise ValueError("No gateway accounting session is configured")
        return self._accept(await self.client.read_session(self.session_id))

    async def begin_arbitration(self) -> AccountingSessionSnapshot:
        if self.client is None:
            raise ValueError("No gateway accounting session is configured")
        return self._accept(await self.client.begin_arbitration(self.session_id))

    async def seal_after_confirmed_stop(self) -> AccountingSessionSnapshot:
        """Read authoritative phase after response loss, then fence and seal it."""
        if self.client is None:
            raise ValueError("No gateway accounting session is configured")
        last_error: Exception | None = None
        for _ in range(5):
            try:
                observed = await self.refresh()
                if observed.state == AccountingSessionState.SEALED:
                    if self.active_since is not None:
                        await self.end_generation()
                    return observed
                if observed.state == AccountingSessionState.OPEN:
                    await self.begin_arbitration()
                else:
                    await self.resolve(ArbitrationDecision.SEAL)
            except Exception as error:
                last_error = error
        raise RuntimeError("Could not confirm gateway accounting session sealed") from last_error

    async def resolve(self, decision: ArbitrationDecision) -> AccountingSessionSnapshot:
        if self.client is None:
            raise ValueError("No gateway accounting session is configured")
        return self._accept(await self.client.resolve_arbitration(self.session_id, decision))

    def summary(
        self,
        snapshot: AccountingSessionSnapshot | None = None,
    ) -> ExternalServiceAccountingSummary:
        observed = snapshot or self.snapshot
        if observed is None:
            raise ValueError("No gateway accounting session is configured")
        applied = self.applied_credit_seconds(observed)
        return ExternalServiceAccountingSummary(
            accounting_session_id=observed.session_id,
            base_generation_allowance_seconds=self.base_allowance_seconds,
            cumulative_time_credit_cap_seconds=self.credit_cap_seconds,
            external_service_overhead_seconds=(observed.cumulative_neutral_overhead_ms / 1000),
            external_service_credit_applied_seconds=applied,
            effective_generation_allowance_seconds=(self.base_allowance_seconds + applied),
            external_service_credit_revision=observed.revision,
        )
