"""Passive External Service Gateway counter client and Tracker deadline accounting."""

from dataclasses import dataclass
import time

import httpx
from pydantic import BaseModel


class AccountingSnapshot(BaseModel):
    accounting_id: str
    cumulative_neutral_overhead_ms: int
    revision: int


class ExternalServiceGatewayClient:
    """Authenticated Tracker-only reader for gateway counters."""

    def __init__(
        self,
        base_url: str,
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

    async def read_counter(self, accounting_id: str) -> AccountingSnapshot:
        url = f"{self.base_url}/counters/{accounting_id}"
        headers = {"X-SSP-Control-Token": self.control_token}
        if self._client is not None:
            response = await self._client.get(url, headers=headers)
        else:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(url, headers=headers)
        response.raise_for_status()
        return AccountingSnapshot.model_validate(response.json())


@dataclass(frozen=True)
class GenerationAccountingSummary:
    accounting_id: str | None
    base_generation_allowance_seconds: float | None
    cumulative_time_credit_cap_seconds: float | None
    external_service_overhead_seconds: float | None
    external_service_credit_applied_seconds: float | None
    external_service_credit_revision: int | None
    generation_elapsed_seconds: float


@dataclass
class ExternalServiceDeadlineController:
    """Tracker-owned cumulative active-time policy over optional gateway credit."""

    base_allowance_seconds: float | None
    credit_cap_seconds: float = 0.0
    client: ExternalServiceGatewayClient | None = None
    snapshot: AccountingSnapshot | None = None
    active_elapsed_seconds: float = 0.0
    active_since: float | None = None
    credit_frozen: bool = False

    @property
    def accounting_id(self) -> str:
        if self.snapshot is None:
            raise ValueError("No gateway accounting counter is configured")
        return self.snapshot.accounting_id

    def applied_credit_seconds(self) -> float:
        return min(
            self.snapshot.cumulative_neutral_overhead_ms / 1000 if self.snapshot is not None else 0.0,
            self.credit_cap_seconds,
        )

    def freeze_credit(self) -> None:
        """Latch the accepted credit at the definitive generation cutoff."""
        self.credit_frozen = True

    def effective_allowance_seconds(self) -> float | None:
        if self.base_allowance_seconds is None:
            return None
        return self.base_allowance_seconds + self.applied_credit_seconds()

    def elapsed_seconds(self, now: float) -> float:
        return self.active_elapsed_seconds + (now - self.active_since if self.active_since is not None else 0.0)

    def deadline(self, now: float) -> float | None:
        allowance = self.effective_allowance_seconds()
        return None if allowance is None else now + allowance - self.elapsed_seconds(now)

    def begin_generation(self, now: float | None = None) -> None:
        if self.active_since is not None:
            raise ValueError("Generation interval already active")
        self.active_since = time.monotonic() if now is None else now

    def end_generation(self, now: float) -> None:
        if self.active_since is None:
            raise ValueError("No generation interval is active")
        self.active_elapsed_seconds += now - self.active_since
        self.active_since = None

    def _accept(self, snapshot: AccountingSnapshot) -> AccountingSnapshot:
        if snapshot.accounting_id != self.accounting_id:
            raise ValueError("Gateway returned a counter for a different accounting ID")
        self.snapshot = snapshot
        return snapshot

    async def refresh(self) -> AccountingSnapshot:
        if self.credit_frozen:
            raise ValueError("Generation credit snapshot is frozen")
        if self.client is None:
            raise ValueError("No gateway accounting counter is configured")
        return self._accept(await self.client.read_counter(self.accounting_id))

    async def finalize_after_confirmed_stop(self, absence_confirmed_at: float) -> None:
        """Observe terminal counter, without changing credit latched at exhaustion."""
        if self.active_since is not None:
            self.end_generation(absence_confirmed_at)
        if self.client is not None and not self.credit_frozen:
            await self.refresh()
            self.freeze_credit()

    def summary(self) -> GenerationAccountingSummary:
        observed = self.snapshot
        applied = self.applied_credit_seconds()
        return GenerationAccountingSummary(
            accounting_id=observed.accounting_id if observed is not None else None,
            base_generation_allowance_seconds=self.base_allowance_seconds,
            cumulative_time_credit_cap_seconds=self.credit_cap_seconds if observed is not None else None,
            external_service_overhead_seconds=(observed.cumulative_neutral_overhead_ms / 1000)
            if observed is not None
            else None,
            external_service_credit_applied_seconds=applied if observed is not None else None,
            external_service_credit_revision=observed.revision if observed is not None else None,
            generation_elapsed_seconds=self.active_elapsed_seconds,
        )
