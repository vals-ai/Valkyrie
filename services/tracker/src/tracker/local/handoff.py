"""Keep local execution credentials in memory until the claimed child receives them."""

import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from uuid import UUID

from tracker.exceptions import SecretsError
from tracker.local.secrets import InMemorySecretStore


@dataclass
class _PendingSecrets:
    values: dict[str, str] = field(repr=False)
    expires_at: float
    claim_token: str | None = field(default=None, repr=False)


class PendingExecutionSecrets:
    """A bounded, process-local handoff; restarting Tracker requires fresh input."""

    def __init__(self, *, ttl_seconds: float = 900, maximum_pending: int = 100) -> None:
        self._ttl_seconds = ttl_seconds
        self._maximum_pending = maximum_pending
        self._pending: dict[UUID, _PendingSecrets] = {}
        self._lock = threading.Lock()

    def put(self, dispatch_id: UUID, references: Mapping[str, str], values: Mapping[str, str]) -> None:
        """Validate the declared keys before retaining a private copy."""
        store = InMemorySecretStore(references, values)
        store.close()
        if sum(len(key.encode()) + len(value.encode()) for key, value in values.items()) > 1024 * 1024:
            raise SecretsError("Local execution secrets exceed the one-megabyte limit")
        with self._lock:
            self._expire()
            if dispatch_id in self._pending:
                raise SecretsError("Local execution secrets are already registered for this dispatch")
            if len(self._pending) >= self._maximum_pending:
                raise SecretsError("Too many pending local executions")
            self._pending[dispatch_id] = _PendingSecrets(dict(values), time.monotonic() + self._ttl_seconds)

    def receive(self, dispatch_id: UUID, claim_token: str) -> dict[str, str]:
        """Return credentials only after the caller verifies the durable claim."""
        with self._lock:
            self._expire()
            pending = self._pending.get(dispatch_id)
            if pending is None:
                raise SecretsError("Local execution secrets are unavailable; resume with fresh execution secrets")
            if pending.claim_token is not None and pending.claim_token != claim_token:
                raise SecretsError("Local execution secrets belong to another dispatch claimant")
            pending.claim_token = claim_token
            return dict(pending.values)

    def acknowledge(self, dispatch_id: UUID, claim_token: str) -> None:
        """Forget values after the same claimant reports child receipt."""
        with self._lock:
            pending = self._pending.get(dispatch_id)
            if pending is None:
                return
            if pending.claim_token != claim_token:
                raise SecretsError("Local execution secrets have not been received by this claimant")
            self._discard(dispatch_id)

    def discard(self, dispatch_id: UUID) -> None:
        with self._lock:
            self._discard(dispatch_id)

    def pending_ids(self) -> tuple[UUID, ...]:
        with self._lock:
            self._expire()
            return tuple(self._pending)

    def close(self) -> None:
        with self._lock:
            for dispatch_id in tuple(self._pending):
                self._discard(dispatch_id)

    def _discard(self, dispatch_id: UUID) -> None:
        pending = self._pending.pop(dispatch_id, None)
        if pending is not None:
            pending.values.clear()

    def _expire(self) -> None:
        now = time.monotonic()
        for dispatch_id, pending in tuple(self._pending.items()):
            if pending.expires_at <= now:
                self._discard(dispatch_id)


pending_execution_secrets = PendingExecutionSecrets()
