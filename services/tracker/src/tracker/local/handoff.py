"""Keep local execution credentials in memory until the claimed child receives them."""

import threading
from collections.abc import Mapping
from uuid import UUID

from tracker.exceptions import SecretsError
from tracker.local.secrets import InMemorySecretStore


class PendingExecutionSecrets:
    """Bound credentials in memory; the reaper applies durable dispatch deadlines."""

    def __init__(self, *, maximum_pending: int = 100) -> None:
        self._maximum_pending = maximum_pending
        self._pending: dict[UUID, dict[str, str]] = {}
        self._lock = threading.Lock()

    def put(self, dispatch_id: UUID, references: Mapping[str, str], values: Mapping[str, str]) -> None:
        """Validate the declared keys before retaining a private copy."""
        store = InMemorySecretStore(references, values)
        store.close()
        if sum(len(key.encode()) + len(value.encode()) for key, value in values.items()) > 1024 * 1024:
            raise SecretsError("Local execution secrets exceed the one-megabyte limit")
        with self._lock:
            if dispatch_id in self._pending:
                raise SecretsError("Local execution secrets are already registered for this dispatch")
            if len(self._pending) >= self._maximum_pending:
                raise SecretsError("Too many pending local executions")
            self._pending[dispatch_id] = dict(values)

    def receive(self, dispatch_id: UUID) -> dict[str, str]:
        """Return credentials only after the caller verifies the durable claim."""
        with self._lock:
            pending = self._pending.get(dispatch_id)
            if pending is None:
                raise SecretsError("Local execution secrets are unavailable; resume with fresh execution secrets")
            return dict(pending)

    def discard(self, dispatch_id: UUID) -> None:
        with self._lock:
            self._discard(dispatch_id)

    def pending_ids(self) -> tuple[UUID, ...]:
        with self._lock:
            return tuple(self._pending)

    def close(self) -> None:
        with self._lock:
            for dispatch_id in tuple(self._pending):
                self._discard(dispatch_id)

    def _discard(self, dispatch_id: UUID) -> None:
        pending = self._pending.pop(dispatch_id, None)
        if pending is not None:
            pending.clear()


pending_execution_secrets = PendingExecutionSecrets()
