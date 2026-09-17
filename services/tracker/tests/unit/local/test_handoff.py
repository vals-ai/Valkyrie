"""Credentials remain transient and bound to the current dispatch claimant."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import ExecutorDispatch
from tracker.database.session import get_session
from tracker.exceptions import SecretsError
from tracker.local.api import router
from tracker.local.handoff import PendingExecutionSecrets, pending_execution_secrets


def test_pending_secrets_require_same_claimant_and_receipt() -> None:
    """Keep values through retrieval, then discard only after claimant acknowledgement."""
    registry = PendingExecutionSecrets()
    dispatch_id = uuid4()
    values = {"KEY": "value"}
    registry.put(dispatch_id, {"KEY": "reference"}, values)
    values.clear()
    received = registry.receive(dispatch_id, "claim-one")
    assert received == {"KEY": "value"}
    received.clear()
    assert registry.receive(dispatch_id, "claim-one") == {"KEY": "value"}
    with pytest.raises(SecretsError, match="another"):
        registry.receive(dispatch_id, "claim-two")
    with pytest.raises(SecretsError, match="not been received"):
        registry.acknowledge(dispatch_id, "claim-two")
    registry.acknowledge(dispatch_id, "claim-one")
    registry.acknowledge(dispatch_id, "claim-one")
    with pytest.raises(SecretsError, match="fresh execution secrets"):
        registry.receive(dispatch_id, "claim-one")


def test_pending_secrets_expire_and_capacity_is_bounded() -> None:
    """Bound retained credentials and require a fresh handoff after expiry or restart."""
    dispatch_id = uuid4()
    expired = PendingExecutionSecrets(ttl_seconds=0)
    expired.put(dispatch_id, {}, {})
    with pytest.raises(SecretsError, match="unavailable"):
        expired.receive(dispatch_id, "claim")
    registry = PendingExecutionSecrets(maximum_pending=1)
    registry.put(dispatch_id, {}, {})
    with pytest.raises(SecretsError, match="already registered"):
        registry.put(dispatch_id, {}, {})
    with pytest.raises(SecretsError, match="Too many"):
        registry.put(uuid4(), {}, {})
    registry.discard(dispatch_id)
    registry.put(uuid4(), {}, {})
    registry.close()
    assert registry.pending_ids() == ()


def test_handoff_api_verifies_live_claim_before_revealing_values(
    database_session: Session,
    executor_authority_kwargs: Callable[..., dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject nonlocal access, wrong claims, and expired leases without revealing values."""
    benchmark = make_benchmark(session=database_session)
    kwargs = executor_authority_kwargs(benchmark)
    dispatch_id = UUID(str(kwargs["executor_dispatch_id"]))
    dispatch = database_session.get(ExecutorDispatch, dispatch_id)
    assert dispatch is not None
    dispatch.claim_token = "current-claim"
    dispatch.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    database_session.add(dispatch)
    database_session.commit()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_session] = lambda: database_session
    pending_execution_secrets.put(dispatch_id, {"KEY": "reference"}, {"KEY": "sensitive"})
    url = f"/internal/local-execution-secrets/{dispatch_id}"
    headers = {"x-executor-claim": "current-claim"}
    try:
        with TestClient(app) as client:
            monkeypatch.delenv("VALKYRIE_RUNTIME", raising=False)
            assert client.post(f"{url}/receive", headers=headers).status_code == 404
            monkeypatch.setenv("VALKYRIE_RUNTIME", "local")
            rejected = client.post(f"{url}/receive", headers={"x-executor-claim": "wrong-claim"})
            assert rejected.status_code == 403
            assert "sensitive" not in rejected.text
            response = client.post(f"{url}/receive", headers=headers)
            assert response.status_code == 200
            assert response.json() == {"KEY": "sensitive"}
            assert response.headers["cache-control"] == "no-store"
            assert client.post(f"{url}/acknowledge", headers=headers).status_code == 204
            assert client.post(f"{url}/receive", headers=headers).status_code == 409
            pending_execution_secrets.put(dispatch_id, {}, {})
            dispatch.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            database_session.add(dispatch)
            database_session.commit()
            assert client.post(f"{url}/receive", headers=headers).status_code == 403
    finally:
        pending_execution_secrets.close()


def test_handoff_reconciliation_preserves_active_dispatches(
    database_session: Session,
    executor_authority_kwargs: Callable[..., dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Discard expired dispatch credentials while retaining an active claimant's input."""
    from tracker.local.handoff_lifecycle import reap_execution_secrets

    benchmark = make_benchmark(session=database_session)
    kwargs = executor_authority_kwargs(benchmark)
    dispatch_id = UUID(str(kwargs["executor_dispatch_id"]))
    dispatch = database_session.get(ExecutorDispatch, dispatch_id)
    assert dispatch is not None
    dispatch.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    database_session.add(dispatch)
    database_session.commit()
    monkeypatch.setattr("tracker.local.handoff_lifecycle.engine", database_session.bind)
    pending_execution_secrets.put(dispatch_id, {}, {})
    try:
        reap_execution_secrets()
        assert pending_execution_secrets.pending_ids() == (dispatch_id,)
        dispatch.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        database_session.add(dispatch)
        database_session.commit()
        reap_execution_secrets()
        assert pending_execution_secrets.pending_ids() == ()
    finally:
        pending_execution_secrets.close()
