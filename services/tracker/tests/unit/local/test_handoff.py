"""Credentials remain transient and bound to the current dispatch claimant."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import BenchmarkStatus, ExecutorDispatch, ExecutorDispatchStatus
from tracker.database.session import get_session
from tracker.exceptions import SecretsError
from tracker.local.api import router
from tracker.local.handoff import PendingExecutionSecrets, pending_execution_secrets
from tracker.local.secret_pipe import local_handoff_token


def test_pending_secrets_remain_until_child_receipt() -> None:
    """Keep values through retrieval, then discard only after child acknowledgement."""
    registry = PendingExecutionSecrets()
    dispatch_id = uuid4()
    values = {"KEY": "value"}
    registry.put(dispatch_id, {"KEY": "reference"}, values)
    values.clear()
    received = registry.receive(dispatch_id)
    assert received == {"KEY": "value"}
    received.clear()
    assert registry.receive(dispatch_id) == {"KEY": "value"}
    registry.discard(dispatch_id)
    registry.discard(dispatch_id)
    with pytest.raises(SecretsError, match="fresh execution secrets"):
        registry.receive(dispatch_id)


def test_pending_secrets_capacity_is_bounded() -> None:
    """Bound retained credentials and release capacity on discard or shutdown."""
    dispatch_id = uuid4()
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
    tmp_path: Path,
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
    dispatch.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    database_session.add(dispatch)
    database_session.commit()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_session] = lambda: database_session
    pending_execution_secrets.put(dispatch_id, {"KEY": "reference"}, {"KEY": "sensitive"})
    url = f"/internal/local-execution-secrets/{dispatch_id}"
    monkeypatch.setenv("VALKYRIE_LOCAL_DATA_ROOT", str(tmp_path))
    headers = {"x-local-handoff-token": local_handoff_token()}
    try:
        with TestClient(app) as client:
            monkeypatch.delenv("VALKYRIE_RUNTIME", raising=False)
            assert client.post(f"{url}/receive", headers=headers).status_code == 404
            monkeypatch.setenv("VALKYRIE_RUNTIME", "local")
            rejected = client.post(f"{url}/receive", headers={"x-local-handoff-token": "wrong-token"})
            assert rejected.status_code == 403
            assert "sensitive" not in rejected.text
            response = client.post(f"{url}/receive", headers=headers)
            assert response.status_code == 200
            assert response.json() == {"KEY": "sensitive"}
            assert response.headers["cache-control"] == "no-store"
            assert client.post(f"{url}/acknowledge", headers={"x-local-handoff-token": "wrong"}).status_code == 403
            assert pending_execution_secrets.pending_ids() == (dispatch_id,)
            benchmark.status = BenchmarkStatus.FINISHED
            database_session.add(benchmark)
            database_session.commit()
            assert client.post(f"{url}/acknowledge", headers=headers).status_code == 204
            assert pending_execution_secrets.pending_ids() == ()
            assert client.post(f"{url}/acknowledge", headers=headers).status_code == 204
            assert client.post(f"{url}/receive", headers=headers).status_code == 409
            benchmark.status = BenchmarkStatus.IN_PROGRESS
            database_session.add(benchmark)
            pending_execution_secrets.put(dispatch_id, {}, {})
            dispatch.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            database_session.add(dispatch)
            database_session.commit()
            assert client.post(f"{url}/receive", headers=headers).status_code == 403
    finally:
        pending_execution_secrets.close()


@pytest.mark.parametrize("status", [ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING])
def test_handoff_reconciliation_preserves_active_dispatches(
    status: ExecutorDispatchStatus,
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
    dispatch.status = status
    dispatch.claim_deadline_at = datetime.now(UTC) + timedelta(minutes=5)
    dispatch.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    database_session.add(dispatch)
    database_session.commit()
    monkeypatch.setattr("tracker.local.handoff_lifecycle.engine", database_session.bind)
    pending_execution_secrets.put(dispatch_id, {}, {})
    try:
        reap_execution_secrets()
        assert pending_execution_secrets.pending_ids() == (dispatch_id,)
        dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
        dispatch.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        database_session.add(dispatch)
        database_session.commit()
        reap_execution_secrets()
        assert pending_execution_secrets.pending_ids() == ()
    finally:
        pending_execution_secrets.close()
