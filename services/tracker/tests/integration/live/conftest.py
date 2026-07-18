"""Shared live-integration fixtures."""

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.types import HarnessConfig
from tracker.utils import fetch_harness_config


@pytest.fixture
def live_api_client(
    tracker_database: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[TestClient, None, None]:
    """Route the app through local persistence and real harness credentials."""

    def get_test_session() -> Generator[Session, None, None]:
        yield tracker_database

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setitem(app.dependency_overrides, fetch_harness_config, lambda: harness_config)
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_org,
        lambda: Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME),
    )

    with TestClient(app) as client:
        yield client
