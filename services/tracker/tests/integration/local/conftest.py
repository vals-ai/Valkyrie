"""Fixtures for local tracker API integration tests."""

import importlib
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import fetch_harness_config

_FAKE_HARNESS_CONFIG = HarnessConfig(
    aws=AWSCredentials(
        aws_access_key_id="test-aws-access-key-id",
        aws_secret_access_key="test-aws-secret-access-key",
        aws_default_region="us-east-1",
    ),
    s3_bucket="test-bucket",
    log_group="test-log-group",
    log_retention_policy=30,
    sandbox_provider_secret_name="test-daytona-secret",
)


@pytest.fixture(autouse=True)
def setup_app_dependencies(
    database_session: Session,
    tracker_database: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route the app through the local database and organization."""

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setitem(app.dependency_overrides, fetch_harness_config, lambda: _FAKE_HARNESS_CONFIG)
    test_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: test_org)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> Generator[TestClient, None, None]:
    """Use authenticated FastAPI routes against the local database."""
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")

    import tracker.config as config_module

    importlib.reload(config_module)
    import tracker.auth as auth_module

    importlib.reload(auth_module)
    import main as main_module

    importlib.reload(main_module)

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    main_module.app.dependency_overrides[get_session] = get_test_session
    main_module.app.dependency_overrides[fetch_harness_config] = lambda: _FAKE_HARNESS_CONFIG
    monkeypatch.setattr("tracker.database.session.engine", database_session.bind)

    with patch.object(auth_module, "_descope_client") as mock_client:
        mock_client.validate_session.return_value = {
            "tenants": {"default": {}},
            "userId": "U_caller",
            "user": {"email": "caller@example.com"},
        }
        try:
            with TestClient(main_module.app) as test_client:
                yield test_client
        finally:
            main_module.app.dependency_overrides.clear()
