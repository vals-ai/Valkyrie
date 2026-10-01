"""Fixtures for local tracker API integration tests."""

import importlib
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

import main as main_module
import tracker.auth as auth_module
import tracker.config as config_module
from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session


@pytest.fixture(autouse=True)
def setup_app_dependencies(
    tracker_database: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route the app through the local database and organization."""

    def get_test_session() -> Generator[Session, None, None]:
        yield tracker_database

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    test_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: test_org)


@pytest.fixture
def local_app(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
) -> Generator[FastAPI, None, None]:
    """Configure the local app and database shared by API clients."""
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")

    importlib.reload(config_module)
    importlib.reload(auth_module)
    importlib.reload(main_module)

    for key, value in {
        "AWS_MANAGED_SUBMISSIONS_ENABLED": True,
        "AWS_DEPLOYMENT_ROLE_ORG_IDS": str(TEST_ORG_ID),
        "AWS_DEPLOYMENT_ACCOUNT_ID": "123456789012",
        "AWS_DEPLOYMENT_REGION": "us-east-1",
        "AWS_DEPLOYMENT_S3_BUCKET": "test-bucket",
        "AWS_DEPLOYMENT_LOG_GROUP": "test-log-group",
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": "30",
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER": "daytona",
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME": "test-provider-secret",
    }.items():
        monkeypatch.setattr(config_module, key, value)

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    main_module.app.dependency_overrides[get_session] = get_test_session
    monkeypatch.setattr("tracker.database.session.engine", database_session.bind)

    try:
        yield main_module.app
    finally:
        main_module.app.dependency_overrides.clear()


@pytest.fixture
def access_key_auth(local_app: FastAPI) -> Generator[None, None, None]:
    with patch.object(auth_module, "_descope_client") as mock_client:
        mock_client.exchange_access_key.return_value = {
            "tenants": {"default": {}},
            "keyId": "K_caller",
            "sub": "K_caller",
            "userId": "U_caller",
            "user_id": "U_caller",
            "email": "caller@example.com",
        }
        yield


@pytest.fixture
def access_key_client(access_key_auth: None, local_app: FastAPI) -> Generator[TestClient, None, None]:
    with TestClient(local_app) as test_client:
        yield test_client


@pytest.fixture
def client(access_key_client: TestClient) -> Generator[TestClient, None, None]:
    """Use access-key authentication with the local tracker app."""
    yield access_key_client
