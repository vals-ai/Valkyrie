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
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import fetch_harness_config


@pytest.fixture
def harness_config(aws_credentials: AWSCredentials) -> HarnessConfig:
    """Provide deterministic harness configuration for local integration tests."""
    return HarnessConfig(
        aws=aws_credentials,
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=30,
        sandbox_provider_secret_name="test-daytona-secret",
    )


@pytest.fixture(autouse=True)
def setup_app_dependencies(
    tracker_database: Session,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    """Route the app through the local database and organization."""

    def get_test_session() -> Generator[Session, None, None]:
        yield tracker_database

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setitem(app.dependency_overrides, fetch_harness_config, lambda: harness_config)
    test_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: test_org)


@pytest.fixture
def local_app(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    harness_config: HarnessConfig,
) -> Generator[FastAPI, None, None]:
    """Configure the local app and database shared by API clients."""
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")

    importlib.reload(config_module)
    importlib.reload(auth_module)
    importlib.reload(main_module)

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    main_module.app.dependency_overrides[get_session] = get_test_session
    main_module.app.dependency_overrides[fetch_harness_config] = lambda: harness_config
    monkeypatch.setattr("tracker.database.session.engine", database_session.bind)

    try:
        yield main_module.app
    finally:
        main_module.app.dependency_overrides.clear()


@pytest.fixture
def client(local_app: FastAPI) -> Generator[TestClient, None, None]:
    """Use bearer authentication with the local tracker app."""
    with patch.object(auth_module, "_descope_client") as mock_client:
        mock_client.validate_session.return_value = {
            "tenants": {"default": {}},
            "userId": "U_caller",
            "user": {"email": "caller@example.com"},
        }
        with TestClient(local_app) as test_client:
            yield test_client


@pytest.fixture
def access_key_client(local_app: FastAPI) -> Generator[TestClient, None, None]:
    """Use access-key authentication with the local tracker app."""
    with patch.object(auth_module, "_descope_client") as mock_client:
        mock_client.exchange_access_key.return_value = {
            "tenants": {"default": {}},
            "keyId": "K_caller",
            "sub": "K_caller",
            "userId": "U_caller",
            "user_id": "U_caller",
            "email": "caller@example.com",
        }
        with TestClient(local_app) as test_client:
            yield test_client
