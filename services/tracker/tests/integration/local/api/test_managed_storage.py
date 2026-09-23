"""Persisted run locations through the HTTP API and real PostgreSQL."""

import json

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.integration.local.database.conftest import (
    postgres_container as postgres_container,
    postgres_url as postgres_url,
)
from tests.integration.local.database.conftest import (
    postgres_engine as postgres_engine,
)
from tests.integration.local.database.conftest import (
    postgres_session as postgres_session,
)
from tests.storage_lifecycle_support import MemoryS3
from tests.utils import TEST_ORG_ID
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.runtime import AWSResources


@pytest.fixture
def database_session(postgres_session: Session) -> Session:
    return postgres_session


def test_saved_location_survives_new_request_and_default_change(
    client: TestClient,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = make_benchmark(session=database_session)
    benchmark.aws_managed = True
    benchmark.arguments = benchmark.arguments.model_copy(
        update={
            "properties": AWSResources(
                region="us-east-1",
                s3_bucket="vs-dev-acme-123",
                log_group="logs",
                log_retention_days=30,
            )
        }
    )
    database_session.add(benchmark)
    database_session.commit()
    run_id = benchmark.id
    database_session.expunge_all()
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ROLE_ORG_IDS", str(TEST_ORG_ID))
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_S3_BUCKET", "changed-shared-bucket")
    monkeypatch.setattr("tracker.config.AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS", json.dumps({str(TEST_ORG_ID): ["dev"]}))
    storage = MemoryS3()

    def s3_client(_provider: DefaultChainAWSClientProvider) -> MemoryS3:
        return storage

    monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)
    for endpoint in [f"benchmarks/{run_id}", f"fetch-benchmark-metadata/{run_id}"]:
        response = client.get(f"/{endpoint}", headers={"Authorization": "Bearer fake"})
        assert response.status_code == 200, response.text
        assert response.json()["storage_bucket"] == "vs-dev-acme-123"

    assert storage.calls
    assert all(arguments["Bucket"] == "vs-dev-acme-123" for _, arguments in storage.calls)
