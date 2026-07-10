from sqlmodel import Session

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    ManagedRunRuntimeLocator,
)


def test_runtime_locator_is_nullable_and_round_trips(database_session: Session) -> None:
    legacy_row = Benchmark(
        org_id=TEST_ORG_ID,
        name="legacy",
        arguments=BenchmarkArguments(contract=AgentContractRequest(name="agent"), concurrency=1),
    )
    managed_row = Benchmark(
        org_id=TEST_ORG_ID,
        name="managed",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent"),
            concurrency=1,
            runtime=ManagedRunRuntimeLocator(
                aws_default_region="us-east-1",
                s3_bucket="bucket",
                s3_prefix="orgs/org-id",
                log_group="logs",
                log_retention_policy=30,
                sandbox_provider_secret_name="provider",
            ),
        ),
    )
    database_session.add(legacy_row)
    database_session.add(managed_row)
    database_session.commit()
    database_session.expire_all()

    stored_legacy = database_session.get(Benchmark, legacy_row.id)
    stored_managed = database_session.get(Benchmark, managed_row.id)

    assert stored_legacy is not None
    assert stored_legacy.arguments.runtime is None
    assert stored_managed is not None
    assert stored_managed.arguments.runtime == managed_row.arguments.runtime
    assert "runtime" not in stored_managed.arguments.model_dump()
    assert "runtime" not in BenchmarkArguments.model_json_schema()["properties"]
    assert "runtime" not in app.openapi()["components"]["schemas"]["BenchmarkArguments"]["properties"]
