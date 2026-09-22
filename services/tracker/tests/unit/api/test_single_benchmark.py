"""Run with `uv run pytest tests/unit/api/test_single_benchmark.py`.

Cover single-benchmark details and task listing behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session

from main import app
from tests.factories import make_error_result, make_task
from tests.utils import TEST_ORG_ID
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    Benchmark,
    BenchmarkArguments,
    FinalEvaluation,
    Org,
    TaskStatus,
)

_client = TestClient(app)


def test_single_benchmark_reports_terminal_progress_and_enforces_org_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Run detail must count terminal outcomes and hide another organization's run.

    Test cases:
    - Finished, error, and stopped tasks all count toward completed progress.
    - Final score and run links are returned with the persisted metadata.
    - A benchmark from another organization returns 404.
    """
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    database_session.add_all(
        [
            make_task(
                benchmark,
                "finished",
                status=TaskStatus.FINISHED,
                finished_at=benchmark.started_at,
            ),
            make_task(
                benchmark,
                "error",
                status=TaskStatus.ERROR,
                finished_at=benchmark.started_at,
            ),
            make_task(benchmark, "stopped", status=TaskStatus.STOPPED),
            make_task(benchmark, "pending"),
            FinalEvaluation(org_id=TEST_ORG_ID, benchmark=benchmark.id, final_score=0.75),
        ]
    )
    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(org_id=other_org.id, name=benchmark.name, arguments=benchmark.arguments)
    database_session.add_all([other_org, other_benchmark])
    database_session.commit()

    response = _client.get(
        f"/benchmarks/{benchmark.id}",
        headers={
            "x-harness-aws-access-key-id": "test-key",
            "x-harness-aws-secret-access-key": "test-secret",
            "x-harness-aws-default-region": "us-east-1",
            "x-harness-s3-bucket": "test-bucket",
            "x-harness-log-group": "test-log-group",
        },
    )
    other_org_response = _client.get(f"/benchmarks/{other_benchmark.id}")

    response_body = response.json()
    assert response.status_code == 200
    assert response_body["total_tasks"] == 4
    assert response_body["finished_tasks"] == 3
    assert response_body["task_state_counts"] == {
        "ERROR": 1,
        "FINISHED": 1,
        "PENDING": 1,
        "STOPPED": 1,
    }
    assert response_body["final_score"] == 0.75
    assert str(benchmark.id) in response_body["cloudwatch_url"]
    assert str(benchmark.id) in response_body["s3_bucket_url"]
    assert other_org_response.status_code == 404


def test_single_benchmark_revalidates_saved_owner_bucket(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
) -> None:
    class ChangedOwnerTagClient:
        async def __aenter__(self) -> "ChangedOwnerTagClient":
            return self

        async def __aexit__(self, *_exc: object) -> None:
            pass

        async def head_bucket(self, **_request: str) -> dict[str, str]:
            return {"BucketRegion": "us-east-1"}

        async def get_bucket_tagging(self, **_request: str) -> dict[str, list[dict[str, str]]]:
            return {
                "TagSet": [
                    {"Key": "valsmith:environment", "Value": "dev"},
                    {"Key": "valsmith:owner-account-id", "Value": "999"},
                    {"Key": "valsmith:backup", "Value": "true"},
                    {"Key": "valsmith:valkyrie-org-id", "Value": str(TEST_ORG_ID)},
                ]
            }

    owner_bucket = "vs-dev-acme-123"
    example_benchmark_object.aws_managed = True
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={
            "properties": AWSResources(
                region="us-east-1",
                s3_bucket=owner_bucket,
                log_group="logs",
                log_retention_days=30,
            )
        }
    )
    database_session.add(example_benchmark_object)
    database_session.commit()
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ROLE_ORG_IDS", str(TEST_ORG_ID))
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")
    monkeypatch.setattr(
        "tracker.config.AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS",
        json.dumps({str(TEST_ORG_ID): ["dev"]}),
    )
    changed_owner_client = ChangedOwnerTagClient()

    def s3_client(
        _provider: DefaultChainAWSClientProvider,
    ) -> ChangedOwnerTagClient:
        return changed_owner_client

    monkeypatch.setattr(
        DefaultChainAWSClientProvider,
        "s3_client",
        s3_client,
    )

    response = _client.get(f"/benchmarks/{example_benchmark_object.id}")

    assert response.status_code == 403


def test_single_benchmark_legacy_access_key_without_credentials_omits_storage_links(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    example_benchmark_object.aws_managed = False
    example_benchmark_object.arguments = BenchmarkArguments(
        contract=example_benchmark_object.arguments.contract,
        concurrency=1,
        properties=None,
    )
    database_session.add(example_benchmark_object)
    database_session.commit()

    response = _client.get(f"/benchmarks/{example_benchmark_object.id}")

    assert response.status_code == 200
    assert response.json()["s3_bucket_url"] is None
    assert response.json()["storage_bucket"] is None


def test_benchmark_tasks_filter_literal_search_and_latest_error(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Task listing must preserve attention order, literal search, and retry history.

    Test cases:
    - Status sorting places errors before finished tasks.
    - Percent and underscore search characters are treated literally.
    - The newest terminal error is returned when a newer scheduled-retry row exists.
    """
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    literal_task = make_task(
        benchmark,
        "literal_%_match",
        status=TaskStatus.ERROR,
        started_at=now,
        finished_at=now,
    )
    other_error = make_task(
        benchmark,
        "ordinary-error",
        status=TaskStatus.ERROR,
        started_at=now - timedelta(minutes=1),
        finished_at=now,
    )
    finished_task = make_task(
        benchmark,
        "finished",
        status=TaskStatus.FINISHED,
        started_at=now,
        finished_at=now,
    )
    database_session.add_all([literal_task, other_error, finished_task])
    database_session.flush()
    database_session.add_all(
        [
            make_error_result(literal_task, "old failure", now - timedelta(minutes=1)),
            make_error_result(literal_task, "latest failure", now),
            make_error_result(
                literal_task,
                "scheduled retry",
                now + timedelta(minutes=1),
                producer="sandbox_provider",
                operation="setup",
                error_type="SandboxSetupError",
                retry_scheduled=True,
                failed_attempt_number=1,
            ),
            make_error_result(other_error, "other failure", now),
        ]
    )
    database_session.commit()

    sorted_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks",
        params={"status": "ERROR,FINISHED", "sort": "status", "sort_dir": "desc"},
    )
    literal_search_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks",
        params={"task_id_search": "_%"},
    )

    sorted_body = sorted_response.json()
    assert sorted_response.status_code == 200
    assert sorted_body["total_count"] == 3
    assert [task["status"] for task in sorted_body["tasks"]] == ["ERROR", "ERROR", "FINISHED"]
    literal_row = next(task for task in sorted_body["tasks"] if task["task_id"] == literal_task.task_id)
    assert literal_row["error_message"] == "latest failure"
    assert literal_search_response.status_code == 200
    assert literal_search_response.json()["total_count"] == 1
    assert literal_search_response.json()["tasks"][0]["task_id"] == "literal_%_match"


def test_single_benchmark_returns_saved_bucket_after_default_changes(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
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
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ROLE_ORG_IDS", str(TEST_ORG_ID))
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_S3_BUCKET", "changed-default")
    monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")
    validation = AsyncMock()
    monkeypatch.setattr("tracker.api.single_benchmark.http_validate_saved_managed_storage_runtime", validation)

    response = _client.get(f"/benchmarks/{benchmark.id}")

    assert response.status_code == 200, response.text
    assert response.json()["storage_bucket"] == "vs-dev-acme-123"
    assert validation.call_args.args[0].resources.s3_bucket == "vs-dev-acme-123"
