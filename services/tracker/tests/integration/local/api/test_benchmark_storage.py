"""Run with `uv run pytest tests/integration/local/api/test_benchmark_storage.py`.

Exercise run-scoped storage routes through the real app, database, and auth stack.
"""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import Org

_HARNESS_HEADERS = {
    "X-Harness-AWS-Access-Key-Id": "test-access-key",
    "X-Harness-AWS-Secret-Access-Key": "test-secret-key",
    "X-Harness-AWS-Default-Region": "us-east-1",
    "X-Harness-S3-Bucket": "test-bucket",
}


def test_output_urls_and_agent_version_accept_api_key_auth(
    access_key_client: TestClient,
    database_session: Session,
    monkeypatch: MonkeyPatch,
) -> None:
    """Hosted API-key auth with an access-key runtime must reach the run-scoped routes.

    Test cases:
    - output-keys lists the caller's own run and output-urls signs that batch.
    - agent-version promotes an existing agent for the same caller.
    """
    benchmark = make_benchmark(session=database_session)
    database_session.add(benchmark)
    database_session.commit()
    key = f"benchmarks/{benchmark.id}/task-a/output.json"

    async def list_objects(_prefix: str, _runtime: object) -> AsyncIterator[str]:
        yield key

    monkeypatch.setattr("tracker.api.benchmark_storage.list_s3_objects", list_objects)
    monkeypatch.setattr(
        "tracker.api.benchmark_storage.create_presigned_urls",
        AsyncMock(return_value=["https://example.test/file"]),
    )
    monkeypatch.setattr("tracker.api.benchmark_storage.s3_object_exists", AsyncMock(return_value=True))
    monkeypatch.setattr("tracker.api.benchmark_storage.copy_s3_object", AsyncMock(return_value=None))
    headers = {"x-api-key": "fake-key", **_HARNESS_HEADERS}

    keys_response = access_key_client.get(f"/benchmarks/{benchmark.id}/output-keys", headers=headers)
    assert keys_response.status_code == 200, keys_response.text
    assert keys_response.json()["keys"] == [key]

    urls_response = access_key_client.post(
        f"/benchmarks/{benchmark.id}/output-urls",
        json={"keys": [key]},
        headers=headers,
    )
    assert urls_response.status_code == 200, urls_response.text
    assert urls_response.json()["files"] == [{"key": key, "download_url": "https://example.test/file"}]

    promote_response = access_key_client.post(
        f"/benchmarks/{benchmark.id}/agent-version",
        json={"agent_name": "demo"},
        headers=headers,
    )
    assert promote_response.status_code == 204, promote_response.text


def test_output_keys_unauth_401(client: TestClient, database_session: Session) -> None:
    benchmark = make_benchmark(session=database_session)
    database_session.add(benchmark)
    database_session.commit()

    response = client.get(f"/benchmarks/{benchmark.id}/output-keys")

    assert response.status_code == 401


def test_run_storage_hides_foreign_runs_before_signing(
    access_key_client: TestClient,
    database_session: Session,
    monkeypatch: MonkeyPatch,
) -> None:
    foreign_org_id = uuid4()
    database_session.add(Org(id=foreign_org_id, name="foreign-org"))
    foreign_benchmark = make_benchmark(org_id=foreign_org_id)
    database_session.add(foreign_benchmark)
    database_session.commit()
    presign = AsyncMock(return_value=["https://example.test/file"])
    monkeypatch.setattr("tracker.api.benchmark_storage.create_presigned_urls", presign)

    response = access_key_client.post(
        f"/benchmarks/{foreign_benchmark.id}/output-urls",
        json={"keys": [f"benchmarks/{foreign_benchmark.id}/output.json"]},
        headers={"x-api-key": "fake-key", **_HARNESS_HEADERS},
    )

    assert response.status_code == 404
    presign.assert_not_awaited()
