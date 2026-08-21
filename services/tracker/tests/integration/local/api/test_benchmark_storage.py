"""Run with `uv run pytest tests/integration/local/api/test_benchmark_storage.py`.

Exercise run-scoped storage routes through the real app, database, and auth stack.
"""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session

from tests.factories import make_benchmark

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
    """The keyless CLI's api-key auth mode must reach the run-scoped storage routes.

    Test cases:
    - output-urls returns the signed listing for the caller's own run.
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

    output_response = access_key_client.get(f"/benchmarks/{benchmark.id}/output-urls", headers=headers)
    assert output_response.status_code == 200, output_response.text
    assert output_response.json()["files"] == [{"key": key, "download_url": "https://example.test/file"}]

    promote_response = access_key_client.post(
        f"/benchmarks/{benchmark.id}/agent-version",
        json={"agent_name": "demo"},
        headers=headers,
    )
    assert promote_response.status_code == 204, promote_response.text


def test_output_urls_unauth_401(client: TestClient, database_session: Session) -> None:
    benchmark = make_benchmark(session=database_session)
    database_session.add(benchmark)
    database_session.commit()

    response = client.get(f"/benchmarks/{benchmark.id}/output-urls")

    assert response.status_code == 401
