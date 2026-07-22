"""Live S3 coverage for tracker API routes.

Run: uv run pytest tests/integration/live/api/test_s3_routes.py
"""

from io import BytesIO
from uuid import uuid4
from zipfile import ZipFile

import httpx
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import delete_from_s3, upload_to_s3
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Task
from tracker.types import HarnessConfig


async def test_agent_catalog_and_download_url_round_trip_real_s3(
    live_api_client: TestClient,
    seeded_test_agent_artifact: str,
) -> None:
    """Agent listing and download signing must agree on the seeded S3 object.

    Test cases:
    - The live catalog contains the uniquely seeded agent with its last-modified value.
    - The route's five-minute presigned URL downloads that valid agent archive.
    - A name absent from the real bucket returns 404.
    """
    catalog_response = live_api_client.get("/agents")

    assert catalog_response.status_code == 200
    selected_agent = next(
        agent for agent in catalog_response.json()["agents"] if agent["name"] == seeded_test_agent_artifact
    )
    assert selected_agent["last_modified"] is not None

    download_response = live_api_client.get(f"/agents/{selected_agent['name']}/download-url")
    assert download_response.status_code == 200
    assert download_response.json()["expires_in"] == 300

    async with httpx.AsyncClient() as http_client:
        artifact_response = await http_client.get(download_response.json()["download_url"])

    assert artifact_response.status_code == 200
    with ZipFile(BytesIO(artifact_response.content)) as archive:
        assert archive.namelist()
        assert archive.testzip() is None

    missing_response = live_api_client.get(f"/agents/missing-{uuid4()}/download-url")
    assert missing_response.status_code == 404


async def test_task_artifact_route_round_trips_real_s3_and_handles_missing_output(
    live_api_client: TestClient,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    """Task artifact signing must expose existing output and suppress missing output.

    Test cases:
    - A real output object is discovered, signed for five minutes, and downloaded byte-for-byte.
    - Deleting the object makes the same route return no output URL.
    """
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="live-s3-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
    )
    task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="live-s3-output")
    database_session.add_all([benchmark, task])
    database_session.commit()

    object_key = f"benchmarks/{benchmark.id}/{task.task_id}/agent_output.tar.gz"
    expected_content = b"live tracker output artifact"
    aws_runtime = AWSRuntime.from_harness_config(harness_config)
    await upload_to_s3(
        file_content=expected_content,
        s3_key=object_key,
        runtime=aws_runtime,
    )

    try:
        artifact_response = live_api_client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")
        assert artifact_response.status_code == 200
        assert artifact_response.json()["agent_output_expires_in"] == 300
        assert artifact_response.json()["cloudwatch_url"] is not None

        async with httpx.AsyncClient() as http_client:
            download_response = await http_client.get(artifact_response.json()["agent_output_url"])

        assert download_response.status_code == 200
        assert download_response.content == expected_content
    finally:
        await delete_from_s3(
            s3_key=object_key,
            runtime=aws_runtime,
        )

    missing_response = live_api_client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")
    assert missing_response.status_code == 200
    assert missing_response.json()["agent_output_url"] is None
    assert missing_response.json()["agent_output_expires_in"] is None
