import asyncio
import io
import json
import os
import tarfile
from typing import Any, cast

import boto3
import pytest
from boto3.dynamodb.types import TypeDeserializer
from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.agent.contract import get_contract_from_zip_bytes
from tracker.agent.schemas import AgentConfig
from tracker.auth import RequestIdentity
from tracker.aws.s3 import download_from_s3, get_agent_result_s3_key, get_contract_s3_key
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    Org,
    Task,
    TaskStatus,
)
from tracker.types import AWSCredentials, HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark  # pyright: ignore[reportUnknownVariableType]
from tracker.utils import start_benchmark_request_to_benchmark

_AGENT_EMAIL = "gateway-integration@example.com"
_AGENT_NAME = "mini_sweagent"
_BENCHMARK_NAME = "swebench"
_MODEL = "openai/gpt-4o-mini"
_RUN_SHARDS = 16
_TASK_ID = "astropy__astropy-12907"


async def _load_contract(no_model_gateway: bool, harness_config: HarnessConfig) -> AgentContractRequest:
    bundle = await download_from_s3(
        s3_key=get_contract_s3_key(_AGENT_NAME),
        aws=harness_config.aws,
        s3_bucket=harness_config.s3_bucket,
    )
    return get_contract_from_zip_bytes(
        _AGENT_NAME,
        bundle,
        AgentConfig(model=_MODEL, kwargs={"no_model_gateway": no_model_gateway}),
    )


async def _read_api_calls(benchmark_id: str, harness_config: HarnessConfig) -> int:
    archive_bytes = await download_from_s3(
        s3_key=get_agent_result_s3_key(benchmark_id, _TASK_ID, "agent_output.tar.gz"),
        aws=harness_config.aws,
        s3_bucket=harness_config.s3_bucket,
    )
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
        matches = [
            member for member in archive.getmembers() if member.isfile() and member.name.endswith("/trajectory.json")
        ]
        assert len(matches) == 1
        trajectory_file = archive.extractfile(matches[0])
        assert trajectory_file is not None
        trajectory = cast(dict[str, Any], json.load(trajectory_file))

    api_calls = trajectory["info"]["model_stats"]["api_calls"]
    assert isinstance(api_calls, int)
    return api_calls


def _query_run_events(client: Any, table_name: str, run_id: str) -> list[dict[str, object]]:
    projected_items: list[dict[str, Any]] = []
    for shard in range(_RUN_SHARDS):
        start_key: dict[str, Any] | None = None
        while True:
            query: dict[str, Any] = {
                "TableName": table_name,
                "IndexName": "GSI1",
                "KeyConditionExpression": "GSI1PK = :pk",
                "ExpressionAttributeValues": {":pk": {"S": f"RUN#{run_id}#S#{shard:02d}"}},
            }
            if start_key:
                query["ExclusiveStartKey"] = start_key
            response = cast(dict[str, Any], client.query(**query))
            projected_items.extend(cast(list[dict[str, Any]], response.get("Items", [])))
            start_key = cast(dict[str, Any] | None, response.get("LastEvaluatedKey"))
            if not start_key:
                break

    deserialize = TypeDeserializer().deserialize
    events: list[dict[str, object]] = []
    for item in projected_items:
        response = cast(
            dict[str, Any],
            client.get_item(
                TableName=table_name,
                Key={"PK": item["PK"], "SK": item["SK"]},
                ConsistentRead=True,
            ),
        )
        event = cast(dict[str, Any] | None, response.get("Item"))
        if event:
            events.append({key: deserialize(value) for key, value in event.items()})
    return events


async def _wait_for_run_events(
    client: Any, table_name: str, run_id: str, timeout_seconds: float
) -> list[dict[str, object]]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        events = _query_run_events(client, table_name, run_id)
        if events or loop.time() >= deadline:
            return events
        await asyncio.sleep(min(2, deadline - loop.time()))


def _ledger_client(aws: AWSCredentials) -> Any:
    return boto3.client(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        "dynamodb",
        aws_access_key_id=aws.aws_access_key_id,
        aws_secret_access_key=aws.aws_secret_access_key,
        aws_session_token=aws.aws_session_token,
        region_name=aws.aws_default_region,
    )


def _create_benchmark(
    *,
    contract: AgentContractRequest,
    session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
) -> tuple[Benchmark, StartBenchmarkRequest]:
    request = StartBenchmarkRequest(
        benchmark_name=_BENCHMARK_NAME,
        contract=contract,
        concurrency=1,
        task_ids=[_TASK_ID],
        harness_config=harness_config,
        service_headers=service_headers,
    )
    benchmark = start_benchmark_request_to_benchmark(
        request,
        RequestIdentity(
            org=Org(id=TEST_ORG_ID, name="default"),
            access_key_id=None,
            email=_AGENT_EMAIL,
            name="Model Gateway integration",
        ),
    )
    session.add(benchmark)
    session.commit()
    return benchmark, request


@pytest.mark.parametrize(
    "no_model_gateway",
    [False, True],
    ids=["gateway-on", "gateway-off"],
)
async def test_model_gateway_route_for_one_benchmark_task(
    no_model_gateway: bool,
    database_session: Session,
    harness_config: HarnessConfig,
    service_headers: dict[str, str],
) -> None:
    ledger_table = os.environ["TEST_GATEWAY_USAGE_LEDGER_TABLE_NAME"]
    ledger_client = _ledger_client(harness_config.aws)
    contract = await _load_contract(no_model_gateway, harness_config)
    benchmark, request = _create_benchmark(
        contract=contract,
        session=database_session,
        harness_config=harness_config,
        service_headers=service_headers,
    )

    await process_benchmark(request.model_dump(), str(benchmark.id), [_TASK_ID])

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED, benchmark.error_message
    task = database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == _TASK_ID)
    ).one()
    assert task.status == TaskStatus.FINISHED

    api_calls = await _read_api_calls(str(benchmark.id), harness_config)
    assert api_calls > 0
    events = await _wait_for_run_events(
        ledger_client,
        ledger_table,
        str(benchmark.id),
        30 if no_model_gateway else 90,
    )

    if no_model_gateway:
        assert events == []
        return

    assert len(events) == api_calls
    for event in events:
        assert event["run_id"] == str(benchmark.id)
        assert event["question_id"] == _TASK_ID
        assert event["benchmark_name"] == _BENCHMARK_NAME
        assert event["agent_name"] == _AGENT_NAME
        assert event["identity_email"] == _AGENT_EMAIL
        assert event["identity"] == {
            "benchmark_name": _BENCHMARK_NAME,
            "agent_name": _AGENT_NAME,
            "email": _AGENT_EMAIL,
        }
