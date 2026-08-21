"""Run-scoped storage endpoints: output download URLs and agent-version promotion."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Body, HTTPException

from tracker.api.dependencies import RunAWSDependency, validated_agent_name
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    copy_s3_object,
    create_presigned_urls,
    get_benchmark_contract_s3_key,
    get_contract_s3_key,
    list_s3_objects,
    s3_object_exists,
)
from tracker.types import BenchmarkOutputURLsResponse, OutputURLEntry

OUTPUT_URL_EXPIRES_SECONDS = 3600

router = APIRouter(prefix="/benchmarks")


@router.get("/{benchmark_id}/output-urls", response_model=BenchmarkOutputURLsResponse)
async def get_benchmark_output_urls(
    run_context: RunAWSDependency,
    subpath: str = "",
) -> BenchmarkOutputURLsResponse:
    """Return presigned GET URLs for every object under the run's output prefix."""
    prefix = f"{S3_BENCHMARKS_PREFIX}/{run_context.benchmark.id}"
    if subpath:
        prefix = f"{prefix}/{subpath.strip('/')}"

    # A suffix-bearing subpath targets one exact object; anything else is a directory,
    # slash-terminated so sibling prefixes (task-1 vs task-10) never over-match.
    if not Path(prefix).suffix:
        prefix = f"{prefix}/"

    aws_runtime = run_context.aws_runtime
    keys = [key async for key in list_s3_objects(prefix, aws_runtime)]
    if not keys:
        raise HTTPException(status_code=404, detail=f"No files found under '{prefix}'")

    expires_in = aws_runtime.clients.maximum_presign_ttl(OUTPUT_URL_EXPIRES_SECONDS)
    urls = await create_presigned_urls(keys, aws_runtime, expiration=expires_in)
    files = [OutputURLEntry(key=key, download_url=url) for key, url in zip(keys, urls)]

    return BenchmarkOutputURLsResponse(prefix=prefix, files=files, expires_in=expires_in)


@router.post("/{benchmark_id}/agent-version", status_code=204)
async def update_benchmark_agent_version(
    run_context: RunAWSDependency,
    agent_name: str = Body(embed=True),
) -> None:
    """Overwrite the run's frozen agent copy from the latest agents/<name>.zip."""
    agent_name = validated_agent_name(agent_name)
    source_key = get_contract_s3_key(agent_name)
    if not await s3_object_exists(source_key, run_context.aws_runtime):
        raise HTTPException(status_code=404, detail=f"Agent '{agent_name}' not found in S3")

    destination_key = get_benchmark_contract_s3_key(str(run_context.benchmark.id), agent_name)
    await copy_s3_object(source_key, destination_key, run_context.aws_runtime)
