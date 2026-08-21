"""Run-scoped storage endpoints: output download URLs and agent-version promotion."""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from tracker.api.dependencies import RunAWSDependency, validated_agent_name
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    copy_s3_object,
    create_presigned_urls,
    get_benchmark_contract_s3_key,
    get_contract_s3_key,
    list_s3_objects,
    normalize_s3_download_prefix,
    s3_object_exists,
)
from tracker.storage_types import (
    BenchmarkOutputKeysResponse,
    BenchmarkOutputURLsResponse,
    OutputURLEntry,
    OutputURLsRequest,
)

OUTPUT_URL_EXPIRES_SECONDS = 3600

router = APIRouter(prefix="/benchmarks")


@router.get("/{benchmark_id}/output-keys", response_model=BenchmarkOutputKeysResponse)
async def get_benchmark_output_keys(
    run_context: RunAWSDependency,
    subpath: str = "",
) -> BenchmarkOutputKeysResponse:
    """Return the storage keys under one run-owned output prefix."""
    prefix = f"{S3_BENCHMARKS_PREFIX}/{run_context.benchmark.id}"
    if subpath:
        prefix = f"{prefix}/{subpath.strip('/')}"

    prefix = normalize_s3_download_prefix(prefix)

    aws_runtime = run_context.aws_runtime
    keys = [key async for key in list_s3_objects(prefix, aws_runtime)]
    if not keys:
        raise HTTPException(status_code=404, detail=f"No files found under '{prefix}'")

    return BenchmarkOutputKeysResponse(prefix=prefix, keys=keys)


@router.post("/{benchmark_id}/output-urls", response_model=BenchmarkOutputURLsResponse)
async def create_benchmark_output_urls(
    run_context: RunAWSDependency,
    request: OutputURLsRequest,
) -> BenchmarkOutputURLsResponse:
    """Sign one batch of run-owned output keys immediately before download."""
    run_prefix = f"{S3_BENCHMARKS_PREFIX}/{run_context.benchmark.id}/"
    if any(not key.startswith(run_prefix) for key in request.keys):
        raise HTTPException(status_code=400, detail="Output keys must belong to the requested run")

    aws_runtime = run_context.aws_runtime
    expires_in = aws_runtime.clients.maximum_presign_ttl(OUTPUT_URL_EXPIRES_SECONDS)
    urls = await create_presigned_urls(request.keys, aws_runtime, expiration=expires_in)
    files = [OutputURLEntry(key=key, download_url=url) for key, url in zip(request.keys, urls)]

    return BenchmarkOutputURLsResponse(files=files, expires_in=expires_in)


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
