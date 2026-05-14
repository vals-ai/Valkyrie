import logging
import tarfile
import traceback
from typing import Annotated
from uuid import UUID

import httpx
import logfire
import sentry_sdk
from benchmark_service.client import BenchmarkServiceError
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from opentelemetry.propagate import inject
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import joinedload
from sqlmodel import Session

from tracker.auth import (
    extract_api_key,
    find_org_by_tenant,
    forward_tracker_api_key,
    get_current_org,
    resolve_descope_tenant,
)
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    copy_agent_to_benchmark,
    create_benchmark_url,
    create_console_url,
    create_presigned_url,
    download_from_s3_stream,
    list_s3_objects,
    s3_object_exists,
)
from tracker.config import AUTH_REQUIRED, ENVIRONMENT, create_benchmark_service_url
from tracker.database.models import Benchmark, BenchmarkStatus, FinalEvaluation, Org, RetryMode
from tracker.database.scoping import assert_org, get_scoped
from tracker.database.session import check_database_connection, get_session
from tracker.exceptions import TrackerServiceError
from tracker.logging import benchmark_id_var, configure_logging, get_logger, request_id_var
from tracker.middleware import RequestContextMiddleware
from tracker.observability import configure_observability
from tracker.types import (
    BenchmarkTableRow,
    FetchBenchmarkTasksRequest,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    HarnessConfig,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkErrorResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
)
from tracker.utils import (
    BenchmarkContext,
    YieldingWriter,
    commit_benchmark_error,
    create_benchmark_service_client,
    create_final_view,
    fetch_filtered_benchmark_rows,
    fetch_harness_config,
    force_stop_sandboxes,
    initiate_stop_benchmark,
    process_benchmark,
    reset_to_in_progress_status,
    start_benchmark_request_to_benchmark,
    stream_benchmark_results,
    upload_final_view,
)

configure_logging()
configure_observability("valkyrie-tracker", environment=ENVIRONMENT)

logger = get_logger(__name__)

app = FastAPI()

logfire.instrument_fastapi(app, excluded_urls="/health$")

app.add_middleware(RequestContextMiddleware)


# Preserve health check log suppression after configure_logging() replaced handlers
class HealthCheckFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find("/health") == -1


logging.getLogger("uvicorn.access").addFilter(HealthCheckFilter())


def bind_benchmark_id(benchmark_id: UUID) -> UUID:
    """Dependency that binds benchmark_id to the logging context."""
    benchmark_id_var.set(str(benchmark_id))
    return benchmark_id


TrackedBenchmarkId = Annotated[UUID, Depends(bind_benchmark_id)]


def _taskiq_labels() -> dict[str, str]:
    """Labels attached to a kicked task: current request id + injected OTel trace context."""
    trace_context: dict[str, str] = {}
    inject(trace_context)
    return {"request_id": request_id_var.get(), **trace_context}


@app.exception_handler(TrackerServiceError)
async def tracker_service_error_handler(_request: Request, exc: TrackerServiceError):
    logger.error(exc, exc_info=True)
    sentry_sdk.capture_exception(exc)
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.exception_handler(BenchmarkServiceError)
async def benchmark_service_error_handler(_request: Request, exc: BenchmarkServiceError):
    logger.error(exc, exc_info=True)
    sentry_sdk.capture_exception(exc)
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health_check() -> dict[str, str]:
    """
    Health check to ensure that the tracker service is running correctly.

    Usage:
    curl -X GET http://<endpoint>/health

    Returns:
    {
        "status": "ok"
    }

    Returns:
    - 200 OK if the server is running and database is accessible
    - 503 Service Unavailable if the database is not accessible
    """
    if not check_database_connection():
        raise HTTPException(status_code=503, detail="Database is not accessible")
    return {"status": "ok"}


@app.post("/init")
def init_org(
    request: Request,
    session: Session = Depends(get_session),
) -> dict[str, str | bool]:
    """Initialize org for hosted mode. Validates Descope key and creates org if needed."""
    if not AUTH_REQUIRED:
        raise HTTPException(status_code=405, detail="Init is only available in hosted mode")

    api_key = extract_api_key(request)
    tenant_name = resolve_descope_tenant(api_key)

    stmt = pg_insert(Org).values(name=tenant_name).on_conflict_do_nothing(index_elements=["name"])
    result = session.exec(stmt)
    created = result.rowcount > 0
    session.commit()

    org = find_org_by_tenant(tenant_name, session)
    if not org:
        raise HTTPException(status_code=500, detail="Internal error during org creation")
    return {"org_name": org.name, "created": created}


@app.post("/start-benchmark")
async def start_benchmark(
    http_request: Request,
    request: StartBenchmarkRequest,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> StartBenchmarkResponse:
    """
    Start a benchmark run with the uploaded contract.

    Usage:
    curl -X POST http://<endpoint>/start-benchmark \
      -H "Content-Type: application/json" \
      -d '{"agent_name": "claude_code", "benchmark_name": "swebench", "task_ids": ["astropy__astropy-12907"]}'

    Returns:
        StartBenchmarkResponse

    Returns:
    - 200 OK if benchmark starts successfully
    - 400 Bad Request if parameters are invalid
    - 500 Internal Server Error if benchmark fails to start
    """
    request = request.model_copy(
        update={
            "service_headers": forward_tracker_api_key(
                request.service_headers,
                http_request.headers.get("x-api-key"),
            )
        }
    )
    logger.info(f"Starting benchmark run - contract: {request.contract.name}, benchmark: {request.benchmark_name}")

    benchmark_service = request.benchmark_service

    # Check service is running
    try:
        _ = await benchmark_service.health_check()
    except httpx.ConnectError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Benchmark service '{request.benchmark_name}' is not reachable",
        ) from exc

    # Create benchmark row inside of database to mark start of the benchmark
    benchmark_row = start_benchmark_request_to_benchmark(request, org)
    session.add(benchmark_row)
    session.commit()
    benchmark_id_var.set(str(benchmark_row.id))

    # Verify task ids passed in (they exist within dataset and all dependencies are met to run them)
    try:
        verify_response = await benchmark_service.verify_task_ids(
            task_ids=request.task_ids, slice_str=request.slice_str, dataset=request.dataset
        )

        # Copy agent so edits to agents/<name>.zip during the run doesn't affect it
        await copy_agent_to_benchmark(
            str(benchmark_row.id),
            request.contract.name,
            request.harness_config.aws,
            request.harness_config.s3_bucket,
        )
    except Exception as e:
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        commit_benchmark_error(benchmark_row, session, error_message)
        error_response = StartBenchmarkErrorResponse(
            benchmark_id=benchmark_row.id,
            error_message=error_message,
        )

        raise TrackerServiceError(error_response.model_dump_json()) from e

    await (
        process_benchmark.kicker()
        .with_labels(**_taskiq_labels())
        .kiq(
            start_benchmark_request_json=request.model_dump(),
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verify_response.task_ids,
        )
    )

    return StartBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        agent_name=request.contract.name,
        benchmark_id=benchmark_row.id,
        concurrency=request.concurrency,
        started_at=benchmark_row.started_at,
        task_count=len(verify_response.task_ids),
        cloudwatch_url=get_benchmark_log_url(
            str(benchmark_row.id), request.harness_config.aws.aws_default_region, request.harness_config.log_group
        ),
        s3_bucket_url=create_benchmark_url(
            str(benchmark_row.id), request.harness_config.aws.aws_default_region, request.harness_config.s3_bucket
        ),
    )


@app.post("/fetch-benchmark-tasks")
async def fetch_benchmark_tasks(
    http_request: Request,
    request: FetchBenchmarkTasksRequest,
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    _org: Org = Depends(get_current_org),
) -> VerifyTaskIdsResponse:
    """
    Fetch all task ids for a benchmark dataset.
    """
    try:
        benchmark_service = create_benchmark_service_client(
            url=request.custom_benchmark_service or create_benchmark_service_url(request.benchmark_name),
            daytona_secret_name=harness_config.daytona_secret_name,
            aws=harness_config.aws,
            service_headers=forward_tracker_api_key(request.service_headers, http_request.headers.get("x-api-key")),
        )
        try:
            return await benchmark_service.verify_task_ids(
                task_ids=None,
                slice_str=None,
                dataset=request.dataset,
            )
        finally:
            await benchmark_service.close()
    except (BenchmarkServiceError, httpx.HTTPError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch task ids from benchmark service '{request.benchmark_name}': {exc}",
        ) from exc


@app.get("/fetch-benchmark", response_model=None)
async def fetch_benchmark(
    benchmark_id: TrackedBenchmarkId,
    connect: bool = Query(default=False),
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> FetchBenchmarkResponse | StreamingResponse:
    """
    Fetch a benchmark by its id.

    Usage:
    curl -X GET http://<endpoint>/fetch-benchmark/<benchmark_id>?connect=true

    Returns:
        FetchBenchmarkResponse

    Returns:
    - 200 OK if benchmark is found
    - 404 Not Found if benchmark is not found
    """
    benchmark_row = get_scoped(Benchmark, benchmark_id, session, org)

    # When we connect to the client every 60 seconds we send the latest benchmark status
    # and additional updates about the tasks completed
    if connect:
        return StreamingResponse(
            stream_benchmark_results(benchmark_id, session, harness_config, org),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    benchmark_context = BenchmarkContext(benchmark_row, session, org)

    return FetchBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        benchmark_id=benchmark_row.id,
        details=benchmark_context.benchmark_details,
        s3_bucket_url=create_benchmark_url(
            str(benchmark_row.id), harness_config.aws.aws_default_region, harness_config.s3_bucket
        ),
    )


@app.get("/retrieve-results")
async def retrieve_results(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    s3: bool = Query(default=False),
    task_ids: list[str] | None = Query(default=None),
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> RetrieveResultsResponse:
    """
    Retrieve the results of a benchmark by its id. When task_ids is non-empty, the final view is
    filtered to that subset and the final score is recomputed over the subset; the persisted
    FinalEvaluation / per-task rows are left untouched.

    Note: with `s3=True` the S3 final view at the canonical key is overwritten with whatever was
    just computed (full or subset). The DB remains source of truth, so re-running without
    task_ids re-uploads the canonical full view.

    Usage:
    curl -X GET http://<endpoint>/retrieve-results?benchmark_id=<uuid>&s3=false
    curl -X GET 'http://<endpoint>/retrieve-results?benchmark_id=<uuid>&task_ids=task_1&task_ids=task_2'
    """
    benchmark_row = session.get(Benchmark, benchmark_id, options=[joinedload(Benchmark.final_evaluation)])
    assert_org(benchmark_row, org)

    final_view = create_final_view(benchmark_row, session, org)

    if task_ids:
        task_ids_set = set(task_ids)

        def _filter_task_map(task_map):
            return {task_id: value for task_id, value in (task_map or {}).items() if task_id in task_ids_set} or None

        final_view.evaluation_results = _filter_task_map(final_view.evaluation_results)
        final_view.task_errors = _filter_task_map(final_view.task_errors)

        effective_service_headers = forward_tracker_api_key(None, http_request.headers.get("x-api-key"))
        benchmark_service = benchmark_row.benchmark_service(
            harness_config.daytona_secret_name,
            harness_config.aws,
            service_headers=effective_service_headers,
        )
        try:
            resp = await benchmark_service.final_score(
                evaluation_results=final_view.evaluation_results or {},
                dataset=benchmark_row.arguments.dataset,
            )
        finally:
            await benchmark_service.close()
        final_view.final_evaluation = FinalEvaluation(
            org_id=org.id,
            benchmark=benchmark_row.id,
            final_score=resp.final_score,
            properties=resp.metadata,
        )

    if s3:
        s3_key = await upload_final_view(benchmark_row, final_view, harness_config)

        https_url = f"s3://{harness_config.s3_bucket}/{s3_key}"
        presigned_url = create_presigned_url(s3_key, harness_config.aws, harness_config.s3_bucket)
        console_url = create_console_url(s3_key, harness_config.aws.aws_default_region, harness_config.s3_bucket)

        return S3UploadResultsResponse(s3_url=https_url, presigned_url=presigned_url, console_url=console_url)

    return final_view


@app.get("/check-results-exist")
async def check_results_exist(
    benchmark_id: TrackedBenchmarkId,
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> dict[str, bool]:
    """
    Check if results.json already exists in S3 for the given benchmark.

    Usage:
    curl -X GET http://<endpoint>/check-results-exist?benchmark_id=<uuid>

    Returns:
        {"exists": true/false}
    """
    get_scoped(Benchmark, benchmark_id, session, org)

    s3_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/results.json"
    exists = s3_object_exists(s3_key, harness_config.aws, harness_config.s3_bucket)
    return {"exists": exists}


@app.post("/stop-benchmark/{benchmark_id}")
async def stop_benchmark(
    benchmark_id: TrackedBenchmarkId,
    force: bool = Query(default=False),
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> StopBenchmarkResponse:
    """
    Stop a benchmark by its id.
    If force is True, the sandboxes will be stopped and deleted, even if the tasks are in progress.

    Usage:
    curl -X POST http://<endpoint>/stop-benchmark/<benchmark_id>?force=true

    Returns:
        StopBenchmarkResponse
    """
    benchmark_row = get_scoped(Benchmark, benchmark_id, session, org)

    valid_stop_states = [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPING]

    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status} state. Can only pause an in progress or error run.",
        )

    await initiate_stop_benchmark(benchmark_row, session, force, org)

    if force:
        await force_stop_sandboxes(benchmark_row, session, harness_config.daytona_secret_name, harness_config.aws, org)

    return StopBenchmarkResponse(
        status="success",
    )


@app.post("/retry-or-resume-benchmark/{benchmark_id}")
async def retry_or_resume_benchmark(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    retry: bool = Query(default=False),
    retry_mode: RetryMode = Query(default=RetryMode.AUTO),
    concurrency: int | None = Query(default=None),
    task_ids: list[str] = Body(default=[]),
    service_headers: dict[str, str] = Body(default={}),
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> RetryOrResumeBenchmarkResponse:
    """
    Retry or resume a benchmark run by its id, we only can retry or resume a benchmark if its not currently running.

    Usage:
    curl -X POST http://<endpoint>/retry-or-resume-benchmark/<benchmark_id>?retry=true&concurrency=20
      -d '{"task_ids": ["task_id_1", "task_id_2"]}'

    Args:
        benchmark_id: The benchmark ID to retry/resume
        retry: If true, retry failed tasks. If false, resume from where it left off
        concurrency: Optional new concurrency level (overrides original value)
        task_ids: Optional list of specific task IDs to run. If a task id is not yet
            registered but is valid in the current dataset, a fresh PENDING row is created.

    Returns:
        RetryOrResumeBenchmarkResponse
    """
    benchmark_row = get_scoped(Benchmark, benchmark_id, session, org)

    invalid_states = [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING]

    if benchmark_row.status in invalid_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is in the {benchmark_row.status} state. Cannot continue a run that is currently running.",
        )

    effective_service_headers = forward_tracker_api_key(
        service_headers,
        http_request.headers.get("x-api-key"),
    )

    # NOTE: 0 is not acceptable
    if concurrency:
        benchmark_row.arguments.concurrency = concurrency
        session.add(benchmark_row)
        session.commit()

    # Reset tasks and retry or resume benchmark
    verified_task_ids = await reset_to_in_progress_status(
        benchmark_row=benchmark_row,
        session=session,
        benchmark_service=benchmark_row.benchmark_service(
            harness_config.daytona_secret_name, harness_config.aws, service_headers=effective_service_headers
        ),
        retry=retry,
        retry_mode=retry_mode,
        rerun_task_ids=task_ids,
        org=org,
    )

    # Ensure that credentials are included with the model dump
    resume_request_json = benchmark_row.start_benchmark_request(
        harness_config, service_headers=effective_service_headers
    ).model_dump()

    # start the benchmark with the same args used to create it
    # we will delegate inside what tasks we are running
    await (
        process_benchmark.kicker()
        .with_labels(**_taskiq_labels())
        .kiq(
            start_benchmark_request_json=resume_request_json,
            benchmark_id_str=str(benchmark_row.id),
            verified_task_ids=verified_task_ids,
        )
    )

    return RetryOrResumeBenchmarkResponse(
        status="success",
    )


@app.get("/fetch-benchmarks")
async def fetch_benchmarks(
    request: FetchBenchmarksRequest = Depends(),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> FetchBenchmarksResponse:
    """
    Fetch benchmarks based on the request parameters.

    Usage:
    curl -X GET http://<endpoint>/fetch-benchmarks?agent_name=claude_code&benchmark_name=swebench&status=IN_PROGRESS&order_by=DESC&limit=5&offset=0

    Returns:
        list[FetchBenchmarksResponse]
    """

    benchmark_rows, total_count = fetch_filtered_benchmark_rows(request, session, org)

    benchmark_table_rows: list[BenchmarkTableRow] = [
        benchmark_row.create_benchmark_table_row(session) for benchmark_row in benchmark_rows
    ]

    return FetchBenchmarksResponse(
        benchmarks=benchmark_table_rows,
        total_count=total_count,
    )


@app.get("/fetch-benchmark-metadata/{benchmark_id}")
async def fetch_benchmark_metadata(
    benchmark_id: TrackedBenchmarkId,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> FetchBenchmarkMetadataResponse:
    """
    Fetch benchmark metadata by its id.

    Usage:
    curl -X GET http://<endpoint>/fetch-benchmark-metadata/<benchmark_id>

    Returns:
        FetchBenchmarkMetadataResponse
    """
    benchmark_row = get_scoped(Benchmark, benchmark_id, session, org)

    return benchmark_row.benchmark_metadata


@app.get("/fetch-agent-outputs/{benchmark_id}")
async def fetch_agent_outputs(
    benchmark_id: TrackedBenchmarkId,
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
    task_ids: list[str] | None = Query(default=None),
) -> StreamingResponse:
    """
    Stream a tar file with agent outputs to the client.

    Usage:
    curl -X GET http://<endpoint>/fetch-agent-outputs/<benchmark_id>

    Returns:
        StreamingResponse
    """
    get_scoped(Benchmark, benchmark_id, session, org)

    benchmark_prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"
    if task_ids:
        s3_keys = [
            key
            for task_id in task_ids
            for key in list_s3_objects(f"{benchmark_prefix}{task_id}/", harness_config.aws, harness_config.s3_bucket)
        ]
    else:
        s3_keys = list_s3_objects(benchmark_prefix, harness_config.aws, harness_config.s3_bucket)

    if not s3_keys:
        raise HTTPException(
            status_code=404,
            detail=f"No outputs found for run '{benchmark_id}'",
        )

    def tar_generator():
        writer: YieldingWriter = YieldingWriter()

        with tarfile.open(fileobj=writer, mode="w|") as tar:
            for s3_key in s3_keys:
                relative_path: str = s3_key.removeprefix(benchmark_prefix)

                try:
                    body, size = download_from_s3_stream(s3_key, harness_config.aws, harness_config.s3_bucket)

                    tarinfo = tarfile.TarInfo(name=relative_path)
                    tarinfo.size = size

                    tar.addfile(tarinfo, fileobj=body)

                    chunk = writer.pop()
                    if chunk:
                        yield chunk

                except Exception as e:
                    logger.warning(f"Failed to add {s3_key} to tar: {e}", exc_info=True)

                    continue

        final_chunk = writer.pop()
        if final_chunk:
            yield final_chunk

    return StreamingResponse(
        tar_generator(),
        media_type="application/x-tar",
        headers={"Content-Disposition": f"attachment; filename=benchmark_{benchmark_id}_outputs.tar"},
    )
