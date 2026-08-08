import asyncio
import io
import logging
import tarfile
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

import httpx
import logfire
import sentry_sdk
from benchmark_service.client import BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute
from opentelemetry.propagate import inject
from sqlmodel import Session

from tracker.api.agents import router as agents_router
from tracker.api.benchmark_services import router as benchmark_services_router
from tracker.api.benchmarks_status import router as benchmarks_status_router
from tracker.api.filter_options import router as filter_options_router
from tracker.api.single_benchmark import router as single_benchmark_router
from tracker.api.single_task import router as single_task_router
from tracker.auth import (
    RequestIdentity,
    extract_api_key,
    forward_tracker_api_key,
    get_current_org,
    get_current_starter,
    resolve_descope_identity,
)
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.secrets import resolve_secrets
from tracker.agent.contract import get_contract_from_zip_bytes
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    copy_agent_to_benchmark,
    create_benchmark_url,
    delete_from_s3,
    create_console_url,
    create_presigned_url,
    download_from_s3,
    download_many_from_s3,
    get_benchmark_contract_s3_key,
    get_contract_s3_key,
    list_s3_objects,
    s3_object_exists,
)
from tracker.agent.schemas import AgentConfig
from tracker.config import AUTH_REQUIRED, ENVIRONMENT, broker, create_benchmark_service_url
from tracker.database.dependencies import (
    BenchmarkRepositoryDep,
    ExecutorControlRepositoryDep,
    OrgRepositoryDep,
    ReportingRepositoryDep,
    RunControlRepositoryDep,
    TaskRepositoryDep,
)
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    FinalEvaluation,
    Org,
    RetryMode,
)
from tracker.database.repositories import (
    BenchmarkRepository,
    EnqueueFailureResolution,
    ExecutorControlRepository,
    TaskRepository,
)
from tracker.database.scoping import assert_org
from tracker.executor.dispatch_control import admit_recovery_dispatch, admit_start_dispatch
from tracker.database.session import check_database_connection, get_session
from tracker.database.transaction import TrackerTransaction
from tracker.docent_analysis import (
    analyze_event_stream,
)
from tracker.exceptions import MaintenanceModeError, ReleaseControlError, TrackerServiceError
from executor_protocol import EXECUTOR_TASK_NAME, executor_task_signature
from tracker.logging import benchmark_id_var, configure_logging, get_logger, request_id_var
from tracker.executor.release_retirement import AutomaticReleaseRetirement
from tracker.middleware import RequestContextMiddleware
from tracker.observability import configure_observability
from tracker.outbound_security import validate_service_url_syntax
from tracker.types import (
    AnalyzeBenchmarkRequest,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FetchBenchmarkTasksRequest,
    HarnessConfig,
    Order,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
    UpdateBenchmarkConcurrencyRequest,
    UpdateBenchmarkConcurrencyResponse,
)
from tracker.utils import (
    BenchmarkConcurrencyUpdate,
    BenchmarkContext,
    YieldingWriter,
    build_benchmark_table_rows,
    create_benchmark_service_client,
    create_final_view,
    fetch_benchmark_row,
    fetch_filtered_benchmark_rows,
    fetch_harness_config,
    try_fetch_harness_config,
    force_stop_sandboxes,
    initiate_stop_benchmark,
    reset_to_in_progress_status,
    start_benchmark_request_to_benchmark,
    stream_benchmark_results,
    upload_final_view,
    update_benchmark_concurrency,
    update_benchmark_resume_arguments,
)

configure_logging()
configure_observability("valkyrie-tracker", environment=ENVIRONMENT)

logger = get_logger(__name__)

# Tracker publishes the stable wire contract; ExecutorHost resolves the same
# task name before launching the pinned executor artifact.
process_benchmark = broker.task(EXECUTOR_TASK_NAME)(executor_task_signature)


def _operation_id(route: APIRoute) -> str:
    """Use route function name as operation_id so generated client hooks are short.
    E.g. `list_agents` (not `list_agents_agents_get`)."""
    return route.name


@asynccontextmanager
async def tracker_lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    retirement = AutomaticReleaseRetirement()
    retirement.start()
    try:
        yield
    finally:
        retirement.stop()


app = FastAPI(generate_unique_id_function=_operation_id, redirect_slashes=False, lifespan=tracker_lifespan)

logfire.instrument_fastapi(app, excluded_urls="/health$")

app.add_middleware(RequestContextMiddleware)

app.include_router(agents_router)
app.include_router(benchmark_services_router)
app.include_router(benchmarks_status_router)
app.include_router(filter_options_router)
app.include_router(single_benchmark_router)
app.include_router(single_task_router)


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


async def _enqueue_executor_dispatch(
    dispatch: ExecutorDispatch,
    *,
    session_factory: Callable[[], Session],
    caller_session: Session,
    start_benchmark_request_json: dict[str, Any],
    verified_task_ids: list[str],
) -> None:
    for attempt in range(3):
        try:
            await (
                process_benchmark.kicker()
                .with_labels(**_taskiq_labels())
                .kiq(
                    start_benchmark_request_json=start_benchmark_request_json,
                    benchmark_id_str=str(dispatch.benchmark_id),
                    verified_task_ids=verified_task_ids,
                    executor_dispatch_id=str(dispatch.id),
                    executor_release_id=dispatch.executor_release_id,
                    executor_artifact_uri=dispatch.executor_artifact_uri,
                    executor_artifact_digest=dispatch.executor_artifact_digest,
                    executor_protocol_version=dispatch.executor_protocol_version,
                )
            )
            return
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(0.1 * (2**attempt))
                continue

            logger.exception(
                "Executor dispatch enqueue acknowledgement failed",
                extra={"executor_dispatch_id": str(dispatch.id)},
            )
            with session_factory() as resolution_session:
                transaction = TrackerTransaction.from_session(resolution_session)
                resolution = transaction.executor_control.resolve_enqueue_failure(
                    benchmark_id=dispatch.benchmark_id,
                    dispatch_id=dispatch.id,
                    task_ids=verified_task_ids,
                )
                resolution_session.commit()
            caller_session.expire_all()
            if resolution == EnqueueFailureResolution.DELIVERED:
                return
            if resolution == EnqueueFailureResolution.SUPERSEDED:
                raise HTTPException(
                    status_code=409, detail="Executor dispatch was superseded by a newer Retry"
                ) from exc
            raise HTTPException(
                status_code=503,
                detail={
                    "message": "Executor dispatch enqueue acknowledgement failed; use Retry to continue",
                    "benchmark_id": str(dispatch.benchmark_id),
                    "executor_dispatch_id": str(dispatch.id),
                },
            ) from exc


async def _delete_uncommitted_agent_copy(
    *,
    created: bool,
    benchmark_id: UUID,
    request: StartBenchmarkRequest,
) -> None:
    if not created:
        return
    try:
        await delete_from_s3(
            get_benchmark_contract_s3_key(str(benchmark_id), request.contract.name),
            request.harness_config.aws,
            request.harness_config.s3_bucket,
        )
    except Exception:
        logger.exception(
            "Failed to delete uncommitted benchmark agent copy",
            extra={"benchmark_id": str(benchmark_id)},
        )


@app.exception_handler(TrackerServiceError)
async def tracker_service_error_handler(_request: Request, exc: TrackerServiceError):
    logger.error(exc, exc_info=True)
    sentry_sdk.capture_exception(exc)
    raise HTTPException(status_code=500, detail="Tracker service operation failed") from exc


@app.exception_handler(BenchmarkServiceUnauthenticatedError)
async def benchmark_service_unauth_error_handler(_request: Request, exc: BenchmarkServiceUnauthenticatedError):
    logger.warning("Benchmark service authentication failed: %s", exc)
    raise HTTPException(status_code=502, detail="Benchmark service authentication failed") from exc


@app.exception_handler(BenchmarkServiceError)
async def benchmark_service_error_handler(_request: Request, exc: BenchmarkServiceError):
    logger.error(exc, exc_info=True)
    sentry_sdk.capture_exception(exc)
    raise HTTPException(status_code=500, detail="Benchmark service request failed") from exc


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
    org_repository: OrgRepositoryDep,
    request: Request,
    session: Session = Depends(get_session),
) -> dict[str, str | bool]:
    """Initialize org for hosted mode. Validates Descope key and creates org if needed."""
    if not AUTH_REQUIRED:
        raise HTTPException(status_code=405, detail="Init is only available in hosted mode")

    api_key = extract_api_key(request)
    identity = resolve_descope_identity(api_key, include_user_profile=True)

    org, created = org_repository.ensure_by_name(identity.tenant_name)
    session.commit()

    if not org:
        raise HTTPException(status_code=500, detail="Internal error during org creation")

    return {"org_name": org.name, "created": created, "email_claim_missing": identity.email is None}


async def _resolve_contract_from_s3(request: StartBenchmarkRequest) -> AgentContractRequest:
    """Resolve install_cmd/run_cmd/etc by parsing the agent's contract file inside its S3 zip."""
    zip_bytes = await download_from_s3(
        get_contract_s3_key(request.contract.name),
        request.harness_config.aws,
        request.harness_config.s3_bucket,
    )
    agent_config = AgentConfig(model=request.contract.model, kwargs=dict(request.contract.kwargs))
    resolved = get_contract_from_zip_bytes(request.contract.name, zip_bytes, agent_config)
    if request.contract.secrets:
        resolved.secrets = {**resolved.secrets, **request.contract.secrets}
    return resolved


@app.post("/start-benchmark")
async def start_benchmark(
    task_repository: TaskRepositoryDep,
    executor_control_repository: ExecutorControlRepositoryDep,
    http_request: Request,
    request: StartBenchmarkRequest,
    session: Session = Depends(get_session),
    run_starter: RequestIdentity = Depends(get_current_starter),
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
    session.rollback()

    # Prefer harness_config from X-Harness-* headers (web FE); fall back to request body (CLI).
    header_harness_config = try_fetch_harness_config(http_request)
    effective_harness_config = header_harness_config or request.harness_config
    # TODO: Drop the top-level fallback after legacy clients have aged out.
    provider_secret_name = request.harness_config.sandbox_provider_secret_name or request.sandbox_provider_secret_name
    if provider_secret_name:
        effective_harness_config = effective_harness_config.model_copy(
            update={"sandbox_provider_secret_name": provider_secret_name}
        )

    service_headers = dict(request.service_headers)
    if request.service_auth_header_name and request.service_auth_secret_name:
        resolved = resolve_secrets(
            {request.service_auth_header_name: request.service_auth_secret_name},
            effective_harness_config.aws,
        )
        service_headers.update(resolved)

    request = request.model_copy(
        update={
            "harness_config": effective_harness_config,
            "service_headers": forward_tracker_api_key(
                service_headers,
                http_request.headers.get("x-api-key"),
            ),
        }
    )

    if not request.contract.install_cmd and not request.contract.run_cmd:
        request = request.model_copy(update={"contract": await _resolve_contract_from_s3(request)})

    logger.info(f"Starting benchmark run - contract: {request.contract.name}, benchmark: {request.benchmark_name}")

    benchmark_service = request.benchmark_service

    # Validate benchmark service is reachable + tasks resolve BEFORE creating the DB row,
    # so failed auth / unreachable services don't pollute the benchmark list.
    try:
        try:
            _ = await benchmark_service.health_check()
        except httpx.ConnectError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Benchmark service '{request.benchmark_name}' is not reachable",
            ) from exc

        try:
            verify_response = await benchmark_service.verify_task_ids(
                task_ids=request.task_ids, slice_str=request.slice_str, dataset=request.dataset
            )
        except BenchmarkServiceUnauthenticatedError as exc:
            logger.warning("Benchmark service authentication failed for %s: %s", request.benchmark_name, exc)
            raise HTTPException(status_code=502, detail="Benchmark service authentication failed") from exc
        except Exception as exc:
            logger.error("Failed to verify task ids for %s", request.benchmark_name, exc_info=True)
            raise HTTPException(status_code=502, detail="Failed to verify task ids") from exc
    finally:
        try:
            await benchmark_service.close()
        except Exception:
            logger.exception("Failed to close benchmark service client for %s", request.benchmark_name)

    benchmark_row = start_benchmark_request_to_benchmark(request, run_starter)
    dispatch_id = uuid4()
    agent_copy_created = False
    try:
        agent_copy_created = bool(
            await copy_agent_to_benchmark(
                str(benchmark_row.id),
                request.contract.name,
                request.harness_config.aws,
                request.harness_config.s3_bucket,
            )
        )
        session.add(benchmark_row)
        session.flush()
        task_repository.create_missing_task_rows(
            benchmark_row.id,
            verify_response.task_ids,
            benchmark_row.org_id,
        )
        executor_dispatch = admit_start_dispatch(
            session,
            benchmark=benchmark_row,
            dispatch_id=dispatch_id,
            executor_control_repository=executor_control_repository,
        )
        session.commit()
    except ReleaseControlError as exc:
        session.rollback()
        await _delete_uncommitted_agent_copy(
            created=agent_copy_created,
            benchmark_id=benchmark_row.id,
            request=request,
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        session.rollback()
        await _delete_uncommitted_agent_copy(
            created=agent_copy_created,
            benchmark_id=benchmark_row.id,
            request=request,
        )
        raise TrackerServiceError("Failed to admit benchmark execution") from exc

    benchmark_id_var.set(str(benchmark_row.id))

    if run_starter.access_key_id is not None and run_starter.email is None:
        logger.warning(
            "Access key %s resolved no user email; run attribution for this run will be empty",
            run_starter.access_key_id,
        )

    session.rollback()
    await _enqueue_executor_dispatch(
        executor_dispatch,
        session_factory=lambda: Session(bind=session.bind, expire_on_commit=False),
        caller_session=session,
        start_benchmark_request_json=request.model_dump(),
        verified_task_ids=verify_response.task_ids,
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
        executor_release_id=benchmark_row.executor_release_id,
        current_execution_release_id=benchmark_row.current_execution_release_id,
        executor_artifact_digest=benchmark_row.executor_artifact_digest,
        executor_protocol_version=benchmark_row.executor_protocol_version,
    )


@app.post("/fetch-benchmark-tasks")
async def fetch_benchmark_tasks(
    http_request: Request,
    request: FetchBenchmarkTasksRequest,
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    _org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> VerifyTaskIdsResponse:
    """
    Fetch all task ids for a benchmark dataset.
    """
    session.rollback()
    try:
        benchmark_service = create_benchmark_service_client(
            url=request.custom_benchmark_service or create_benchmark_service_url(request.benchmark_name),
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
        logger.warning("Failed to fetch task ids from benchmark service %s: %s", request.benchmark_name, exc)
        raise HTTPException(status_code=502, detail="Failed to fetch task ids from benchmark service") from exc


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
    session.rollback()
    read_session = Session(bind=session.bind)
    try:
        transaction = TrackerTransaction.from_session(read_session)
        benchmark_row = assert_org(transaction.benchmarks.get_for_org_with_final_evaluation(benchmark_id, org.id), org)
    except BaseException:
        read_session.rollback()
        read_session.close()
        raise

    # When we connect to the client every 60 seconds we send the latest benchmark status
    # and additional updates about the tasks completed
    if connect:
        read_session.rollback()
        read_session.close()
        return StreamingResponse(
            stream_benchmark_results(benchmark_id, harness_config, org),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    try:
        benchmark_context = BenchmarkContext(benchmark_row, transaction.reporting, org)
        response = FetchBenchmarkResponse(
            benchmark_name=benchmark_row.name,
            benchmark_id=benchmark_row.id,
            details=benchmark_context.benchmark_details,
            s3_bucket_url=create_benchmark_url(
                str(benchmark_row.id), harness_config.aws.aws_default_region, harness_config.s3_bucket
            ),
            label=benchmark_row.label,
            final_score=benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None,
            error_message=benchmark_row.error_message if benchmark_row.status == BenchmarkStatus.ERROR else None,
            executor_release_id=benchmark_row.executor_release_id,
            current_execution_release_id=benchmark_row.current_execution_release_id,
            executor_artifact_digest=benchmark_row.executor_artifact_digest,
            executor_protocol_version=benchmark_row.executor_protocol_version,
        )
    finally:
        read_session.rollback()
        read_session.close()
    return response


@app.post("/analyze-benchmark/{benchmark_id}", response_model=None)
async def analyze_benchmark(
    benchmark_id: TrackedBenchmarkId,
    body: AnalyzeBenchmarkRequest,
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> dict[str, str] | StreamingResponse:
    """
    Invoke the Docent analyzer Lambda for a benchmark and emit SSE-formatted progress events
    (started/heartbeat/done/error) that clients consume as buffered text (the response is read
    in full; the terminal done/error event carries the result).

    Cache short-circuit: when the benchmark already has docent_reading_status=DONE and
    no_cache=false, returns the existing reading_plan_url without invoking the Lambda.
    """
    session.rollback()
    read_session = Session(bind=session.bind)
    try:
        transaction = TrackerTransaction.from_session(read_session)
        benchmark_row = assert_org(transaction.benchmarks.get_for_org(benchmark_id, org.id), org)
    except BaseException:
        read_session.rollback()
        read_session.close()
        raise

    benchmark_status = benchmark_row.status
    contract_name = benchmark_row.arguments.contract.name
    if benchmark_status != BenchmarkStatus.FINISHED:
        read_session.rollback()
        read_session.close()
        raise HTTPException(
            status_code=400,
            detail=f"Cannot analyze run {benchmark_id}: status is {benchmark_status.value} (must be FINISHED).",
        )

    if (
        not body.no_cache
        and benchmark_row.docent_reading_status == DocentReadingStatus.DONE
        and benchmark_row.docent_reading_url
    ):
        response = {
            "status": "done",
            "reading_plan_url": benchmark_row.docent_reading_url,
        }
        read_session.rollback()
        read_session.close()
        return response

    if not body.lambda_function:
        read_session.rollback()
        read_session.close()
        raise HTTPException(
            status_code=400,
            detail=(
                f"No ingest_lambda provided for agent '{contract_name}'. "
                "The CLI normally resolves this from the agent's pushed contract — if you're "
                "calling this endpoint directly, supply `lambda_function` in the request body."
            ),
        )

    payload: dict[str, Any] = {
        "benchmark_id": str(benchmark_id),
        "benchmark_name": benchmark_row.name,
        "s3_bucket": harness_config.s3_bucket,
        "contract": {"name": benchmark_row.arguments.contract.name},
    }
    read_session.rollback()
    read_session.close()

    return StreamingResponse(
        analyze_event_stream(
            benchmark_id=benchmark_id,
            lambda_function=body.lambda_function,
            payload=payload,
            aws=harness_config.aws,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/retrieve-results")
async def retrieve_results(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    s3: bool = Query(default=False),
    task_ids: list[str] | None = Query(default=None),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
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
    session.rollback()
    read_session = Session(bind=session.bind)
    try:
        transaction = TrackerTransaction.from_session(read_session)
        benchmark_repository = transaction.benchmarks
        reporting_repository = transaction.reporting
        benchmark_row = benchmark_repository.get_for_org_with_final_evaluation(benchmark_id, org.id)
    except BaseException:
        read_session.rollback()
        read_session.close()
        raise
    if benchmark_row is None:
        # Keep the legacy distinction between a missing run and a run owned by another org.
        benchmark_exists = benchmark_repository.get_by_id(benchmark_id) is not None
        read_session.rollback()
        read_session.close()
        if benchmark_exists:
            raise HTTPException(status_code=404, detail="Not found")
        raise HTTPException(status_code=404, detail=f"Run {benchmark_id} not found")

    try:
        final_view = create_final_view(benchmark_row, reporting_repository, org)
        if benchmark_row.final_evaluation is not None:
            final_view.final_evaluation = FinalEvaluation.model_validate(benchmark_row.final_evaluation.model_dump())

        # Snapshot every ORM value needed after the request transaction ends. External work must not
        # access expired benchmark attributes and reopen the request session implicitly.
        benchmark_id_value = benchmark_row.id
        benchmark_name_value = benchmark_row.name
        benchmark_dataset = benchmark_row.arguments.dataset
        benchmark_service_url = benchmark_row.custom_benchmark_service or create_benchmark_service_url(
            benchmark_row.name
        )
        benchmark_snapshot = Benchmark.model_construct(id=benchmark_id_value, name=benchmark_name_value)
    except BaseException:
        read_session.rollback()
        read_session.close()
        raise

    if task_ids:
        task_ids_set = set(task_ids)

        def _filter_task_map(task_map: dict[str, Any] | None) -> dict[str, Any] | None:
            return {task_id: value for task_id, value in (task_map or {}).items() if task_id in task_ids_set} or None

        # Include every requested task with its result or None, so tasks without a result
        # (e.g. stopped/errored) still count toward the denominator instead of being dropped.
        try:
            scored_results = {
                task_id: result
                for task_id, result in reporting_repository.fetch_final_score_inputs(benchmark_row.id, org.id).items()
                if task_id in task_ids_set
            }
        except BaseException:
            read_session.rollback()
            read_session.close()
            raise

        read_session.rollback()
        read_session.close()
        final_view.evaluation_results = _filter_task_map(final_view.evaluation_results)
        final_view.task_errors = _filter_task_map(final_view.task_errors)

        effective_service_headers = forward_tracker_api_key(None, http_request.headers.get("x-api-key"))
        benchmark_service = create_benchmark_service_client(
            url=benchmark_service_url,
            service_headers=effective_service_headers,
        )
        try:
            resp = await benchmark_service.final_score(
                evaluation_results=scored_results,
                dataset=benchmark_dataset,
            )
        finally:
            await benchmark_service.close()
        final_view.final_evaluation = FinalEvaluation(
            org_id=org.id,
            benchmark=benchmark_id_value,
            final_score=resp.final_score,
            properties=resp.metadata,
        )

    if not task_ids:
        read_session.rollback()
        read_session.close()

    if s3:
        s3_key = await upload_final_view(benchmark_snapshot, final_view, harness_config)

        https_url = f"s3://{harness_config.s3_bucket}/{s3_key}"
        presigned_url = await create_presigned_url(
            s3_key, harness_config.aws, harness_config.s3_bucket, expiration=86400
        )
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
    Check if the benchmark's final view already exists in S3.

    Usage:
    curl -X GET http://<endpoint>/check-results-exist?benchmark_id=<uuid>

    Returns:
        {"exists": true/false}
    """
    session.rollback()
    read_session = Session(bind=session.bind)
    try:
        transaction = TrackerTransaction.from_session(read_session)
        benchmark_row = assert_org(transaction.benchmarks.get_for_org(benchmark_id, org.id), org)
        benchmark_name = benchmark_row.name
    except BaseException:
        read_session.rollback()
        read_session.close()
        raise
    read_session.rollback()
    read_session.close()

    s3_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{benchmark_name}.json"
    exists = await s3_object_exists(s3_key, harness_config.aws, harness_config.s3_bucket)
    return {"exists": exists}


async def validate_tasks_exist(
    benchmark_row: Benchmark,
    task_ids: list[str],
    task_repository: TaskRepository,
    org: Org,
) -> list[str]:
    """Validate that selected tasks belong to the run."""
    if not task_ids:
        raise HTTPException(status_code=400, detail="task_ids cannot be empty")

    requested_task_ids = list(dict.fromkeys(task_ids))
    existing_task_ids = task_repository.get_existing_task_ids(benchmark_row.id, requested_task_ids, org.id)
    missing_task_ids = [task_id for task_id in requested_task_ids if task_id not in existing_task_ids]
    if missing_task_ids:
        raise HTTPException(
            status_code=400,
            detail=f"Task IDs are not part of run {benchmark_row.id}: {', '.join(missing_task_ids)}",
        )

    return requested_task_ids


@app.post("/stop-benchmark/{benchmark_id}")
async def stop_benchmark(
    benchmark_repository: BenchmarkRepositoryDep,
    task_repository: TaskRepositoryDep,
    executor_control_repository: ExecutorControlRepositoryDep,
    run_control_repository: RunControlRepositoryDep,
    benchmark_id: TrackedBenchmarkId,
    force: bool = Query(default=False),
    task_ids: list[str] | None = Body(default=None, embed=True),
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
    session.expire_all()
    benchmark_row = assert_org(benchmark_repository.get_for_org(benchmark_id, org.id), org)

    valid_stop_states = [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPING]

    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status} state. Can only pause an in progress or error run.",
        )

    selected_task_ids = (
        await validate_tasks_exist(benchmark_row, task_ids, task_repository, org) if task_ids is not None else None
    )

    benchmark_row = fetch_benchmark_row(benchmark_id, benchmark_repository, org, for_update=True)
    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status} state. Can only pause an in progress or error run.",
        )

    await initiate_stop_benchmark(
        benchmark_row,
        session,
        force,
        org,
        task_ids=selected_task_ids,
        repository=run_control_repository,
    )

    if force:
        # TODO: Drop the row fallback after legacy benchmark rows have aged out.
        provider_secret_name = (
            benchmark_row.arguments.sandbox_provider_secret_name or harness_config.sandbox_provider_secret_name
        )
        await force_stop_sandboxes(
            benchmark_row,
            session,
            provider_secret_name,
            harness_config.aws,
            org,
            sandbox_provider=benchmark_row.arguments.sandbox_provider,
            task_ids=selected_task_ids,
            repository=run_control_repository,
            benchmark_repository=benchmark_repository,
            executor_control_repository=executor_control_repository,
        )

    return StopBenchmarkResponse(
        status="success",
    )


def _update_benchmark_concurrency(
    benchmark_id: UUID,
    concurrency: int,
    session: Session,
    benchmark_repository: BenchmarkRepository,
    executor_control_repository: ExecutorControlRepository,
    org: Org,
) -> BenchmarkConcurrencyUpdate:
    try:
        with session.no_autoflush:
            executor_control_repository.lock_executor_admission()
        benchmark_row = update_benchmark_concurrency(
            benchmark_id,
            concurrency,
            session,
            benchmark_repository,
            org,
        )
        if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            status = benchmark_row.status
            raise HTTPException(
                status_code=409,
                detail=f"Run {benchmark_id} is currently in the {status} state.",
            )
        session.commit()
        return benchmark_row
    except MaintenanceModeError as exc:
        session.rollback()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=404, detail="Not found") from exc
    except HTTPException:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise


@app.patch("/benchmarks/{benchmark_id}/concurrency")
def patch_benchmark_concurrency(
    benchmark_repository: BenchmarkRepositoryDep,
    executor_control_repository: ExecutorControlRepositoryDep,
    benchmark_id: TrackedBenchmarkId,
    request: UpdateBenchmarkConcurrencyRequest,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> UpdateBenchmarkConcurrencyResponse:
    benchmark_row = _update_benchmark_concurrency(
        benchmark_id,
        request.concurrency,
        session,
        benchmark_repository,
        executor_control_repository,
        org,
    )
    return UpdateBenchmarkConcurrencyResponse(
        benchmark_id=benchmark_row.benchmark_id,
        status=benchmark_row.status,
        concurrency=benchmark_row.concurrency,
    )


@app.post("/retry-or-resume-benchmark/{benchmark_id}")
async def retry_or_resume_benchmark(
    benchmark_repository: BenchmarkRepositoryDep,
    run_control_repository: RunControlRepositoryDep,
    executor_control_repository: ExecutorControlRepositoryDep,
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    retry: bool = Query(default=False),
    retry_mode: RetryMode = Query(default=RetryMode.AUTO),
    concurrency: int | None = Query(default=None),
    task_ids: list[str] = Body(default=[]),
    service_headers: dict[str, str] = Body(default={}),
    secrets: dict[str, str] = Body(default={}),
    benchmark_url: str | None = Body(default=None),
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
) -> RetryOrResumeBenchmarkResponse:
    """
    Retry or resume a benchmark run by its id.

    Usage:
    curl -X POST http://<endpoint>/retry-or-resume-benchmark/<benchmark_id>?retry=true&concurrency=20
      -d '{"task_ids": ["task_id_1", "task_id_2"]}'

    Args:
        benchmark_id: The benchmark ID to retry/resume
        retry: If true, retry failed tasks. If false, resume from where it left off
        concurrency: Optional new concurrency level (overrides original value)
        task_ids: Optional list of specific task IDs to run. If a task id is not yet
            registered but is valid in the current dataset, a fresh PENDING row is created.
        benchmark_url: Optional replacement benchmark service URL stored on the run.

    Returns:
        RetryOrResumeBenchmarkResponse
    """
    session.expire_all()
    benchmark_row = assert_org(benchmark_repository.get_for_org(benchmark_id, org.id), org)

    if benchmark_row.status == BenchmarkStatus.STOPPING:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is in the {benchmark_row.status} state. Cannot continue a run that is stopping.",
        )

    if benchmark_url is not None:
        try:
            benchmark_url = validate_service_url_syntax(benchmark_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    if concurrency is not None and concurrency < 1:
        raise HTTPException(status_code=400, detail="Concurrency must be greater than 0.")

    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and not retry:
        if concurrency is not None:
            _update_benchmark_concurrency(
                benchmark_id,
                concurrency,
                session,
                benchmark_repository,
                executor_control_repository,
                org,
            )
        if secrets or benchmark_url is not None:
            update_benchmark_resume_arguments(
                benchmark_id,
                benchmark_repository,
                session,
                org,
                secrets=secrets,
                concurrency=None,
                benchmark_url=benchmark_url,
            )
            session.commit()
        return RetryOrResumeBenchmarkResponse(status="success")

    effective_service_headers = forward_tracker_api_key(
        service_headers,
        http_request.headers.get("x-api-key"),
    )

    dispatch_id = uuid4()
    pre_action_status: BenchmarkStatus | None = None
    phase_status: list[BenchmarkStatus] = []
    try:
        benchmark_service = benchmark_row.benchmark_service(
            service_headers=effective_service_headers,
            benchmark_url=benchmark_url,
        )
        try:
            verified_task_ids = await reset_to_in_progress_status(
                benchmark_row=benchmark_row,
                repository=run_control_repository,
                benchmark_repository=benchmark_repository,
                executor_control_repository=executor_control_repository,
                benchmark_service=benchmark_service,
                retry=retry,
                retry_mode=retry_mode,
                rerun_task_ids=task_ids,
                org=org,
                phase_status=phase_status,
            )
        finally:
            try:
                await benchmark_service.close()
            except Exception:
                logger.exception("Failed to close benchmark service client for %s", benchmark_row.name)

        pre_action_status = phase_status[0] if phase_status else benchmark_row.status
        if pre_action_status == BenchmarkStatus.IN_PROGRESS and not verified_task_ids:
            if secrets or concurrency is not None or benchmark_url is not None:
                update_benchmark_resume_arguments(
                    benchmark_id,
                    benchmark_repository,
                    session,
                    org,
                    secrets=secrets,
                    concurrency=concurrency,
                    benchmark_url=benchmark_url,
                )
                session.commit()
            else:
                session.rollback()
            return RetryOrResumeBenchmarkResponse(status="success")

        if secrets or concurrency is not None or benchmark_url is not None:
            benchmark_row = update_benchmark_resume_arguments(
                benchmark_id,
                benchmark_repository,
                session,
                org,
                secrets=secrets,
                concurrency=concurrency,
                benchmark_url=benchmark_url,
            )

        resume_request = benchmark_row.start_benchmark_request(
            harness_config,
            service_headers=effective_service_headers,
        )
        dispatch_kind = ExecutorDispatchKind.RETRY if retry else ExecutorDispatchKind.RESUME
        executor_dispatch = admit_recovery_dispatch(
            session,
            benchmark=benchmark_row,
            pre_action_status=pre_action_status,
            dispatch_id=dispatch_id,
            kind=dispatch_kind,
            executor_control_repository=executor_control_repository,
        )
        session.commit()
    except ReleaseControlError as exc:
        session.rollback()
        status_code = (
            400
            if (phase_status and phase_status[0] == BenchmarkStatus.STOPPING)
            or benchmark_row.status == BenchmarkStatus.STOPPING
            else (
                503
                if isinstance(exc, MaintenanceModeError)
                else (409 if pre_action_status == BenchmarkStatus.IN_PROGRESS else 503)
            )
        )
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except Exception:
        session.rollback()
        raise

    session.rollback()
    await _enqueue_executor_dispatch(
        executor_dispatch,
        session_factory=lambda: Session(bind=session.bind, expire_on_commit=False),
        caller_session=session,
        start_benchmark_request_json=resume_request.model_dump(),
        verified_task_ids=verified_task_ids,
    )
    return RetryOrResumeBenchmarkResponse(status="success")


@app.get("/fetch-benchmarks")
async def fetch_benchmarks(
    reporting_repository: ReportingRepositoryDep,
    benchmark_repository: BenchmarkRepositoryDep,
    agent_name: list[str] | None = Query(default=None),
    benchmark_name: list[str] | None = Query(default=None),
    status: list[BenchmarkStatus] | None = Query(default=None),
    started_by: list[str] | None = Query(default=None),
    model: str | None = Query(default=None),
    dataset: str | None = Query(default=None),
    label: str | None = Query(default=None),
    started_after: datetime | None = Query(default=None),
    started_before: datetime | None = Query(default=None),
    order_by: Order = Query(default=Order.DESC),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
) -> FetchBenchmarksResponse:
    """
    Fetch benchmarks based on the request parameters.

    Usage:
    curl -X GET http://<endpoint>/fetch-benchmarks?agent_name=claude_code&benchmark_name=swebench&status=IN_PROGRESS&order_by=DESC&limit=5&offset=0
    """
    # Inline Query params (not `request: PydanticModel = Depends()`) so FastAPI doesn't
    # emit a requestBody on this GET — browsers reject GET-with-body.
    request = FetchBenchmarksRequest(
        agent_name=agent_name,
        benchmark_name=benchmark_name,
        status=status,
        started_by=started_by,
        model=model,
        dataset=dataset,
        label=label,
        started_after=started_after,
        started_before=started_before,
        order_by=order_by,
        cursor=cursor,
        limit=limit,
        offset=offset,
    )

    benchmark_rows, total_count, next_cursor = fetch_filtered_benchmark_rows(request, reporting_repository, org)

    return FetchBenchmarksResponse(
        benchmarks=build_benchmark_table_rows(benchmark_rows, benchmark_repository),
        total_count=total_count,
        next_cursor=next_cursor,
    )


@app.get("/fetch-benchmark-metadata/{benchmark_id}")
async def fetch_benchmark_metadata(
    benchmark_repository: BenchmarkRepositoryDep,
    benchmark_id: TrackedBenchmarkId,
    org: Org = Depends(get_current_org),
) -> FetchBenchmarkMetadataResponse:
    """
    Fetch benchmark metadata by its id.

    Usage:
    curl -X GET http://<endpoint>/fetch-benchmark-metadata/<benchmark_id>

    Returns:
        FetchBenchmarkMetadataResponse
    """
    benchmark_row = assert_org(benchmark_repository.get_for_org(benchmark_id, org.id), org)

    return benchmark_row.benchmark_metadata


def _safe_output_tar_member(s3_key: str, benchmark_prefix: str) -> str | None:
    """Return a safe relative tar member name for an object under a benchmark prefix."""
    if not s3_key.startswith(benchmark_prefix):
        return None

    relative_path = s3_key[len(benchmark_prefix) :]
    parts = relative_path.split("/")
    if (
        not relative_path
        or "\x00" in relative_path
        or "\\" in relative_path
        or any(part in {"", ".", ".."} for part in parts)
        or (len(parts[0]) >= 2 and parts[0][1] == ":")
    ):
        return None

    return relative_path


async def _output_keys(
    benchmark_prefix: str,
    task_ids: list[str] | None,
    harness_config: HarnessConfig,
    benchmark_id: TrackedBenchmarkId,
) -> AsyncIterator[str]:
    prefixes = [f"{benchmark_prefix}{task_id}/" for task_id in task_ids] if task_ids else [benchmark_prefix]
    for prefix in prefixes:
        async for key in list_s3_objects(prefix, harness_config.aws, harness_config.s3_bucket):
            if _safe_output_tar_member(key, benchmark_prefix) is None:
                logger.warning("Skipping unsafe output archive member for benchmark %s", benchmark_id)
                continue
            yield key


async def _output_keys_with_first(first_key: str, keys: AsyncIterator[str]) -> AsyncIterator[str]:
    yield first_key
    async for key in keys:
        yield key


async def _tar_output_stream(
    keys: AsyncIterator[str],
    benchmark_prefix: str,
    harness_config: HarnessConfig,
) -> AsyncIterator[bytes]:
    writer: YieldingWriter = YieldingWriter()

    # download_many_from_s3 reuses a single client/connection pool across all keys,
    # and reads one object into memory at a time (bounded by the largest object).
    with tarfile.open(fileobj=writer, mode="w|") as tar:
        async for s3_key, data in download_many_from_s3(keys, harness_config.aws, harness_config.s3_bucket):
            relative_path = s3_key.removeprefix(benchmark_prefix)
            tarinfo = tarfile.TarInfo(name=relative_path)
            tarinfo.size = len(data)
            tar.addfile(tarinfo, fileobj=io.BytesIO(data))

            chunk = writer.pop()
            if chunk:
                yield chunk

    final_chunk = writer.pop()
    if final_chunk:
        yield final_chunk


@app.get("/fetch-run-outputs/{benchmark_id}", response_model=None)
async def fetch_run_outputs(
    benchmark_id: TrackedBenchmarkId,
    session: Session = Depends(get_session),
    harness_config: HarnessConfig = Depends(fetch_harness_config),
    org: Org = Depends(get_current_org),
    task_ids: list[str] | None = Query(default=None),
) -> StreamingResponse:
    """
    Stream a tar file with run outputs to the client.

    Usage:
    curl -X GET http://<endpoint>/fetch-run-outputs/<benchmark_id>

    Returns:
        StreamingResponse
    """
    session.rollback()
    read_session = Session(bind=session.bind)
    try:
        transaction = TrackerTransaction.from_session(read_session)
        assert_org(transaction.benchmarks.get_for_org(benchmark_id, org.id), org)
    except BaseException:
        read_session.rollback()
        read_session.close()
        raise
    read_session.rollback()
    read_session.close()

    benchmark_prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"

    # Peek a single key so an empty result still returns 404 before the stream starts.
    keys = _output_keys(benchmark_prefix, task_ids, harness_config, benchmark_id)
    first_key = await anext(keys, None)
    if first_key is None:
        raise HTTPException(status_code=404, detail=f"No outputs found for run '{benchmark_id}'")

    return StreamingResponse(
        _tar_output_stream(_output_keys_with_first(first_key, keys), benchmark_prefix, harness_config),
        media_type="application/x-tar",
        headers={"Content-Disposition": f"attachment; filename=benchmark_{benchmark_id}_outputs.tar"},
    )
