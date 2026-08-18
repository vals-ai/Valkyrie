import asyncio
import io
import logging
import tarfile
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, cast
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
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import joinedload
from sqlmodel import Session, col, select

from tracker.api.agents import router as agents_router
from tracker.api.benchmark_services import router as benchmark_services_router
from tracker.api.benchmarks_status import router as benchmarks_status_router
from tracker.api.dependencies import RunAWSDependency
from tracker.api.filter_options import router as filter_options_router
from tracker.api.single_benchmark import router as single_benchmark_router
from tracker.api.single_task import router as single_task_router
from tracker.auth import (
    RequestIdentity,
    extract_api_key,
    find_org_by_tenant,
    forward_tracker_api_key,
    get_current_org,
    get_current_starter,
    resolve_descope_identity,
)
from tracker.aws.cloudwatch_logs import get_benchmark_log_url
from tracker.aws.resolver import (
    resolve_aws_runtime_metadata,
    resolve_run_aws_runtime,
    resolve_run_aws_runtime_and_access_key_config,
    resolve_start_aws_runtime,
)
from tracker.aws.runtime import AWSRuntime
from tracker.aws.secrets import resolve_secrets
from tracker.agent.contract import get_contract_from_zip_bytes
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    S3ObjectCopy,
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
from tracker.config import (
    AUTH_REQUIRED,
    ENVIRONMENT,
    broker,
    classify_benchmark_service_destination,
    create_benchmark_service_url,
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
    Task,
)
from tracker.database.scoping import assert_org, get_scoped
from tracker.executor.dispatch_control import (
    EnqueueFailureResolution,
    admit_recovery_dispatch,
    admit_start_dispatch,
    resolve_enqueue_failure,
    validate_managed_execution_release,
)
from tracker.database.session import check_database_connection, get_session
from tracker.docent_analysis import (
    analyze_event_stream,
)
from tracker.exceptions import TrackerServiceError
from executor_protocol import EXECUTOR_TASK_NAME, executor_task_signature
from tracker.logging import benchmark_id_var, configure_logging, get_logger, request_id_var
from tracker.executor.release_control import MaintenanceModeError, ReleaseControlError, lock_executor_admission
from tracker.executor.release_retirement import AutomaticReleaseRetirement
from tracker.middleware import RequestContextMiddleware
from tracker.observability import configure_observability
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax
from tracker.types import (
    AnalyzeBenchmarkRequest,
    AWSRuntimeResponse,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FetchBenchmarkTasksRequest,
    HarnessConfig,
    ManagedExecutionContext,
    Order,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
    UpdateBenchmarkConcurrencyRequest,
    UpdateBenchmarkConcurrencyResponse,
    validate_managed_execution_request,
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
    fetch_final_score_inputs,
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


def _process_benchmark_kwargs(
    benchmark_row: Benchmark,
    request: StartBenchmarkRequest,
    verified_task_ids: list[str],
) -> dict[str, Any]:
    if benchmark_row.aws_managed:
        return {
            "execution_context_json": ManagedExecutionContext(
                version=2,
                benchmark_id=benchmark_row.id,
                verified_task_ids=verified_task_ids,
                start_benchmark_request=request,
            ).model_dump(mode="json")
        }
    return {
        "start_benchmark_request_json": request.model_dump(),
        "benchmark_id_str": str(benchmark_row.id),
        "verified_task_ids": verified_task_ids,
    }


async def _enqueue_executor_dispatch(
    dispatch: ExecutorDispatch,
    *,
    session: Session,
    payload: dict[str, Any],
    verified_task_ids: list[str],
) -> None:
    for attempt in range(3):
        try:
            await (
                process_benchmark.kicker()
                .with_labels(**_taskiq_labels())
                .kiq(
                    **payload,
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
            resolution = resolve_enqueue_failure(
                session,
                benchmark_id=dispatch.benchmark_id,
                dispatch_id=dispatch.id,
                task_ids=verified_task_ids,
            )
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
    created_copy: S3ObjectCopy | None,
    benchmark_id: UUID,
    request: StartBenchmarkRequest,
    aws_runtime: AWSRuntime,
) -> None:
    if created_copy is None:
        return
    try:
        await delete_from_s3(
            get_benchmark_contract_s3_key(str(benchmark_id), request.contract.name),
            aws_runtime,
            version_id=created_copy.version_id,
        )
    except Exception:
        logger.exception(
            "Failed to delete uncommitted benchmark agent copy",
            extra={"benchmark_id": str(benchmark_id)},
        )


def _start_admission_is_absent(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
) -> bool:
    """Return whether a fresh database read proves the start admission was not committed."""
    try:
        with Session(bind=session.get_bind()) as verification_session:
            benchmark = verification_session.get(Benchmark, benchmark_id)
            dispatch = verification_session.get(ExecutorDispatch, dispatch_id)
    except Exception:
        logger.exception(
            "Could not verify failed benchmark admission; retaining its agent copy",
            extra={"benchmark_id": str(benchmark_id), "executor_dispatch_id": str(dispatch_id)},
        )
        return False
    return benchmark is None and dispatch is None


async def _rollback_failed_start_admission(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
    created_copy: S3ObjectCopy | None,
    request: StartBenchmarkRequest,
    aws_runtime: AWSRuntime,
) -> None:
    try:
        session.rollback()
    except Exception:
        logger.exception(
            "Failed to roll back benchmark admission",
            extra={"benchmark_id": str(benchmark_id), "executor_dispatch_id": str(dispatch_id)},
        )
        return
    if not _start_admission_is_absent(
        session,
        benchmark_id=benchmark_id,
        dispatch_id=dispatch_id,
    ):
        return
    await _delete_uncommitted_agent_copy(
        created_copy=created_copy,
        benchmark_id=benchmark_id,
        request=request,
        aws_runtime=aws_runtime,
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


@app.get("/aws-runtime", response_model=AWSRuntimeResponse)
def fetch_aws_runtime_metadata(org: Org = Depends(get_current_org)) -> AWSRuntimeResponse:
    """Return managed AWS resource locations without credential material."""
    resources = resolve_aws_runtime_metadata(org.id)
    if resources is None:
        return AWSRuntimeResponse(mode="access_key")
    return AWSRuntimeResponse(
        mode="managed",
        region=resources.region,
        s3_bucket=resources.s3_bucket,
    )


@app.post("/init")
def init_org(
    request: Request,
    session: Session = Depends(get_session),
) -> dict[str, str | bool]:
    """Initialize org for hosted mode. Validates Descope key and creates org if needed."""
    if not AUTH_REQUIRED:
        raise HTTPException(status_code=405, detail="Init is only available in hosted mode")

    api_key = extract_api_key(request)
    identity = resolve_descope_identity(api_key, include_user_profile=True)

    stmt = pg_insert(Org).values(name=identity.tenant_name).on_conflict_do_nothing(index_elements=["name"])
    result = session.exec(stmt)
    created = result.rowcount > 0
    session.commit()

    org = find_org_by_tenant(identity.tenant_name, session)
    if not org:
        raise HTTPException(status_code=500, detail="Internal error during org creation")

    return {"org_name": org.name, "created": created, "email_claim_missing": identity.email is None}


async def _resolve_contract_from_s3(request: StartBenchmarkRequest, aws_runtime: AWSRuntime) -> AgentContractRequest:
    """Resolve install_cmd/run_cmd/etc by parsing the agent's contract file inside its S3 zip."""
    zip_bytes = await download_from_s3(
        get_contract_s3_key(request.contract.name),
        aws_runtime,
    )
    agent_config = AgentConfig(model=request.contract.model, kwargs=dict(request.contract.kwargs))
    resolved = get_contract_from_zip_bytes(request.contract.name, zip_bytes, agent_config)
    if request.contract.secrets:
        resolved.secrets = {**resolved.secrets, **request.contract.secrets}
    return resolved


def _authorize_custom_benchmark_destination(url: str, org: Org) -> None:
    try:
        validate_custom_service_destination(
            url,
            org_name=org.name,
            auth_required=AUTH_REQUIRED,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@app.post("/start-benchmark")
async def start_benchmark(
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
    if request.custom_benchmark_service is not None:
        _authorize_custom_benchmark_destination(request.custom_benchmark_service, run_starter.org)

    runtime_resolution = resolve_start_aws_runtime(http_request, request.harness_config, run_starter.org.id)
    aws_runtime = runtime_resolution.runtime
    effective_harness_config = runtime_resolution.access_key_harness_config
    aws_managed = runtime_resolution.aws_managed

    if aws_managed:
        if not request.sandbox_provider or not request.sandbox_provider_secret_name:
            raise HTTPException(
                status_code=400,
                detail="Managed runs require a sandbox provider and sandbox provider secret name.",
            )
        try:
            validate_managed_execution_request(request)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            validate_managed_execution_release(session)
        except ReleaseControlError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    else:
        effective_harness_config = cast(HarnessConfig, effective_harness_config)
        body_provider_secret_name = (
            request.harness_config.sandbox_provider_secret_name if request.harness_config is not None else None
        )
        provider_secret_name = (
            body_provider_secret_name
            or request.sandbox_provider_secret_name
            or effective_harness_config.sandbox_provider_secret_name
        )
        if provider_secret_name:
            effective_harness_config = effective_harness_config.model_copy(
                update={"sandbox_provider_secret_name": provider_secret_name}
            )

    service_headers = dict(request.service_headers)
    if request.service_auth_header_name and request.service_auth_secret_name:
        resolved = resolve_secrets(
            {request.service_auth_header_name: request.service_auth_secret_name},
            aws_runtime.clients,
        )
        service_headers.update(resolved)

    request = request.model_copy(
        update={
            "harness_config": effective_harness_config,
            "service_headers": forward_tracker_api_key(
                service_headers,
                http_request.headers.get("x-api-key"),
                destination=classify_benchmark_service_destination(
                    request.benchmark_name,
                    request.custom_benchmark_service,
                ),
            ),
        }
    )

    if aws_managed:
        try:
            validate_managed_execution_request(request)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not request.contract.install_cmd and not request.contract.run_cmd:
        request = request.model_copy(update={"contract": await _resolve_contract_from_s3(request, aws_runtime)})
        if aws_managed:
            try:
                validate_managed_execution_request(request)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
    elif aws_managed and not await s3_object_exists(get_contract_s3_key(request.contract.name), aws_runtime):
        raise HTTPException(
            status_code=404,
            detail=f"Agent '{request.contract.name}' is not available in the deployment bucket.",
        )

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

    benchmark_row = start_benchmark_request_to_benchmark(
        request,
        run_starter,
        aws_managed=aws_managed,
    )
    dispatch_id = uuid4()
    created_agent_copy: S3ObjectCopy | None = None
    try:
        created_agent_copy = await copy_agent_to_benchmark(
            str(benchmark_row.id),
            request.contract.name,
            aws_runtime,
        )
        for task_id in verify_response.task_ids:
            session.add(Task(org_id=benchmark_row.org_id, benchmark=benchmark_row.id, task_id=task_id))
        executor_dispatch = admit_start_dispatch(
            session,
            benchmark=benchmark_row,
            dispatch_id=dispatch_id,
        )
        executor_payload = _process_benchmark_kwargs(benchmark_row, request, verify_response.task_ids)
        session.commit()
    except Exception as exc:
        await _rollback_failed_start_admission(
            session,
            benchmark_id=benchmark_row.id,
            dispatch_id=dispatch_id,
            created_copy=created_agent_copy,
            request=request,
            aws_runtime=aws_runtime,
        )
        if isinstance(exc, ReleaseControlError):
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        raise TrackerServiceError("Failed to admit benchmark execution") from exc

    benchmark_id_var.set(str(benchmark_row.id))

    if run_starter.access_key_id is not None and run_starter.email is None:
        logger.warning(
            "Access key %s resolved no user email; run attribution for this run will be empty",
            run_starter.access_key_id,
        )

    await _enqueue_executor_dispatch(
        executor_dispatch,
        session=session,
        payload=executor_payload,
        verified_task_ids=verify_response.task_ids,
    )

    return StartBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        agent_name=request.contract.name,
        benchmark_id=benchmark_row.id,
        concurrency=request.concurrency,
        started_at=benchmark_row.started_at,
        task_count=len(verify_response.task_ids),
        cloudwatch_url=get_benchmark_log_url(str(benchmark_row.id), aws_runtime.resources),
        s3_bucket_url=create_benchmark_url(str(benchmark_row.id), aws_runtime.resources),
        executor_release_id=benchmark_row.executor_release_id,
        current_execution_release_id=benchmark_row.current_execution_release_id,
        executor_artifact_digest=benchmark_row.executor_artifact_digest,
        executor_protocol_version=benchmark_row.executor_protocol_version,
    )


@app.post("/fetch-benchmark-tasks")
async def fetch_benchmark_tasks(
    http_request: Request,
    request: FetchBenchmarkTasksRequest,
    org: Org = Depends(get_current_org),
) -> VerifyTaskIdsResponse:
    """
    Fetch all task ids for a benchmark dataset.
    """
    if request.custom_benchmark_service is not None:
        _authorize_custom_benchmark_destination(request.custom_benchmark_service, org)

    try:
        benchmark_service = create_benchmark_service_client(
            url=request.custom_benchmark_service or create_benchmark_service_url(request.benchmark_name),
            service_headers=forward_tracker_api_key(
                request.service_headers,
                http_request.headers.get("x-api-key"),
                destination=classify_benchmark_service_destination(
                    request.benchmark_name,
                    request.custom_benchmark_service,
                ),
            ),
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
    run_context: RunAWSDependency,
    connect: bool = Query(default=False),
    session: Session = Depends(get_session),
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
    benchmark_row = run_context.benchmark
    aws_runtime = run_context.aws_runtime

    # When we connect to the client every 60 seconds we send the latest benchmark status
    # and additional updates about the tasks completed
    if connect:
        return StreamingResponse(
            stream_benchmark_results(benchmark_id, session, aws_runtime, org),
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
        s3_bucket_url=create_benchmark_url(str(benchmark_row.id), aws_runtime.resources),
        label=benchmark_row.label,
        final_score=benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None,
        error_message=benchmark_row.error_message if benchmark_row.status == BenchmarkStatus.ERROR else None,
        executor_release_id=benchmark_row.executor_release_id,
        current_execution_release_id=benchmark_row.current_execution_release_id,
        executor_artifact_digest=benchmark_row.executor_artifact_digest,
        executor_protocol_version=benchmark_row.executor_protocol_version,
    )


@app.post("/analyze-benchmark/{benchmark_id}", response_model=None)
async def analyze_benchmark(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunAWSDependency,
    body: AnalyzeBenchmarkRequest,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> dict[str, str] | StreamingResponse:
    """
    Invoke the Docent analyzer Lambda for a benchmark and emit SSE-formatted progress events
    (started/heartbeat/done/error) that clients consume as buffered text (the response is read
    in full; the terminal done/error event carries the result).

    Cache short-circuit: when the benchmark already has docent_reading_status=DONE and
    no_cache=false, returns the existing reading_plan_url without invoking the Lambda.
    """
    benchmark_row = run_context.benchmark
    aws_runtime = run_context.aws_runtime

    if benchmark_row.status != BenchmarkStatus.FINISHED:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot analyze run {benchmark_id}: status is {benchmark_row.status.value} (must be FINISHED).",
        )

    if (
        not body.no_cache
        and benchmark_row.docent_reading_status == DocentReadingStatus.DONE
        and benchmark_row.docent_reading_url
    ):
        return {
            "status": "done",
            "reading_plan_url": benchmark_row.docent_reading_url,
        }

    if not body.lambda_function:
        raise HTTPException(
            status_code=400,
            detail=(
                f"No ingest_lambda provided for agent '{benchmark_row.arguments.contract.name}'. "
                "The CLI normally resolves this from the agent's pushed contract — if you're "
                "calling this endpoint directly, supply `lambda_function` in the request body."
            ),
        )

    payload: dict[str, Any] = {
        "benchmark_id": str(benchmark_id),
        "benchmark_name": benchmark_row.name,
        "s3_bucket": aws_runtime.resources.s3_bucket,
        "contract": {"name": benchmark_row.arguments.contract.name},
    }

    return StreamingResponse(
        analyze_event_stream(
            benchmark_id=benchmark_row.id,
            lambda_function=body.lambda_function,
            payload=payload,
            clients=aws_runtime.clients,
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
    session: Session = Depends(get_session),
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
    benchmark_row = assert_org(
        session.get(Benchmark, benchmark_id, options=[joinedload(Benchmark.final_evaluation)]),
        org,
    )
    aws_runtime = resolve_run_aws_runtime(
        http_request,
        aws_managed=benchmark_row.aws_managed,
        org_id=org.id,
    )

    final_view = create_final_view(benchmark_row, session, org)

    if task_ids:
        task_ids_set = set(task_ids)

        def _filter_task_map(task_map: dict[str, Any] | None) -> dict[str, Any] | None:
            return {task_id: value for task_id, value in (task_map or {}).items() if task_id in task_ids_set} or None

        final_view.evaluation_results = _filter_task_map(final_view.evaluation_results)
        final_view.task_errors = _filter_task_map(final_view.task_errors)

        # Include every requested task with its result or None, so tasks without a result
        # (e.g. stopped/errored) still count toward the denominator instead of being dropped.
        scored_results = {
            task_id: result
            for task_id, result in fetch_final_score_inputs(session, benchmark_row, org).items()
            if task_id in task_ids_set
        }

        if benchmark_row.custom_benchmark_service is not None:
            _authorize_custom_benchmark_destination(benchmark_row.custom_benchmark_service, org)

        effective_service_headers = forward_tracker_api_key(
            None,
            http_request.headers.get("x-api-key"),
            destination=classify_benchmark_service_destination(
                benchmark_row.name,
                benchmark_row.custom_benchmark_service,
            ),
        )
        benchmark_service = benchmark_row.benchmark_service(service_headers=effective_service_headers)
        try:
            resp = await benchmark_service.final_score(
                evaluation_results=scored_results,
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
        s3_key = await upload_final_view(benchmark_row, final_view, aws_runtime)

        https_url = f"s3://{aws_runtime.resources.s3_bucket}/{s3_key}"
        expires_in = aws_runtime.clients.maximum_presign_ttl(86400)
        presigned_url = await create_presigned_url(s3_key, aws_runtime, expiration=expires_in)
        console_url = create_console_url(s3_key, aws_runtime.resources)

        return S3UploadResultsResponse(
            s3_url=https_url,
            presigned_url=presigned_url,
            console_url=console_url,
            expires_in=expires_in,
        )

    return final_view


@app.get("/check-results-exist")
async def check_results_exist(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunAWSDependency,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> dict[str, bool]:
    """
    Check if the benchmark's final view already exists in S3.

    Usage:
    curl -X GET http://<endpoint>/check-results-exist?benchmark_id=<uuid>

    Returns:
        {"exists": true/false}
    """
    benchmark_row = run_context.benchmark
    aws_runtime = run_context.aws_runtime

    s3_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{benchmark_row.name}.json"
    exists = await s3_object_exists(s3_key, aws_runtime)
    return {"exists": exists}


async def validate_tasks_exist(
    benchmark_row: Benchmark,
    task_ids: list[str],
    session: Session,
    org: Org,
) -> list[str]:
    """Validate that selected tasks belong to the run."""
    if not task_ids:
        raise HTTPException(status_code=400, detail="task_ids cannot be empty")

    requested_task_ids = list(dict.fromkeys(task_ids))
    existing_task_ids = set(
        session.exec(
            select(Task.task_id)
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.task_id).in_(requested_task_ids))
        ).all()
    )
    missing_task_ids = [task_id for task_id in requested_task_ids if task_id not in existing_task_ids]
    if missing_task_ids:
        raise HTTPException(
            status_code=400,
            detail=f"Task IDs are not part of run {benchmark_row.id}: {', '.join(missing_task_ids)}",
        )

    return requested_task_ids


def _resolve_force_stop_provider_secret_name(
    benchmark_row: Benchmark,
    access_key_harness_config: HarnessConfig | None,
) -> str:
    """Return the persisted or access-key provider secret name for force-stop."""
    provider_secret_name = benchmark_row.arguments.sandbox_provider_secret_name
    if not provider_secret_name and access_key_harness_config is not None:
        provider_secret_name = access_key_harness_config.sandbox_provider_secret_name
    if provider_secret_name:
        return provider_secret_name

    detail = "The run does not have a sandbox provider secret name."
    if access_key_harness_config is not None:
        detail += " Provide the x-harness-sandbox-provider-secret-name header and retry."
    raise HTTPException(status_code=400, detail=detail)


@app.post("/stop-benchmark/{benchmark_id}")
async def stop_benchmark(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    force: bool = Query(default=False),
    task_ids: list[str] | None = Body(default=None, embed=True),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> StopBenchmarkResponse:
    """
    Stop a benchmark by its id.
    If force is True, the database run is stopped immediately and provider kill signals are sent
    without waiting for provider teardown to complete.

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

    selected_task_ids = (
        await validate_tasks_exist(benchmark_row, task_ids, session, org) if task_ids is not None else None
    )

    runtime_resolution = resolve_run_aws_runtime_and_access_key_config(
        http_request,
        aws_managed=benchmark_row.aws_managed,
        org_id=org.id,
    )

    provider_secret_name = (
        _resolve_force_stop_provider_secret_name(
            benchmark_row,
            runtime_resolution.access_key_harness_config,
        )
        if force
        else None
    )

    benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status} state. Can only pause an in progress or error run.",
        )

    await initiate_stop_benchmark(benchmark_row, session, force, org, task_ids=selected_task_ids)

    if provider_secret_name is not None:
        await force_stop_sandboxes(
            benchmark_row,
            provider_secret_name,
            runtime_resolution.runtime,
            org,
            sandbox_provider=benchmark_row.arguments.sandbox_provider,
            task_ids=selected_task_ids,
        )

    return StopBenchmarkResponse(
        status="success",
    )


def _update_benchmark_concurrency(
    benchmark_id: UUID,
    concurrency: int,
    session: Session,
    org: Org,
) -> BenchmarkConcurrencyUpdate:
    try:
        with session.no_autoflush:
            lock_executor_admission(session)
        benchmark_row = update_benchmark_concurrency(benchmark_id, concurrency, session, org)
    except MaintenanceModeError as exc:
        session.rollback()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Not found") from exc

    if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
        raise HTTPException(
            status_code=409,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status} state.",
        )
    return benchmark_row


@app.patch("/benchmarks/{benchmark_id}/concurrency")
def patch_benchmark_concurrency(
    benchmark_id: TrackedBenchmarkId,
    request: UpdateBenchmarkConcurrencyRequest,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> UpdateBenchmarkConcurrencyResponse:
    benchmark_row = _update_benchmark_concurrency(benchmark_id, request.concurrency, session, org)
    return UpdateBenchmarkConcurrencyResponse(
        benchmark_id=benchmark_row.benchmark_id,
        status=benchmark_row.status,
        concurrency=benchmark_row.concurrency,
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
    secrets: dict[str, str] = Body(default={}),
    benchmark_url: str | None = Body(default=None),
    session: Session = Depends(get_session),
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
    benchmark_row = get_scoped(Benchmark, benchmark_id, session, org)
    runtime_resolution = resolve_run_aws_runtime_and_access_key_config(
        http_request,
        aws_managed=benchmark_row.aws_managed,
        org_id=org.id,
    )

    if benchmark_row.status == BenchmarkStatus.STOPPING:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is in the {benchmark_row.status} state. Cannot continue a run that is stopping.",
        )

    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and not retry and secrets:
        raise HTTPException(
            status_code=409,
            detail="Secret overrides require retry=true while a run is in progress.",
        )

    if benchmark_url is not None:
        try:
            benchmark_url = validate_service_url_syntax(benchmark_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    effective_benchmark_url = benchmark_url if benchmark_url is not None else benchmark_row.custom_benchmark_service
    if effective_benchmark_url is not None:
        _authorize_custom_benchmark_destination(effective_benchmark_url, org)

    if concurrency is not None and concurrency < 1:
        raise HTTPException(status_code=400, detail="Concurrency must be greater than 0.")

    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and not retry:
        if concurrency is not None:
            _update_benchmark_concurrency(benchmark_id, concurrency, session, org)
        if secrets or benchmark_url is not None:
            update_benchmark_resume_arguments(
                benchmark_id,
                session,
                org,
                secrets=secrets,
                concurrency=None,
                benchmark_url=benchmark_url,
            )
            session.commit()
        return RetryOrResumeBenchmarkResponse(status="success")

    dispatch_id = uuid4()
    pre_action_status: BenchmarkStatus | None = None
    try:
        with session.no_autoflush:
            lock_executor_admission(session)
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        effective_benchmark_url = benchmark_url if benchmark_url is not None else benchmark_row.custom_benchmark_service
        if effective_benchmark_url is not None:
            _authorize_custom_benchmark_destination(effective_benchmark_url, org)
        effective_service_headers = forward_tracker_api_key(
            service_headers,
            http_request.headers.get("x-api-key"),
            destination=classify_benchmark_service_destination(
                benchmark_row.name,
                effective_benchmark_url,
            ),
        )
        if benchmark_row.status == BenchmarkStatus.STOPPING:
            raise HTTPException(
                status_code=400,
                detail=f"Run {benchmark_id} is in the {benchmark_row.status} state. Cannot continue a run that is stopping.",
            )
        pre_action_status = benchmark_row.status

        if benchmark_row.aws_managed:
            prospective_request = benchmark_row.managed_start_benchmark_request(
                service_headers=effective_service_headers,
            )
            if secrets:
                prospective_contract = prospective_request.contract.model_copy(
                    update={"secrets": {**prospective_request.contract.secrets, **secrets}}
                )
                prospective_request = prospective_request.model_copy(update={"contract": prospective_contract})
            try:
                validate_managed_execution_request(prospective_request)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        verified_task_ids = await reset_to_in_progress_status(
            benchmark_row=benchmark_row,
            session=session,
            benchmark_service=benchmark_row.benchmark_service(
                service_headers=effective_service_headers,
                benchmark_url=benchmark_url,
            ),
            retry=retry,
            retry_mode=retry_mode,
            rerun_task_ids=task_ids,
            org=org,
        )

        if pre_action_status == BenchmarkStatus.IN_PROGRESS and not verified_task_ids:
            if secrets or concurrency is not None or benchmark_url is not None:
                update_benchmark_resume_arguments(
                    benchmark_id,
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
                session,
                org,
                secrets=secrets,
                concurrency=concurrency,
                benchmark_url=benchmark_url,
            )

        if benchmark_row.aws_managed:
            resume_request = benchmark_row.managed_start_benchmark_request(
                service_headers=effective_service_headers,
            )
        else:
            access_key_harness_config = cast(HarnessConfig, runtime_resolution.access_key_harness_config)
            resume_request = benchmark_row.access_key_start_benchmark_request(
                access_key_harness_config,
                service_headers=effective_service_headers,
            )
        dispatch_kind = ExecutorDispatchKind.RETRY if retry else ExecutorDispatchKind.RESUME
        executor_dispatch = admit_recovery_dispatch(
            session,
            benchmark=benchmark_row,
            pre_action_status=pre_action_status,
            dispatch_id=dispatch_id,
            kind=dispatch_kind,
        )
        executor_payload = _process_benchmark_kwargs(benchmark_row, resume_request, verified_task_ids)
        session.commit()
    except ReleaseControlError as exc:
        session.rollback()
        status_code = (
            503
            if isinstance(exc, MaintenanceModeError)
            else (409 if pre_action_status == BenchmarkStatus.IN_PROGRESS else 503)
        )
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except Exception:
        session.rollback()
        raise

    await _enqueue_executor_dispatch(
        executor_dispatch,
        session=session,
        payload=executor_payload,
        verified_task_ids=verified_task_ids,
    )
    return RetryOrResumeBenchmarkResponse(status="success")


@app.get("/fetch-benchmarks")
async def fetch_benchmarks(
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
    session: Session = Depends(get_session),
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

    benchmark_rows, total_count, next_cursor = fetch_filtered_benchmark_rows(request, session, org)

    return FetchBenchmarksResponse(
        benchmarks=build_benchmark_table_rows(benchmark_rows, session),
        total_count=total_count,
        next_cursor=next_cursor,
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
    aws_runtime: AWSRuntime,
    benchmark_id: TrackedBenchmarkId,
) -> AsyncIterator[str]:
    prefixes = [f"{benchmark_prefix}{task_id}/" for task_id in task_ids] if task_ids else [benchmark_prefix]
    for prefix in prefixes:
        async for key in list_s3_objects(prefix, aws_runtime):
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
    aws_runtime: AWSRuntime,
) -> AsyncIterator[bytes]:
    writer: YieldingWriter = YieldingWriter()

    # download_many_from_s3 reuses a single client/connection pool across all keys,
    # and reads one object into memory at a time (bounded by the largest object).
    with tarfile.open(fileobj=writer, mode="w|") as tar:
        async for s3_key, data in download_many_from_s3(keys, aws_runtime):
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
    run_context: RunAWSDependency,
    session: Session = Depends(get_session),
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
    aws_runtime = run_context.aws_runtime

    benchmark_prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"

    # Peek a single key so an empty result still returns 404 before the stream starts.
    keys = _output_keys(benchmark_prefix, task_ids, aws_runtime, benchmark_id)
    first_key = await anext(keys, None)
    if first_key is None:
        raise HTTPException(status_code=404, detail=f"No outputs found for run '{benchmark_id}'")

    return StreamingResponse(
        _tar_output_stream(_output_keys_with_first(first_key, keys), benchmark_prefix, aws_runtime),
        media_type="application/x-tar",
        headers={"Content-Disposition": f"attachment; filename=benchmark_{benchmark_id}_outputs.tar"},
    )
