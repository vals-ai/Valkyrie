import traceback
from uuid import UUID

from fastapi import Body, Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import joinedload
from sqlmodel import Session, col, func, select

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus
from tracker.database.session import check_database_connection, get_session
from tracker.exceptions import TrackerServiceError
from tracker.logger import get_logger
from tracker.s3 import get_contract_s3_key, upload_to_s3
from tracker.types import (
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    ResumeBenchmarkResponse,
    RetrieveResultsResponse,
    StartBenchmarkErrorResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
)
from tracker.utils import (
    BenchmarkContext,
    commit_benchmark_error,
    fetch_filtered_benchmark_rows,
    force_stop_sandboxes,
    initiate_resume_benchmark,
    initiate_stop_benchmark,
    process_benchmark,
    stream_benchmark_results,
)

logger = get_logger(__name__)

app = FastAPI()


@app.exception_handler(TrackerServiceError)
async def tracker_service_error_handler(_request: Request, exc: TrackerServiceError):
    logger.error(exc, exc_info=True)
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


@app.post("/upload")
async def upload_contract_to_s3(
    contract: UploadFile = File(..., description="Contract directory zip file"),
) -> dict[str, str]:
    """
    Upload contract to S3.

    Usage:
    curl -X POST http://<endpoint>/upload \
      -F "contract=@claude_code.zip"

    Returns:
    {
        "status": "success",
        "message": "Contract uploaded successfully"
    }

    Returns:
    - 200 OK if upload succeeds
    - 400 Bad Request if files are invalid
    - 500 Internal Server Error if upload fails
    """
    if not contract.filename or not contract.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Contract must be a zip file")

    contract_content = await contract.read()
    # Extract contract name from filename (remove .zip extension)
    contract_name = contract.filename.rsplit(".zip", 1)[0]
    contract_s3_key = get_contract_s3_key(contract_name)
    upload_to_s3(contract_content, contract_s3_key)

    return {
        "status": "success",
        "message": "Contract uploaded successfully",
    }


@app.post("/start-benchmark")
async def start_benchmark(
    request: StartBenchmarkRequest,
    session: Session = Depends(get_session),
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
    logger.info(f"Starting benchmark run - contract: {request.contract.name}, benchmark: {request.benchmark_name}")

    benchmark_service = request.benchmark_service

    # Check service is running
    _ = await benchmark_service.request_health_check()

    # Create benchmark row inside of database to mark start of the benchmark
    benchmark_row = BenchmarkService.start_benchmark_request_to_benchmark_object(request)
    session.add(benchmark_row)
    session.commit()

    # Verify task ids passed in (they exist within dataset and all dependencies are met to run them)
    try:
        verify_response = await benchmark_service.request_verify_task_ids(
            task_ids=request.task_ids, slice_str=request.slice_str
        )
    except Exception as e:
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        commit_benchmark_error(benchmark_row, session, error_message)
        error_response = StartBenchmarkErrorResponse(
            benchmark_id=benchmark_row.id,
            error_message=error_message,
        )

        raise TrackerServiceError(error_response.model_dump_json()) from e

    await process_benchmark.kiq(
        start_benchmark_request_json=request.model_dump(),
        benchmark_id_str=str(benchmark_row.id),
        verified_task_ids=verify_response.task_ids,
    )

    return StartBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        agent_name=request.contract.name,
        benchmark_id=benchmark_row.id,
        concurrency=request.concurrency,
        started_at=benchmark_row.started_at,
        task_count=len(verify_response.task_ids),
    )


@app.get("/fetch-benchmark", response_model=None)
async def fetch_benchmark(
    benchmark_id: UUID, connect: bool = Query(default=False), session: Session = Depends(get_session)
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
    benchmark_row = session.get(Benchmark, benchmark_id)
    if not benchmark_row:
        raise HTTPException(status_code=404, detail=f"Benchmark with id {benchmark_id} not found")

    # When we connect to the client every 60 seconds we send the latest benchmark status
    # and additional updates about the tasks completed
    if connect:
        return StreamingResponse(
            stream_benchmark_results(benchmark_id, session),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    benchmark_context = BenchmarkContext(benchmark_row, session)

    return FetchBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        benchmark_id=benchmark_row.id,
        details=benchmark_context.benchmark_details,
    )


@app.get("/retrieve-results")
async def retrieve_results(benchmark_id: UUID, session: Session = Depends(get_session)) -> RetrieveResultsResponse:
    """
    Retrieve the results of a benchmark by its id.

    Usage:
    curl -X GET http://<endpoint>/retrieve-results/<benchmark_id>

    Returns:
        RetrieveResultsResponse
    """
    statement = select(Benchmark).where(Benchmark.id == benchmark_id).options(joinedload(Benchmark.final_evaluation))
    benchmark_row = session.exec(statement).first()
    if not benchmark_row:
        raise HTTPException(status_code=404, detail=f"Benchmark with id {benchmark_id} not found")

    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    return RetrieveResultsResponse(
        benchmark_name=benchmark_row.name,
        status=benchmark_row.status,
        error_message=benchmark_row.error_message,
        benchmark_id=benchmark_row.id,
        benchmark_arguments=benchmark_row.arguments,
        tasks_stopped=tasks_stopped or None,  # NOTE: Only include if we stopped the benchmark
        final_evaluation=benchmark_row.final_evaluation,
        evaluation_results=benchmark_row.fetch_evaluation_results(session),
        task_errors=benchmark_row.fetch_tasks_with_errors(session),
    )


@app.post("/stop-benchmark/{benchmark_id}")
async def stop_benchmark(
    benchmark_id: UUID, force: bool = Query(default=False), session: Session = Depends(get_session)
) -> StopBenchmarkResponse:
    """
    Stop a benchmark by its id.
    If force is True, the sandboxes will be stopped and deleted, even if the tasks are in progress.

    Usage:
    curl -X POST http://<endpoint>/stop-benchmark/<benchmark_id>?force=true

    Returns:
        StopBenchmarkResponse
    """
    benchmark_row = session.get(Benchmark, benchmark_id)
    if not benchmark_row:
        raise HTTPException(status_code=404, detail=f"Benchmark with id {benchmark_id} not found")

    valid_stop_states = [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPING]

    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Benchmark {benchmark_id} is currently in the {benchmark_row.status} state. Can only pause an in progress or error benchmark.",
        )

    await initiate_stop_benchmark(benchmark_row, session, force)

    if force:
        await force_stop_sandboxes(benchmark_row, session)

    return StopBenchmarkResponse(
        status="success",
    )


@app.post("/resume-benchmark/{benchmark_id}")
async def resume_benchmark(
    benchmark_id: UUID,
    retry: bool = Query(default=False),
    force: list[str] = Body(default=[]),
    session: Session = Depends(get_session),
) -> ResumeBenchmarkResponse:
    """
    Resume a benchmark run by its id.

    Usage:
    curl -X POST http://<endpoint>/resume-benchmark/<benchmark_id>?retry=true
      -d '{"force": ["task_id_1", "task_id_2"]}'
    Returns:
        ResumeBenchmarkResponse
    """
    benchmark_row = session.get(Benchmark, benchmark_id)
    if not benchmark_row:
        raise HTTPException(status_code=404, detail=f"Benchmark with id {benchmark_id} not found")

    valid_resume_states = [BenchmarkStatus.STOPPED, BenchmarkStatus.ERROR]

    if benchmark_row.status not in valid_resume_states:
        raise HTTPException(
            status_code=400,
            detail=f"Benchmark {benchmark_id} is in the {benchmark_row.status} state. Must be in the stopped or error state to resume.",
        )

    start_benchmark_request = benchmark_row.start_benchmark_request

    benchmark_service = start_benchmark_request.benchmark_service

    # prepare benchmark and tasks to be resumed
    verified_task_ids = await initiate_resume_benchmark(benchmark_row, session, benchmark_service, retry, force)

    # start the benchmark with the same args used to create it
    # we will delegate inside what tasks we are running
    await process_benchmark.kiq(
        start_benchmark_request_json=start_benchmark_request.model_dump(),
        benchmark_id_str=str(benchmark_row.id),
        verified_task_ids=verified_task_ids,
    )

    return ResumeBenchmarkResponse(
        status="success",
    )


@app.get("/fetch-benchmarks")
async def fetch_benchmarks(
    request: FetchBenchmarksRequest = Depends(), session: Session = Depends(get_session)
) -> FetchBenchmarksResponse:
    """
    Fetch benchmarks based on the request parameters.

    Usage:
    curl -X GET http://<endpoint>/fetch-benchmarks?agent_name=claude_code&benchmark_name=swebench&status=IN_PROGRESS&order_by=DESC&limit=5&offset=0

    Returns:
        list[FetchBenchmarksResponse]
    """

    benchmark_rows, total_count = fetch_filtered_benchmark_rows(request, session)

    benchmark_table_rows: list[BenchmarkTableRow] = [
        benchmark_row.create_benchmark_table_row(session) for benchmark_row in benchmark_rows
    ]

    return FetchBenchmarksResponse(
        benchmarks=benchmark_table_rows,
        total_count=total_count,
    )
