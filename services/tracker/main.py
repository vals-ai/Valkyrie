from asyncio import Semaphore, gather
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, EvaluationResult, FinalEvaluation, Task, TaskStatus
from tracker.database.session import engine, get_session
from tracker.exceptions import TrackerServiceError
from tracker.logger import get_logger
from tracker.s3 import get_contract_s3_key, upload_to_s3
from tracker.sandbox import create_sandbox, install_dependencies, run_agent, upload_contract_to_sandbox
from tracker.types import StartRunRequest, StartRunResponse

logger = get_logger(__name__)

app = FastAPI()


@app.exception_handler(TrackerServiceError)
async def tracker_service_error_handler(_request: Request, exc: TrackerServiceError):
    logger.error(exc, exc_info=True)
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health_check():
    """
    Health check to ensure that the tracker service is running.

    Usage:
    curl -X GET http://<endpoint>/health

    Returns:
    {
        "status": "ok"
    }

    Returns:
    - 200 OK if the server is running
    - 500 Internal Server Error if the server is not running
    """
    return {"status": "ok"}


@app.post("/upload")
async def upload_contract_to_s3(
    contract: UploadFile = File(..., description="Contract directory zip file"),
):
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


@app.post("/start-run")
async def start_run(request: StartRunRequest, session: Session = Depends(get_session)):
    """
    Start a benchmark run with the uploaded contract.

    Usage:
    curl -X POST http://<endpoint>/start-run \
      -H "Content-Type: application/json" \
      -d '{"contract_name": "claude_code", "benchmark_name": "swebench", "task_ids": ["astropy__astropy-12907"]}'

    Returns:
        StartRunResponse

    Returns:
    - 200 OK if run starts successfully
    - 400 Bad Request if parameters are invalid
    - 500 Internal Server Error if run fails to start
    """
    logger.info(f"Starting benchmark run - contract: {request.contract_name}, benchmark: {request.benchmark_name}")

    benchmark_service = request.benchmark_service

    # Check service is running
    _ = await benchmark_service.request_health_check()

    # Create benchmark row inside of database to mark start of the benchmark
    benchmark_row = Benchmark(name=benchmark_service.name)
    session.add(benchmark_row)
    session.commit()

    # Verify task ids passed in (they exist within dataset and all dependencies are met to run them)
    verified_task_ids = await benchmark_service.request_verify_task_ids(task_ids=request.task_ids)

    # Create tasks inside of the database for each task id
    task_row_mapping: dict[str, Task] = {}
    for task_id in verified_task_ids:
        task_row = Task(task_id=task_id, benchmark_id=cast(UUID, benchmark_row.id))
        task_row_mapping[task_id] = task_row

    session.add_all(list(task_row_mapping.values()))
    session.commit()

    semaphore = Semaphore(request.concurrency)

    async def process_task(task_id: str) -> EvaluationResult:
        async with semaphore:
            # NOTE: This endpoint was made for retrieving task info for a group of tasks
            # Turns out its a better design to retrieve a single task at a time so that it fits better with a semaphore
            task_data = (await benchmark_service.request_retrieve_tasks(task_ids=[task_id]))[task_id]

            with Session(bind=engine) as task_session:
                task_row = task_row_mapping[task_id]
                # Update the task status to in progress
                task_row.status = TaskStatus.IN_PROGRESS
                task_session.add(task_row)
                task_session.commit()

                async with create_sandbox(
                    benchmark_service.daytona_client, task_row.task_id, task_data["docker_image"]
                ) as sandbox:
                    # Upload the contract to the sandbox after creating and install the dependencies
                    await upload_contract_to_sandbox(sandbox, request.contract_name)
                    await install_dependencies(sandbox, request.contract_name)

                    # Setup task if requested
                    if task_data["request_setup"]:
                        _ = await benchmark_service.request_setup_task(task_row.task_id, sandbox.id)

                    # Run the agent inside of the sandbox
                    # NOTE: Currently only testing when agent does not need a response, in the future run agent will return a json to evaluate it needed
                    await run_agent(sandbox, request.contract_name, task_row.task_id, task_data["problem_statement"])

                    # Update the status to evaluating once we finish running the agent
                    task_row.status = TaskStatus.EVALUATING
                    task_session.add(task_row)
                    task_session.commit()

                    # Evaluate the instance
                    # NOTE: only really good for when we need to evaluate the container (for just evaluating a text response we can delegate before this)
                    evaluation_result = await benchmark_service.request_evaluate_instance(task_row.task_id, sandbox.id)

                    # Save the evaluation result to the database with the task row
                    evaluation_result_row = EvaluationResult(
                        task_id=cast(UUID, task_row.id), instance_id=sandbox.id, result=evaluation_result
                    )
                    task_session.add(evaluation_result_row)

                    # Mark the task status as finished since we have finished processing the task
                    task_row.status = TaskStatus.FINISHED
                    task_session.add(task_row)
                    task_session.commit()

                    return evaluation_result_row

    evaluation_result_rows: list[EvaluationResult] = await gather(
        *[process_task(task_id) for task_id in verified_task_ids]
    )

    evaluation_results: dict[str, dict[str, Any]] = {
        str(evaluation_result_row.task_id): evaluation_result_row.result
        for evaluation_result_row in evaluation_result_rows
    }

    # Calculate the final score based off the tasks that were ran
    final_score: dict[str, Any] = await benchmark_service.request_final_score(evaluation_results=evaluation_results)

    # Create the final evaluation row and add it to the database
    final_evaluation_row = FinalEvaluation(
        benchmark_id=cast(UUID, benchmark_row.id),
        final_score=final_score["final_score"],
        resolved_tasks=final_score["resolved_tasks"],
        unresolved_tasks=final_score["unresolved_tasks"],
    )

    session.add(final_evaluation_row)
    session.commit()

    # Mark benchmark as completed
    # NOTE: Finished at will be automatically set by an event when the status becomes finished
    benchmark_row.status = BenchmarkStatus.FINISHED
    session.add(benchmark_row)
    session.commit()

    return StartRunResponse(
        benchmark_name=benchmark_row.name,
        contract_name=request.contract_name,
        concurrency=request.concurrency,
        started_at=benchmark_row.started_at,
        finished_at=cast(datetime, benchmark_row.finished_at),
        task_ids=verified_task_ids,
        final_score=final_score["final_score"],
        resolved_tasks=final_score["resolved_tasks"],
        unresolved_tasks=final_score["unresolved_tasks"],
        evaluation_results=evaluation_results,
    )
