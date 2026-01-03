from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from pydantic import BaseModel

from tracker.benchmark_service import BenchmarkService
from tracker.config import BENCHMARK_SERVICE_URL
from tracker.exceptions import BenchmarkServiceError, S3Error, SandboxError
from tracker.logger import get_logger
from tracker.s3 import get_contract_s3_key, upload_to_s3
from tracker.sandbox import create_sandbox, install_dependencies, run_agent, upload_contract_to_sandbox

logger = get_logger(__name__)

app = FastAPI()


@app.exception_handler(SandboxError)
async def sandbox_error_handler(_request: Request, exc: SandboxError):
    """Handle sandbox-related errors"""
    logger.error(f"Sandbox error: {exc}", exc_info=True)
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.exception_handler(BenchmarkServiceError)
async def benchmark_service_error_handler(_request: Request, exc: BenchmarkServiceError):
    """Handle benchmark service communication errors"""
    logger.error(f"Benchmark service error: {exc}", exc_info=True)
    raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.exception_handler(S3Error)
async def s3_error_handler(_request: Request, exc: S3Error):
    """Handle S3 storage errors"""
    logger.error(f"S3 error: {exc}", exc_info=True)
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


# TODO: move these to a schemas.py have more models
class StartRunRequest(BaseModel):
    contract_name: str
    benchmark_name: str


@app.post("/start-run")
async def start_run(request: StartRunRequest):
    """
    Start a benchmark run with the uploaded contract.

    Usage:
    curl -X POST http://<endpoint>/start-run \
      -H "Content-Type: application/json" \
      -d '{"contract_name": "claude_code", "benchmark_name": "swebench"}'

    Returns:
    {
        "status": "success",
        "message": "Benchmark run started",
        "contract_name": "claude_code",
        "benchmark_name": "swebench"
    }

    Returns:
    - 200 OK if run starts successfully
    - 400 Bad Request if parameters are invalid
    - 500 Internal Server Error if run fails to start
    """
    logger.info(f"Starting benchmark run - contract: {request.contract_name}, benchmark: {request.benchmark_name}")

    benchmark_name = request.benchmark_name
    contract_name = request.contract_name

    benchmark_service = BenchmarkService(benchmark_name, url=BENCHMARK_SERVICE_URL)

    await benchmark_service.request_health_check()

    # TODO: pass in actual list of tasks
    task_ids: list[str] = ["astropy__astropy-12907"]
    verified_task_ids = await benchmark_service.request_verify_task_ids(task_ids=task_ids)
    retrieved_tasks = await benchmark_service.request_retrieve_tasks(verified_task_ids)

    # TODO: run using a semaphore
    evaluation_results: dict[str, dict[str, str]] = {}
    for task_id, task in retrieved_tasks.items():
        logger.info(f"Processing task ID: {task_id}")

        docker_image = task["docker_image"]
        request_setup = task["request_setup"]

        async with create_sandbox(benchmark_service.daytona, task_id, docker_image) as sandbox:
            await upload_contract_to_sandbox(sandbox, contract_name)
            await install_dependencies(sandbox, contract_name)

            if request_setup:
                # TODO: pass in actual instance id
                await benchmark_service.request_setup_task(task_id, task_id)

            await run_agent(sandbox, contract_name, task_id, task["problem_statement"])

            # TODO: pass in actual instance id
            evaluation_result = await benchmark_service.request_evaluate_instance(task_id, task_id)
            evaluation_results[task_id] = evaluation_result

    final_score = await benchmark_service.request_final_score(evaluation_results)

    return {
        "status": "success",
        "message": "Benchmark run started",
        "contract_name": request.contract_name,
        "benchmark_name": request.benchmark_name,
        "final_score": final_score,
    }
