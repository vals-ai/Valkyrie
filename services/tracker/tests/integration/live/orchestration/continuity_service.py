"""Small real benchmark service for local executor-continuity E2E runs."""

import sys
from collections.abc import AsyncGenerator
from typing import Any

import uvicorn
from benchmark_service import BenchmarkService, ImageSource, Resources, Sandbox
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
    StreamResultChunk,
)
from benchmark_service.v1_schemas import V1Task


class ContinuityBenchmark(BenchmarkService):
    """Grade a file written by the real agent after the test releases its wait."""

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        return {"default": {"task-0": {"question": "Write continuity-ok to /workspace/final_output/answer.txt."}}}

    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        return [V1Task(id=task_id, question=task["question"]) for task_id, task in self.get_dataset(dataset).items()]

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        if not skip_validation:
            await self.validate_task_ids([task_id], dataset)

        return RetrieveTaskResponse(
            source=ImageSource(image="python:3.11-slim"),
            problem_path="/tmp/continuity-problem.txt",
            cwd="/workspace",
            agent_timeout=300,
            resources=Resources(vcpu=1, memory=2, disk=3),
        )

    async def setup_task(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        await sandbox.upload_file(
            "/tmp/continuity-problem.txt", self.get_dataset(dataset)[task_id]["question"].encode()
        )
        yield StreamResultChunk(type="result", data={"status": "ok"})

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        return {"score": int(request.response == "continuity-ok")}

    async def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        result = await sandbox.exec("cat /workspace/final_output/answer.txt", timeout=15)
        yield StreamResultChunk(
            type="result",
            data={
                "score": int(result.exit_code == 0 and result.stdout.strip() == "continuity-ok"),
                "sandbox_id": sandbox.id,
            },
        )

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        scores = [result["score"] for result in evaluation_results.values() if result is not None]

        return FinalScoreResult(score=sum(scores) / len(scores) if scores else 0, metadata={"tasks": len(scores)})


if __name__ == "__main__":
    uvicorn.run(BenchmarkServiceApp(ContinuityBenchmark), host="127.0.0.1", port=int(sys.argv[1]), access_log=False)
