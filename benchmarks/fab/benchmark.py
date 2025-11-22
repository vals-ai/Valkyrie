from typing import override
from model_library.base import QueryResult
from agentic_harness.base.benchmark import Benchmark
from vals import QuestionAnswerPair, RunParameters, Suite
from vals import Run

from agentic_harness.base.types import Task, TaskGroup


class FinanceAgentBenchmark(Benchmark):
    _suite_id: str = "fdf9a783-a522-484f-a139-e47bbb5571ac"
    _run: Run | None = None

    async def _create_run(self) -> Run:
        """Helper method to create a run object"""
        parameters = RunParameters(
            eval_model="openai/gpt-4o-2024-08-06",
            system_prompt="",
            run_confidence_evaluation=False,
            create_text_summary=False,
        )

        return await Suite.create_run(self._suite_id, parameters=parameters)

    @override
    async def _create_dataset(self) -> list[TaskGroup]:
        """
        Override to create run and add qa_set_id to each task

        NOTE: This could also be created into a hook but until we really need it, this works
        """
        self._run = await self._create_run()
        dataset = await super()._create_dataset()
        for task_group in dataset:
            for task in task_group.tasks:
                task.extra["qa_set_id"] = self._run.qa_set_id

        return dataset

    @classmethod
    async def evaluate_task_result(cls, task: Task, result: QueryResult) -> None:
        """Uploads the task to the platform where we also evaluate the result"""

        qa_set_id = task.extra.get("qa_set_id", None)

        if qa_set_id is None:
            raise ValueError("Run ID is required to evaluate task result")

        await QuestionAnswerPair.upload(
            qa_set_id=qa_set_id,
            test_id=task.id,
            query_result=result,
        )
