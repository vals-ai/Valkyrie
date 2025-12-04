from typing import override
from agentic_harness.base.benchmark import Benchmark
from vals import RunParameters, Suite
from vals import Run
from agentic_harness.base.types import TaskGroup
from agentic_harness.logger import get_logger
from agentic_harness.utils import setup_environment

logger = get_logger(__name__)

setup_environment()


class FinanceAgentBenchmark(Benchmark):
    _run: Run | None = None
    _EVAL_MODEL: str = "openai/gpt-4o-2024-08-06"

    async def _create_run(self) -> Run:
        """Helper method to create a run object"""

        parameters = RunParameters(
            eval_model=self._EVAL_MODEL,
            run_confidence_evaluation=False,
            create_text_summary=False,
            temperature=self.agent.config.temperature
            or 1,  # NOTE: will raise if its falsy
        )

        logger.info(f"Creating run with parameters: `{str(parameters)}`")

        suite_id = self._dataset.config.get("suite_id")
        if suite_id is None:
            raise ValueError("`dataset.suite_id` is required")

        project_id = self._dataset.config.get("project_id") or "default-project"

        model = self.agent.config.model

        if model is None:
            raise ValueError("`agent.config.model` is required")

        name = self._dataset.config.get("name")
        if name is None:
            raise ValueError("`dataset.name` is required")

        return await Suite.create_run(
            suite_id,
            parameters=parameters,
            model_under_test=model,
            project_id=project_id,
            run_name=f"{name}-{model}",
        )

    @override
    async def _create_dataset(self) -> list[TaskGroup]:
        """
        Override to create run and add qa_set_id to each task
        """
        logger.info(f"Creating run for suite: `{self._dataset.config.get('suite_id')}`")

        self._run = await self._create_run()

        logger.info(
            f"Created run with id `{self._run.id}` and qa_set_id `{self._run.qa_set_id}`"
        )

        dataset = await super()._create_dataset()
        for task_group in dataset:
            for task in task_group.tasks:
                task.extra["qa_set_id"] = self._run.qa_set_id

        return dataset
