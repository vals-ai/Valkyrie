from typing import override
from agentic_harness.base.benchmark import Benchmark
from vals import RunParameters, Suite
from vals import Run
from dotenv import load_dotenv
from agentic_harness.base.types import TaskGroup
import os
from vals import configure_credentials
from agentic_harness.logger import get_logger

logger = get_logger(__name__)

_ = load_dotenv()

_vals_api_key = os.getenv("VALS_API_KEY")
if _vals_api_key is None:
    raise ValueError("VALS_API_KEY is not set")

configure_credentials(api_key=_vals_api_key, endpoint="https://bench.platform.vals.ai")


class FinanceAgentBenchmark(Benchmark):
    _run: Run | None = None
    _EVAL_MODEL: str = "openai/gpt-4o-2024-08-06"

    async def _create_run(self) -> Run:
        """Helper method to create a run object"""

        parameters = RunParameters(
            eval_model=self._EVAL_MODEL,
            run_confidence_evaluation=False,
            create_text_summary=False,
        )

        logger.info(f"Creating run with parameters: `{str(parameters)}`")

        suite_id = self._dataset.config.get("suite_id")
        if suite_id is None:
            raise ValueError("`dataset.suite_id` is required")

        if self.agent.config.model is None:
            raise ValueError("`agent.config.model` is required")

        return await Suite.create_run(
            suite_id, parameters=parameters, model_under_test=self.agent.config.model
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
