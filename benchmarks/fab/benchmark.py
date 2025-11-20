from typing_extensions import override
from agentic_harness.base import Agent
from agentic_harness.base.benchmark import Benchmark
from agentic_harness.base.dataset import Task
from vals import QuestionAnswerPair, RunParameters, Suite

from agentic_harness.registry import register_benchmark


@register_benchmark("fab")
class FinanceAgentBenchmark(Benchmark):
    suite_id = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    def __init__(self):
        self._run = None

    @override
    async def run(self, task: Task, agent: Agent) -> None:
        """
        Evaluate a single task.

        TODO: this should not rely on instance variables;
        """
        if not self._run:
            self._run = await Suite.create_run(
                self._suite_id,
                parameters=RunParameters(
                    eval_model="openai/gpt-4o-2024-08-06",
                    system_prompt="",
                    run_confidence_evaluation=False,
                    create_text_summary=False,
                ),  # TODO: params should live in yaml
            )

        result = await agent.run(task.input)

        await QuestionAnswerPair.upload(
            run_id=self._run.id,
            qa_set_id=self._run.qa_set_id,
            test_id=task.id,
            query_result=result,
        )
