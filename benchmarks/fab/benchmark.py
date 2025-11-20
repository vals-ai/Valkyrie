from typing_extensions import override
from model_library.base import QueryResult
from agentic_harness.base.benchmark import Benchmark
from agentic_harness.base.dataset import Task
from vals import QuestionAnswerPair, RunParameters, Suite

from agentic_harness.registry import register_benchmark


@register_benchmark("fab")
class FinanceAgentBenchmark(Benchmark):
    suite_id = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    def __init__(self):
        self.run = None

    @override
    async def evaluate(self, task: Task, query_result: QueryResult) -> None:
        """
        Evaluate a single task.

        TODO: this should not rely on instance variables;
        """
        if not self.run:
            self.run = await Suite.create_run(
                self.suite_id,
                parameters=RunParameters(
                    eval_model="openai/gpt-4o-2024-08-06",
                    system_prompt="",
                    run_confidence_evaluation=False,
                    create_text_summary=False,
                ),  # TODO: params should live in yaml
            )

        await QuestionAnswerPair.upload(
            run_id=self.run.id,
            qa_set_id=self.run.qa_set_id,
            test_id=task.id,
            query_result=query_result,
        )
