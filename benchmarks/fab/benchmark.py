from typing_extensions import override
from model_library.base import TextInput
from agentic_harness.base.agent import Agent
from agentic_harness.base.benchmark import Benchmark, TaskGroup, Task
from vals import QuestionAnswerPair, RunParameters, Suite

from agentic_harness.registry import register_benchmark


@register_benchmark("fab")
class FinanceAgentBenchmark(Benchmark):
    suite_id = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    def __init__(self):
        self.run = None

    @override
    async def dataset(self) -> list[TaskGroup]:
        suite = await Suite.from_id(self.suite_id)
        tests = suite.tests

        tasks: list[Task] = []
        for test in tests:
            input = [TextInput(text=test.input_under_test)]
            task = Task(id=test._id, input=input)  # pyright: ignore[reportPrivateUsage]
            tasks.append(task)

        return [TaskGroup(tasks=tasks)]

    @override
    async def evaluate(self, task: Task, agent: Agent) -> None:
        """
        Evaluate a single task using an agent.

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

        result = await agent.run(task.input)

        await QuestionAnswerPair.upload(
            run_id=self.run.id,
            qa_set_id=self.run.qa_set_id,
            test_id=task.id,
            query_result=result,
        )
