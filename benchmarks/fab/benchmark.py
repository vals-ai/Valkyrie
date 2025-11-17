from typing_extensions import override
from model_library.base import TextInput
from agentic_harness.base.agent import Agent
from agentic_harness.base.benchmark import Benchmark, TaskGroup, Task
from vals import Suite

from agentic_harness.registry import register_benchmark


@register_benchmark("fab")
class FinanceAgentBenchmark(Benchmark):
    suite_id = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    @override
    async def dataset(self) -> list[TaskGroup]:
        suite = await Suite.from_id(self.suite_id)
        tests = suite.tests

        tasks: list[Task] = []
        for test in tests:
            input = [TextInput(text=test.input_under_test)]
            task = Task(input=input)
            tasks.append(task)

        tasks = tasks[:1]  # TODO: remove
        return [TaskGroup(tasks=tasks)]

    @override
    async def run(self, agent: Agent) -> None:
        run = Suite.create_run(self.suite_id)

        return
