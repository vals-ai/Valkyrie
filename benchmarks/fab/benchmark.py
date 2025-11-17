from asgiref.sync import async_to_sync
from typing_extensions import override
from model_library.base import TextInput
from agentic_harness.base.benchmark import Benchmark, TaskGroup, Task
from vals import Suite


class FinanceAgentBenchmark(Benchmark):
    SUITE_ID = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    @staticmethod
    async def _pull_dataset(suite_id: str) -> list[TaskGroup]:
        suite = await Suite.from_id(suite_id)
        tests = suite.tests

        tasks: list[Task] = []
        for test in tests:
            input = [TextInput(text=test.input_under_test)]
            task = Task(input=input)
            tasks.append(task)

        return [TaskGroup(tasks=tasks)]

    @property
    @override
    def dataset(self) -> list[TaskGroup]:
        return async_to_sync(self._pull_dataset)(self.SUITE_ID)
