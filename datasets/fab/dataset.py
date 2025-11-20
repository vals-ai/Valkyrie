from model_library.base import TextInput
from typing_extensions import override
from vals import Suite
from agentic_harness.base.dataset import Dataset, Task, TaskGroup
from agentic_harness.registry import register_dataset


@register_dataset("fab")
class FinanceAgentDataset(Dataset):
    suite_id = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    @override
    async def create(self) -> list[TaskGroup]:
        suite = await Suite.from_id(self._suite_id)
        tests = suite.tests

        tasks: list[Task] = []
        for test in tests:
            input = [TextInput(text=test.input_under_test)]
            task = Task(id=test._id, input=input)  # pyright: ignore[reportPrivateUsage]
            tasks.append(task)

        return [TaskGroup(tasks=tasks)]
