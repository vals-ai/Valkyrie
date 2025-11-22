from model_library.base import TextInput
from typing_extensions import override
from vals import Suite
from agentic_harness.base.dataset import Dataset
from agentic_harness.base.types import Task, TaskGroup


class FinanceAgentDataset(Dataset):
    _suite_id: str = "fdf9a783-a522-484f-a139-e47bbb5571ac"

    INSTRUCTIONS_PROMPT = """You are a financial agent. Today is April 07, 2025. You are given a question and you need to answer it using the tools provided.
You may not interract with the user.
When you have the answer, you should respond with 'FINAL ANSWER:' followed by your answer.
At the end of your answer, you should provide your sources in a dictionary with the following format:
{{
    "sources": [
        {{
            "url": "https://example.com",
            "name": "Name of the source"
        }},
        ...
    ]
}}

Question:
{question}
"""

    @override
    async def create(self) -> list[TaskGroup]:
        suite = await Suite.from_id(self._suite_id)
        tests = suite.tests

        tasks: list[Task] = []
        for test in tests:
            formatted_question = self.INSTRUCTIONS_PROMPT.format(
                question=test.input_under_test
            )
            input = [TextInput(text=formatted_question)]
            task = Task(id=test.id or "", input=input)
            tasks.append(task)

        return [TaskGroup(tasks=tasks)]
