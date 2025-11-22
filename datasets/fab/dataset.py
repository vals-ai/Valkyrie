from model_library.base import TextInput
from typing_extensions import override
from vals import Suite
from agentic_harness.base.dataset import Dataset
from agentic_harness.base.types import Task, TaskGroup
from textwrap import dedent

from agentic_harness.logger import get_logger

logger = get_logger(__name__)


class FinanceAgentDataset(Dataset):
    INSTRUCTIONS_PROMPT = dedent("""You are a financial agent. Today is April 07, 2025. You are given a question and you need to answer it using the tools provided.
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
    """)

    @override
    async def create(self) -> list[TaskGroup]:
        suite_id = self._config.get("suite_id")
        if suite_id is None:
            raise ValueError("`dataset.suite_id` is required")

        logger.info(f"Creating dataset for suite: `{suite_id}`")

        suite = await Suite.from_id(suite_id)
        tests = suite.tests

        tasks: list[Task] = []
        for test in tests:
            formatted_question = self.INSTRUCTIONS_PROMPT.format(
                question=test.input_under_test
            )
            input = [TextInput(text=formatted_question)]
            task = Task(id=test.id or "", input=input)
            tasks.append(task)

        logger.info(f"Created `{len(tasks)}` tasks")

        return [TaskGroup(tasks=tasks)]
