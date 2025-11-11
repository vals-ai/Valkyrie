"""

General implemention of how we would import a dataset from a file and prepare it for the agent.

If we were downloading this from the platform we would use the sdk

```python
from vals import Suite

suite = await Suite.from_id("fdf9a783-a522-484f-a139-e47bbb5571ac")

tests = [test.model_dump() for test in suite.tests]
```
"""

from textwrap import dedent
from typing import override

from model_library.base import InputItem, TextInput
from vals import Suite
from vals.sdk.run import Test

from src.classes.dataset import Dataset
from src.models import Task


class FinanceAgentDataset(Dataset):
    """
    Implemented example of dataset retrieval for the finance agent benchmark
    """

    _suite_id: str = "xxxxxxxx-x..."

    @property
    def system_prompt(self) -> str | None:
        return dedent("""You are a financial agent. Today is April 07, 2025. You are given a question and you need to answer it using the tools provided.
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

    def _prepare_task(self, test: Test) -> list[InputItem]:
        """
        Takes in a single task and prepares a list of input items to be fed to the agent at run time.

        Finance agent dataset does not require anything extra so here the `len(input_items) == 1`
        """
        if not self.system_prompt:
            raise ValueError("System prompt is required to run this benchmark")

        task_input_text = test.input_under_test

        system_prompt = TextInput(text=self.system_prompt.format(question=task_input_text))

        return [system_prompt]

    @override
    async def _fetch_dataset(self) -> list[Task]:
        """
        Parses the dataset we downloaded locally and returns a list of tasks

        ```json
        {
            "test_id": "fb7a87d4-dd0b-454f-b81e-e1fb37ffa46b",
            "input_under_test": "What was the quarterly revenue of Paylocity (NASDAQ:PCTY) for the quarter ended December 31, 2024?",
            "tags": [
                "Simple retrieval - Quantitative"
            ],
            "files": [],
            "input_context": {
                "Writer": "Andrew Schettino",
                "Reviewer": "Matthew Friday",
                ...
            }
        }
        ```
        """
        suite = await Suite.from_id(self._suite_id)

        return [
            Task(input=self._prepare_task(test), extra=test.model_dump()) for test in suite.tests
        ]
