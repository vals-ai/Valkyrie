from abc import ABC, abstractmethod
from typing import Any

from vals_model_proxy.base import InputItem

from src.classes import Dataset
from src.models import Task


class Benchmark(ABC):
    _dataset: Dataset

    def __init__(self, dataset: Dataset):
        self._dataset = dataset

    @abstractmethod
    async def prepare_task(self, task: Any) -> list[InputItem]:
        """
        Takes in a single task and returns a list of InputItems.
        The result of this will be what is passed down to the agent runner and fed to the model at run time.

        You should also include the system prompt inside of here if necessary.

        example:
        ```python

        system_prompt = TextInput(
            text=self.system_prompt.format(question=task["question"],
            expected_output=task["expected_output"])
        )

        examples = ....

        return [
            system_prompt,
            ...examples,
            TextInput(
                text=f"Task {task_id}: {task['question']}"
            )
        ]
        ```
        """
        ...

    @abstractmethod
    async def run(self) -> None:
        """
        Runs the entire benchmark, this is where we would instantiate the agent runners and handle
        parralization of the tasks.
        """
        ...

    @abstractmethod
    async def evaluate(self, task: Task, output: dict[str, Any]) -> dict[str, Any]:
        """
        Takes in the final response from the agent harness and evaluates it against the expected output for a given task.

        Inside of this method you must define what the evaluation process looks like. The original task is provided for context.
        """
        ...

    @abstractmethod
    async def upload_results(self, evaluation_result: dict[str, Any]) -> None:
        """
        Takes the evaluation result and exports it.

        Possible options
        - Upload to the platform
        - Upload to s3
        - Store locally inside of a json file
        """
        ...
