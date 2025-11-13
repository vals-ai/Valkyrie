from abc import ABC, abstractmethod
from typing import Any

from vals_model_proxy.base import InputItem

from src.classes import Dataset
from src.models import Task


class Benchmark(ABC):
    """
    Abstract base class for orchestrating benchmark execution and evaluation.

    The Benchmark class coordinates the entire benchmark lifecycle: preparing tasks,
    running agents, evaluating results, and uploading outcomes. It acts as the central
    controller that brings together datasets and agents.

    Relationship to other classes:
    - Uses: Dataset (receives via constructor to get tasks)
    - Works with: Agent (calls agent.run() with prepared inputs)
    - Manages: Task preparation, execution, evaluation, and result storage

    Implementation requirements:
    - Subclasses must implement all abstract methods to define benchmark-specific behavior
    - The _dataset property provides access to the Dataset instance
    - You define how tasks are prepared for agents and how results are evaluated
    """

    _dataset: Dataset

    def __init__(self, dataset: Dataset):
        self._dataset = dataset

    @abstractmethod
    async def prepare_task(self, task: Any) -> list[InputItem]:
        """
        Transform a task into a list of InputItems for agent execution.

        This method converts a Task from the dataset into the format required by
        the Agent. It should include system prompts, examples, and task-specific
        instructions as InputItems that will be passed to the agent.

        Args:
            task: A Task object from the dataset to prepare for execution.

        Returns:
            list[InputItem]: Formatted inputs ready to pass to agent.run().

        Example:
            ```python
            from vals_model_proxy.base import TextInput

            async def prepare_task(self, task: Task) -> list[InputItem]:
                # System prompt with instructions
                system_prompt = TextInput(
                    text=f"You are a helpful assistant. Solve the following task: {task.input}"
                )

                # Optional few-shot examples
                examples = [
                    TextInput(text="Example 1: ..."),
                    TextInput(text="Example 2: ..."),
                ]

                # Actual task input
                task_input = TextInput(text=f"Task: {task.input}")

                return [system_prompt, *examples, task_input]
            ```
        """
        ...

    @abstractmethod
    async def run(self) -> None:
        """
        Execute the complete benchmark workflow.

        This is the main entry point that orchestrates the entire benchmark process:
        iterating through dataset tasks, preparing inputs, running agents, evaluating
        outputs, and uploading results. Handle parallelization and error handling here.

        Typical workflow:
            1. Get tasks from self._dataset
            2. For each task, call prepare_task() to format inputs
            3. Pass prepared inputs to agent.run()
            4. Evaluate agent output with evaluate()
            5. Upload results with upload_results()

        Example:
            ```python
            async def run(self) -> None:
                tasks = await self._dataset.dataset

                for task in tasks:
                    # Prepare inputs for the agent
                    input_items = await self.prepare_task(task)

                    # Run agent on prepared inputs
                    output = await self._agent.run(input_items)

                    # Evaluate agent performance
                    evaluation_result = await self.evaluate(task, output)

                    # Store or upload results
                    await self.upload_results(evaluation_result)
            ```
        """
        ...

    @abstractmethod
    async def evaluate(self, task: Task, output: dict[str, Any]) -> dict[str, Any]:
        """
        Evaluate agent output against expected results for a task.

        This method defines the evaluation logic specific to your benchmark. It compares
        the agent's output with the expected output from the task and produces metrics
        or scores indicating performance.

        Args:
            task: The original Task object containing expected output for comparison.
            output: The result dictionary returned by agent.run().

        Returns:
            dict[str, Any]: Evaluation results including metrics, scores, or pass/fail status.

        Example:
            ```python
            async def evaluate(self, task: Task, output: dict[str, Any]) -> dict[str, Any]:
                agent_answer = output.get("response", "")
                expected = task.expected_output

                # Simple exact match evaluation
                is_correct = agent_answer.strip() == expected.strip()

                return {
                    "task_id": task.id,
                    "correct": is_correct,
                    "agent_output": agent_answer,
                    "expected_output": expected,
                    "score": 1.0 if is_correct else 0.0
                }
            ```
        """
        ...

    @abstractmethod
    async def upload_results(self, evaluation_result: dict[str, Any]) -> None:
        """
        Store or upload evaluation results to a destination.

        This method handles persistence of benchmark results. Implementation depends
        on your storage requirements: local files, cloud storage, databases, or
        benchmark platforms.

        Args:
            evaluation_result: The dictionary returned by evaluate() containing results.

        Common implementations:
        - Save to local JSON files for easy inspection
        - Upload to S3 or cloud storage for centralized tracking
        - Send to benchmark platforms or APIs
        - Store in databases for analysis

        Example:
            ```python
            import json
            from pathlib import Path

            async def upload_results(self, evaluation_result: dict[str, Any]) -> None:
                # Save results to local JSON file
                results_dir = Path("results")
                results_dir.mkdir(exist_ok=True)

                task_id = evaluation_result["task_id"]
                output_path = results_dir / f"task_{task_id}_results.json"

                with open(output_path, "w") as f:
                    json.dump(evaluation_result, f, indent=2)
            ```
        """
        ...
