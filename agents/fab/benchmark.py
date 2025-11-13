import json
from typing import Any, cast, override

from model_library.base import QueryResult, QueryResultMetadata
from vals import QuestionAnswerPair

from src.classes import Agent, Benchmark, Dataset
from src.models import Task


class FinanceAgentBenchmark(Benchmark):
    _agent: Agent
    _evalute_locally: bool = False

    def __init__(self, dataset: Dataset, agent: Agent):
        super().__init__(dataset)
        self._agent = agent

    @override
    async def run(self) -> None:
        dataset = await self._dataset.dataset
        for task in dataset:
            # Takes in the task and creates the prompt we feed into the agent at run time
            input_items = await self.prepare_task(task)

            # Runs the agent scaffold and produces the final output
            output = await self._agent.run(input_items)

            # Evaluates the output inside of the platform
            evaluation_result = await self.evaluate(task, output)

            # Downloads the results locally to a json file
            await self.upload_results(evaluation_result)

    async def platform_evaluate(self, task: Task, output: dict[str, Any]) -> dict[str, Any]:
        """
        Evaluates the output on the platform.

        NOTE: This is an example of how it would look, ensure that the final output from the agent scaffold is a dictionary with the keys required to fill out the object.
        """

        qa_set_id = cast(str, output.get("qa_set_id"))
        test_id = cast(str, task.extra.get("test_id"))

        query_result = QueryResult(
            output_text=output.get("final_response"),
            reasoning=output.get("reasoning"),
            metadata=QueryResultMetadata(
                cost=output.get("cost"),
                duration_seconds=output.get("duration_seconds"),
                in_tokens=cast(int, output.get("in_tokens")),
                out_tokens=cast(int, output.get("out_tokens")),
            ),
        )

        result = await QuestionAnswerPair.upload(
            qa_set_id=qa_set_id,
            test_id=test_id,
            query_result=query_result,
        )

        return result.model_dump()

    @override
    async def evaluate(self, task: Task, output: dict[str, Any]) -> dict[str, Any]:
        """
        Takes in the final output from the agent scaffold and evaluates it by uploading the result to the platform.
        """
        if not self._evalute_locally:
            raise ValueError(
                "No funtionality built in to evaluate locally, results are uploaded to the platform"
            )

        return await self.platform_evaluate(task, output)

    @override
    async def upload_results(self, evaluation_result: dict[str, Any]) -> None:
        """
        Can either ommit if we want to just use what is inside of the platform or we can save the results locally to a json file.
        """

        with open("evaluation_results.json", "w") as f:
            json.dump(evaluation_result, f, indent=4)
