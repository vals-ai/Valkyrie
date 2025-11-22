from model_library.base import (
    QueryResult,
)
from typing_extensions import override
from vals import QuestionAnswerPair
from agentic_harness.base.types import Task
from agentic_harness.base.agent import Agent
from agentic_harness.logger import get_logger

logger = get_logger(__name__)


class FinanceAgent(Agent):
    """
    Finance agent that that uses the platform to evaluate the result of the task

    NOTE: This is hardcoded and needs to be changed
    If we abstract the evaluate result method into a class, we can just use a generic agent class for this
    """

    @override
    async def evaluate_result(self, task: Task, result: QueryResult) -> None:
        """Uploads the task to the platform where we also evaluate the result"""

        qa_set_id = task.extra.get("qa_set_id", None)

        if qa_set_id is None:
            raise ValueError("Run ID is required to evaluate task result")

        await QuestionAnswerPair.upload(
            qa_set_id=qa_set_id,
            test_id=task.id,
            query_result=result,
        )

        logger.info(f"Uploaded result for task: `{task.id}` to qa_set: `{qa_set_id}`")
