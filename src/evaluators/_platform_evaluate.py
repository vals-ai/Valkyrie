from model_library.base import (
    QueryResult,
)
from typing_extensions import override
from vals import QuestionAnswerPair
from src.base.evaluate import Evaluator
from src.base.types import Task
from src.logger import get_logger

logger = get_logger(__name__)


class PlatformEvaluator(Evaluator):
    """
    Evaluator that uses the platform to evaluate the result of the task
    """

    @override
    async def evaluate(self, task: Task, result: QueryResult) -> None:
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
