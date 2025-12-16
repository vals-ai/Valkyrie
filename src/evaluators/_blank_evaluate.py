from model_library.base import (
    QueryResult,
)
from typing_extensions import override

from src.base.evaluate import Evaluator
from src.base.types import Task
from src.logger import get_logger

logger = get_logger(__name__)


class BlankEvaluator(Evaluator):
    """
    Evaluator that does nothing
    """

    @override
    async def evaluate(self, task: Task, result: QueryResult) -> None:
        logger.info(f"Evaluated task: `{str(task)}`")
        logger.info(f"Result: `{str(result)}`")
