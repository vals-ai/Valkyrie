from typing import Any

from model_library.base import LLM, InputItem
from typing_extensions import override

from src.classes import Agent


class MathAgent(Agent):
    _model: LLM

    def __init__(self, model: LLM):
        self._model = model

    @override
    async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
        """
        Queries the model and returns the output.
        """
        query_result = await self._model.query(
            input_items,
        )

        return query_result.model_dump()
