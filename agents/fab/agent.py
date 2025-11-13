from typing import Any

from model_library.base import InputItem
from typing_extensions import override

from src.classes import Agent


class FinanceAgent(Agent):
    @override
    async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
        """
        Agent scaffold for the finance agent benchmark
        """
        ...
