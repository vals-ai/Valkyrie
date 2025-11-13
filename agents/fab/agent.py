from typing import Any, override

from vals_model_proxy.base import InputItem

from src.classes import Agent


class FinanceAgent(Agent):
    @override
    async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
        """
        Agent scaffold for the finance agent benchmark
        """
        ...
