from abc import ABC, abstractmethod
from typing import Any

from vals_model_proxy.base import InputItem


class Agent(ABC):
    @abstractmethod
    async def run(self, input_items: list[InputItem]) -> dict[str, Any]: ...
