from abc import ABC, abstractmethod

from model_library.base import InputItem, QueryResult


class Agent(ABC):
    @abstractmethod
    async def run(self, input_items: list[InputItem]) -> QueryResult: ...
