from abc import ABC, abstractmethod
from typing import Any

from src.base.types import Sandbox, Task


class Environment(ABC):
    """
    Base environment class that all environments must implement.

    TODO: Add some examples inside here for usage
    """

    _config: dict[str, Any]

    def __init__(self, config: dict[str, Any]):
        self._config = config

    @abstractmethod
    async def setup(self) -> None: ...

    @abstractmethod
    async def create(self, task: Task) -> None: ...

    @abstractmethod
    async def execute(self, command: str) -> None: ...

    @staticmethod
    @abstractmethod
    async def close(sandbox: Sandbox) -> None: ...
