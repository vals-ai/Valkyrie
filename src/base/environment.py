from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from src.base.types import EnvironmentKeys, Task

if TYPE_CHECKING:
    from src.base_agent import BaseAgent


class Environment(ABC):
    """
    Base environment class that all environments must implement.

    TODO: Add some examples inside here for usage
    """

    _config: dict[str, Any]

    def __init__(self, config: dict[str, Any]):
        self._config = config

    @staticmethod
    @abstractmethod
    def _environment_keys() -> EnvironmentKeys: ...

    @abstractmethod
    async def setup(self) -> None: ...

    @abstractmethod
    async def create(self, task: Task, agent: "BaseAgent") -> None: ...

    @staticmethod
    @abstractmethod
    async def close(sandbox_id: str) -> None: ...
