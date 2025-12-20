from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from src.base.types import EnvironmentKeys, Task

if TYPE_CHECKING:
    from src.base_agent import AgentRunner


class Environment(ABC):
    """
    Base environment class that all environments must implement.
    """

    _submodule_name: str
    _contract_name: str
    _config: dict[str, Any]

    def __init__(self, config: dict[str, Any], submodule_name: str, contract_name: str):
        self._submodule_name = submodule_name
        self._contract_name = contract_name
        self._config = config

    @staticmethod
    @abstractmethod
    def environment_keys() -> EnvironmentKeys: ...

    @abstractmethod
    async def setup(self) -> None: ...

    @abstractmethod
    async def create(self, task: Task, agent: "AgentRunner") -> None: ...

    @staticmethod
    @abstractmethod
    async def close(sandbox_id: str) -> None: ...
