from abc import ABC, abstractmethod
from typing import Any

from src.base.types import DatasetConfig, TaskGroup


class Dataset(ABC):
    """Constructs dataset for a benchmark."""

    _config: DatasetConfig

    def __init__(self, config: DatasetConfig):
        self._config = config

    @property
    def config(self) -> DatasetConfig:
        return self._config

    @property
    def kwargs(self) -> dict[str, Any]:
        return self.config.kwargs

    @abstractmethod
    async def create(self) -> list[TaskGroup]:
        """Creates the final task group list."""
        ...
