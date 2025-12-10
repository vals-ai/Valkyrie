from abc import ABC, abstractmethod

from src.base.types import DatasetConfig, TaskGroup


class Dataset(ABC):
    """Constructs dataset for a benchmark."""

    _config: DatasetConfig

    def __init__(self, config: DatasetConfig):
        self._config = config

    @property
    def config(self) -> DatasetConfig:
        return self._config

    @abstractmethod
    async def create(self) -> list[TaskGroup]:
        """Creates the final task group list."""
        ...
