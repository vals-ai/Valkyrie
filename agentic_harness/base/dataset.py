from abc import ABC, abstractmethod

from agentic_harness.base.types import DatasetConfig, TaskGroup


class Dataset(ABC):
    """Constructs dataset for a benchmark."""

    _config: DatasetConfig

    def __init__(self, config: DatasetConfig):
        self._config = config

    @abstractmethod
    async def create(self) -> list[TaskGroup]:
        """Constructs dataset for a benchmark."""
