from abc import ABC, abstractmethod

from src.models import Task


class Dataset(ABC):
    """
    Base class for fetching datasets and preparing them for agent execution.

    You can define dataset creation, setup scripts inside of here
    """

    _dataset: list[Task] = []

    @property
    async def dataset(self) -> list[Task]:
        """Returns dataset if it exists, else fetches it and caches it."""
        if not self._dataset:
            self._dataset = await self._fetch_dataset()

        return self._dataset

    @abstractmethod
    async def _fetch_dataset(self) -> list[Task]:
        """
        Fetch raw dataset from the source, this is also where any filtering or pruning should be done.
        The goal is to break the dataset into a list of tasks.

        example:
        ```python

        from datasets import load_dataset

        # Load MMMU dataset from huggingface
        ds = load_dataset("MMMU/MMMU_Pro", "standard (4 options)")

        # Return the dataset as a list of dicts
        return ds.to_list()
        ```
        """
        ...
