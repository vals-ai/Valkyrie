from abc import ABC, abstractmethod

from src.models import Task


class Dataset(ABC):
    """
    Abstract base class for managing benchmark datasets.

    The Dataset class is responsible for fetching, caching, and providing structured data
    for benchmark evaluation. It serves as the data source for Benchmark instances.

    Relationship to other classes:
    - Used by: Benchmark (receives dataset through constructor)
    - Provides: A list of Task objects for benchmark execution

    Implementation requirements:
    - Subclasses must implement _fetch_dataset() to load data from any source
    - The dataset property handles caching automatically
    - Each task should be structured according to the Task model
    """

    _dataset: list[Task] = []

    @property
    async def dataset(self) -> list[Task]:
        """
        Returns the dataset, fetching and caching it on first access.

        This property ensures the dataset is only fetched once and cached for
        subsequent access, optimizing performance for repeated benchmark runs.

        Returns:
            list[Task]: A list of Task objects ready for benchmark execution.
        """
        if not self._dataset:
            self._dataset = await self._fetch_dataset()

        return self._dataset

    @abstractmethod
    async def _fetch_dataset(self) -> list[Task]:
        """
        Fetch and process the raw dataset from its source.

        This method should handle all data loading, filtering, and transformation
        logic to convert raw data into a list of Task objects. This is called
        automatically by the dataset property on first access.

        Returns:
            list[Task]: Processed dataset as a list of Task objects.

        Example:
            ```python
            from datasets import load_dataset

            async def _fetch_dataset(self) -> list[Task]:
                # Load dataset from HuggingFace
                ds = load_dataset("MMMU/MMMU_Pro", "standard (4 options)")

                # Convert to Task objects with proper structure
                tasks = [
                    Task(id=i, input=item["question"], expected_output=item["answer"])
                    for i, item in enumerate(ds)
                ]

                return tasks
            ```
        """
        ...
