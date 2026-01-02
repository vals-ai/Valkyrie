import pytest

from agentic_harness.base.types import DatasetConfig
from agentic_harness.logger import get_logger
from agentic_harness.utils import setup_environment
from datasets.ioi.dataset import IOIDataset

logger = get_logger(__name__)

setup_environment()


@pytest.fixture
def dataset_config() -> DatasetConfig:
    return DatasetConfig(name="ioi 2025", suite_id="e37b9fb3-44e5-46c2-94b8-3503ef96ae9b")


@pytest.mark.integration
class TestIOI:
    """
    Integration tests for the IOI benchmark

    TODO: Build out testing for
    - Benchmark class
    - Evaluation class
    - Contract class
    """

    async def test_create_dataset(self, dataset_config: DatasetConfig):
        """Tests that the dataset is created and that the correct amount of task groups are created"""
        dataset = IOIDataset(config=dataset_config)

        task_groups = await dataset.create()

        task_groups_str = "\n".join([str(task_group)[200:500] for task_group in task_groups])

        logger.info(task_groups_str)

        assert len(task_groups) == 6, "Expected 6 task groups"
