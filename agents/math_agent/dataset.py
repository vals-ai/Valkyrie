from model_library.base import TextInput
from typing_extensions import override

from src.classes import Dataset
from src.models import Task


class MathAgentDataset(Dataset):
    _dataset: list[Task] = []

    @override
    async def _fetch_dataset(self) -> list[Task]:
        dataset = [
            Task(
                input=[TextInput(text="whats 1 + 1?")],
                extra={
                    "answer": "2",
                    "test_id": "1",
                },
            )
        ]

        return dataset
