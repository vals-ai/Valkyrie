import uuid
from pathlib import Path

from model_library.base import TextInput
from typing_extensions import override

from src.base.dataset import Dataset
from src.base.types import Image, Sandbox, Task, TaskGroup


class TestDataset(Dataset):
    @property
    def specsheet_path(self) -> Path:
        return Path("src/datasets/test/specsheet.txt")

    @property
    def dockerfile_path(self) -> Path:
        return Path("submodules/claude_code/Dockerfile.external")

    @override
    async def create(self) -> list[TaskGroup]:
        task_groups: list[TaskGroup] = []
        with open(self.specsheet_path, "r") as f:
            specsheet = f.read()

        if not self.dockerfile_path.exists():
            raise ValueError(f"Dockerfile does not exist at {self.dockerfile_path}")

        task = Task(
            id=str(uuid.uuid4()),
            input=[TextInput(text=specsheet)],
            sandbox=Sandbox(image=Image(dockerfile=self.dockerfile_path)),
        )
        task_groups.append(TaskGroup(tasks=[task]))

        return task_groups
