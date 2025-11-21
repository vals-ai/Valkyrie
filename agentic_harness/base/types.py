from pydantic import BaseModel
from typing import Any
from model_library.base import InputItem


class Task(BaseModel):
    """
    Represents one task.

    id field represents the run_id for platform benchmarks.
    """

    id: str
    input: list[InputItem]
    extra: dict[str, Any] = {}


class TaskGroup(BaseModel):
    """Collection of tasks."""

    tasks: list[Task]


DatasetConfig = dict[str, Any]


class BaseParameters(BaseModel):
    """
    Base parameters for an agent.
    """

    # Model Parameters
    model: str
    temperature: float
    max_tokens: int
    top_p: float
    extra: dict[str, Any]
    reasoning_effort: str

    # Dataset Parameters
    dataset: dict[str, Any]
