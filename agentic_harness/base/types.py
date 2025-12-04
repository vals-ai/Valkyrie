from pydantic import BaseModel, model_validator
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


class AgentConfig(BaseModel):
    """Generic agent configuration"""

    model: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    reasoning_effort: str | None = None
    extra: dict[str, Any] = {}


class BaseConfig(BaseModel):
    """
    Main config. should be inside of `config/<benchmark>.yaml`
    """

    class ConfigDict:
        extra = "ignore"

    # Benchmark specific parameters
    benchmark: str
    agent: str

    # Agent Parameters
    agent_config: AgentConfig = AgentConfig()

    # Dataset Parameters
    dataset: dict[str, Any] = {}

    @model_validator(mode="before")
    def validate_config(cls, config: dict[str, Any]) -> dict[str, Any]:
        """NOTE: Human readable validation"""
        if "benchmark" not in config:
            raise ValueError("`benchmark` is required")
        if "agent" not in config:
            raise ValueError("`agent` is required")

        dataset_name = config.get("dataset", {}).get("name", None)
        if dataset_name is None:
            raise ValueError("`dataset.name` is required")

        return config
