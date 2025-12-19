from typing import Any

from model_library.base import InputItem
from pydantic import BaseModel, model_validator


class EnvironmentKeys(BaseModel):
    """Inherited by environments expected to return desired dependencies to inject into the sandbox environment"""

    ...


class Sandbox(BaseModel):
    """
    Represents a sandbox.
    """

    id: str | None = None
    image_path: str | None = None


class Task(BaseModel):
    """
    Represents one task.

    id field represents the run_id for platform benchmarks.
    """

    id: str
    input: list[InputItem]
    extra: dict[str, Any] = {}

    # Sandbox information
    sandbox: Sandbox | None = None


class TaskGroup(BaseModel):
    """Collection of tasks."""

    tasks: list[Task]


DatasetConfig = dict[str, Any]


class AgentConfig(BaseModel):
    """Generic agent configuration"""

    name: str
    model: str | None = None
    parallelism: int = 1
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    reasoning_effort: str | None = None

    # Environment Parameters
    environment: dict[str, Any] = {}

    # Extra parameters we can pass into the agent
    extra: dict[str, Any] = {}


class BaseConfig(BaseModel):
    """
    Main config. should be inside of `config/<benchmark>.yaml`
    """

    class ConfigDict:
        extra = "ignore"

    # Benchmark specific parameters
    benchmark: str = "base"

    agent: AgentConfig

    # Dataset Parameters
    dataset: dict[str, Any] = {}

    @model_validator(mode="before")
    def validate_config(cls, config: dict[str, Any]) -> dict[str, Any]:
        """NOTE: Human readable validation"""
        if "agent" not in config:
            raise ValueError("`agent` is required")

        dataset_name = config.get("dataset", {}).get("name", None)
        if dataset_name is None:
            raise ValueError("`dataset.name` is required")

        return config
