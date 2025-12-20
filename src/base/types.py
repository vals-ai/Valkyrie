from pathlib import Path
from typing import Any

from model_library.base import InputItem
from pydantic import BaseModel, model_validator
from wonderwords import RandomWord

rw = RandomWord()


class EnvironmentKeys(BaseModel):
    """Inherited by environments expected to return desired dependencies to inject into the sandbox environment"""

    ...


class Image(BaseModel):
    dockerfile: Path | None = None
    name: str | None = None
    tag: str = "latest"

    @property
    def image_name(self) -> str:
        if self.name is None:
            adjective = rw.word(include_categories=["adjectives"])
            noun = rw.word(include_categories=["nouns"])

            name = f"{adjective.capitalize()}-{noun.capitalize()}"

            # In case we need to access it again
            self.name = name

            return name

        return self.image_name


class Sandbox(BaseModel):
    """
    Represents a sandbox.
    """

    id: str | None = None
    image: Image


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


class DatasetConfig(BaseModel):
    name: str
    kwargs: dict[str, Any] = {}


class EnvironmentConfig(BaseModel):
    name: str
    kwargs: dict[str, Any] = {}


class AgentConfig(BaseModel):
    """Generic agent configuration"""

    name: str
    model: str | None = None
    parallelism: int = 1
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    reasoning_effort: str | None = None

    # Extra parameters we can pass into the agent
    kwargs: dict[str, Any] = {}


class BaseConfig(BaseModel):
    """
    Main config. should be inside of `config/<benchmark>.yaml`
    """

    class ConfigDict:
        extra = "ignore"

    benchmark: str = "base"

    agent: AgentConfig

    environment: EnvironmentConfig

    dataset: DatasetConfig

    @model_validator(mode="before")
    def validate_config(cls, config: dict[str, Any]) -> dict[str, Any]:
        """NOTE: Human readable validation"""
        if "agent" not in config:
            raise ValueError("`agent` is required")

        dataset_name = config.get("dataset", {}).get("name", None)
        if dataset_name is None:
            raise ValueError("`dataset.name` is required")

        return config
