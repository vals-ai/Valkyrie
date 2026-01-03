from typing import Any

from model_library.base import InputItem
from pydantic import BaseModel, model_validator
from wonderwords import RandomWord

rw = RandomWord()


class Task(BaseModel):
    """
    Represents one task.

    id field represents the run_id for platform benchmarks.
    """

    id: str
    input: list[InputItem]
    extra: dict[str, Any] = {}


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

    @model_validator(mode="before")
    def validate_config(cls, config: dict[str, Any]) -> dict[str, Any]:
        """NOTE: Human readable validation"""
        if "agent" not in config:
            raise ValueError("`agent` is required")

        dataset_name = config.get("dataset", {}).get("name", None)
        if dataset_name is None:
            raise ValueError("`dataset.name` is required")

        return config
