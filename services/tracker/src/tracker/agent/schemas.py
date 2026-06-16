"""Agent contract schemas (declarative YAML contract + AgentConfig)."""

from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ValidationError, create_model, field_validator

from tracker.database.models import OutputArtifact, OutputArtifactSpec
from tracker.exceptions import ContractValidationError


__all__ = ["AgentConfig", "AgentContract", "OutputArtifact", "OutputArtifactSpec", "Parameter"]


class Defaults(str, Enum):
    """Values auto injected into users run cmd if specified"""

    PROBLEM_STATEMENT_PATH = "problem_statement_path"
    TASK_ID = "task_id"


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str | None = None
    """Model key (e.g., openai/gpt-4o)"""

    kwargs: dict[str, Any] = {}
    """Additional arguments to pass into the agent"""


ParameterType = Literal["float", "bool", "dict", "int", "str"]


class Parameter(BaseModel):
    type: ParameterType
    required: bool
    default: float | bool | dict[str, Any] | int | str | None = None
    choices: list[str] | None = None
    description: str | None = None


class AgentContract(BaseModel):
    """Declarative YAML agent contract."""

    name: str
    install_cmd: str
    final_output: Path | None = None
    output_artifacts: list[OutputArtifactSpec] = []
    secrets: dict[str, str] = {}
    ingest_lambda: str | None = None
    defaults: dict[str, Parameter] = {}
    kwargs: dict[str, Parameter] = {}
    run_cmd: str

    @field_validator("run_cmd")
    @classmethod
    def validate_run_cmd(cls, v: str) -> str:
        if f"{{{Defaults.PROBLEM_STATEMENT_PATH.value}}}" not in v:
            raise ValueError(f"run_cmd must contain {{{Defaults.PROBLEM_STATEMENT_PATH.value}}}")

        return v

    def format_run_cmd(self, kwargs: dict[str, Any]) -> str:
        reference = self.run_cmd

        defaults = {f"{{{value.value}}}": f"{{{value.value}}}" for value in Defaults}

        for name, value in {**defaults, **kwargs}.items():
            reference = reference.replace(f"{{{name}}}", str(value))

        return reference

    def validate_kwargs(self, schema: dict[str, Parameter], values: dict[str, Any]) -> dict[str, Any]:
        if not schema:
            return {}

        type_map = {"float": float, "int": int, "bool": bool, "str": str, "dict": dict}

        fields: dict[Any, Any] = {}
        for name, param in schema.items():
            if param.choices:
                python_type = Literal[tuple(param.choices)]  # type: ignore[valid-type]
            else:
                python_type = type_map[param.type]
            default = param.default if not param.required else ...
            fields[name] = (python_type, default)

        DynamicModel = create_model("KwargsModel", **fields)

        try:
            validated = DynamicModel(**values)
        except ValidationError as e:
            raise ContractValidationError(e) from e

        return validated.model_dump(exclude_none=True)
