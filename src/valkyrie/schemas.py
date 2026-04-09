from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ValidationError, create_model, field_validator

from valkyrie.cli.exceptions import ContractValidationError


class Defaults(str, Enum):
    """Values auto injected into users run cmd if specified"""

    PROBLEM_STATEMENT_PATH = "problem_statement_path"
    TASK_ID = "task_id"


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str | None = None
    """Model key (e.g., openai/gpt-4o)"""

    kwargs: dict[str, Any] = {}
    """Additonal arguments we want to pass into the agent"""


ParameterType = Literal["float", "bool", "dict", "int", "str"]


class Parameter(BaseModel):
    """
    A single parameter that needs to be validated when passed into the agent,
    contains all required values to construct a parameter object.

    ```yaml
    temperature:
      type: float
      default: 0.7
      description: "Sampling temperature"
    ```
    """

    type: ParameterType
    """
    The type of the parameter
    ```yaml
    temperature:
      type: float
    ```
    """

    required: bool
    """
    Whether the parameter is required
    ```yaml
    temperature:
      required: false
    ```
    """

    default: float | bool | dict[str, Any] | int | str | None = None
    """
    The default value of the parameter
    ```yaml
    temperature:
      default: 0.7
    ```
    """

    choices: list[str] | None = None
    """
    A list of valid choices for the parameter. When provided, the value is
    validated as a Literal type — any value not in the list will be rejected.
    ```yaml
    mode:
      type: str
      choices: [fast, accurate]
    ```
    """

    description: str | None = None
    """
    A description of the parameter  (Optional)
    ```yaml
    temperature:
      description: "Sampling temperature"
    ```
    """


class AgentContract(BaseModel):
    """
    Breakdown of the agent contract the yaml config file is converted into,
    Examples are provided for the yaml equivalent for clarity.
    """

    name: str
    """
    Name of the Agent, decorative but important

    ```yaml
    name: my_agent
    ```
    """

    install_cmd: str
    """
    Command to install the agent

    ```yaml
    install_cmd: "bash setup.sh"
    ```
    """

    final_output: Path | None = None
    """
    Path from the root of the sandbox to a directory or file \
        that should be saved to S3 after a task has finished running

    ```yaml
    final_output: /artifacts
    ```
    """

    secrets: dict[str, str] = {}
    """
    Secrets for the agent

    ```yaml
    secrets:
      ANTHROPIC_API_KEY: AnthropicKey
      OPENAI_API_KEY: OpenAIKey
    ```
    """

    defaults: dict[str, Parameter] = {}
    """
    Pre-defined parameters with configurable required/default behavior.
    Merged into kwargs at validation time.

    ```yaml
    defaults:
      model:
        type: str
        required: true
    ```
    """

    kwargs: dict[str, Parameter] = {}
    """
    Additional parameters for the agent

    ```yaml
    kwargs:
      temperature:
        type: float
        default: 0.7
        description: "Sampling temperature"
    ```
    """

    run_cmd: str
    """
    Command to run the agent

    ```yaml
    run_cmd: "my_agent --model {model} --task {problem_statement_path} \
--temperature {temperature} --max-tokens {max_tokens}"
    ```
    """

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
