"""
Entry point where we deserialize the command line arguments sent from the host machine to the sandbox environment.

The agent config is used to create the agent runner which is then used to execute the task.

The work directory is located at `app/workspace` and is where the agent will run all of its code / create files, etc...
"""

import argparse
import asyncio
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from src.base.types import AgentConfig, Task
from src.logger import get_logger
from src.registry import create_agent

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


def cli():
    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")
    parser.add_argument("--config", type=str, required=True, help="Path to the config file")
    parser.add_argument("--task", type=str, required=True, help="Path to the task file")
    args = parser.parse_args()

    agent_config = validate_object(args.config, AgentConfig)
    task = validate_object(args.task, Task)
    agent = create_agent(agent_config)

    asyncio.run(agent.run(task))


def validate_object(serialized_object: str, model: type[T]) -> T:
    """Utility that takes in a serialized object and validates if it is a valid instance of the given model"""
    try:
        return model.model_validate_json(serialized_object)
    except ValidationError as e:
        raise ValueError(f"Serialized object is not a valid {model.__name__} instance: {e}") from e


if __name__ == "__main__":
    cli()
