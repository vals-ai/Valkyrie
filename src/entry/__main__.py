import argparse
import asyncio
import json
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from src.base.types import BaseConfig, Task
from src.registry import create_agent

T = TypeVar("T", bound=BaseModel)


async def main(base_config: BaseConfig, task: Task):
    agent = create_agent(base_config)

    await agent.run(task)


def validate_object(serialized_object: str, model: type[T]) -> T:
    """Utility that takes in a serialized object and validates if it is a valid instance of the given model"""
    try:
        return model.model_validate_json(json.loads(serialized_object))
    except ValidationError as e:
        raise ValueError(f"Serialized object is not a valid {model.__name__} instance: {e}") from e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")
    parser.add_argument("--config", type=str, required=True, help="Path to the config file")
    parser.add_argument("--task", type=str, required=True, help="Path to the task file")
    args = parser.parse_args()

    base_config = validate_object(args.config, BaseConfig)
    task = validate_object(args.task, Task)

    asyncio.run(main(base_config, task))
