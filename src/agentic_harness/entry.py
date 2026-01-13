"""Entry point to run the agent inside the sandbox."""

import argparse
import asyncio
from pathlib import Path

from dotenv import load_dotenv

from agentic_harness.base.contract import AgentContract
from agentic_harness.base.types import AgentConfig, Task, TextInput
from agentic_harness.base_agent import AgentRunner


def get_contract(contract_path: Path) -> AgentContract:
    """
    Dynamically import and instantiate the AgentContract from the contract directory.

    Assumes the contract has a contract.py file with an AgentContract subclass.

    Args:
        contract_path: Path to the contract directory (e.g., /bundle/contracts/claude_code)

    Returns:
        An instance of the AgentContract subclass found in the contract

    Raises:
        FileNotFoundError: If contract.py doesn't exist
        ImportError: If the module cannot be loaded
        ValueError: If no AgentContract subclass is found
    """
    import importlib.util
    import sys

    # TODO: Pass in an actual AgentConfig
    agent_config = AgentConfig(name=contract_path.stem)

    # Path to contract.py inside the contract directory
    contract_module_path = contract_path / "contract.py"

    if not contract_module_path.exists():
        raise FileNotFoundError(f"contract.py not found in {contract_path}")

    # Load the module dynamically
    spec = importlib.util.spec_from_file_location("contract", contract_module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {contract_module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["contract"] = module
    spec.loader.exec_module(module)

    # Find the AgentContract subclass in the module
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and issubclass(obj, AgentContract) and obj is not AgentContract:
            return obj(agent_config)  # Instantiate and return

    raise ValueError(f"No AgentContract subclass found in {contract_module_path}")


def cli():
    parser = argparse.ArgumentParser(description="Run the agent inside the sandbox.")
    parser.add_argument("--contract", type=str, required=True, help="Path to contract directory")
    parser.add_argument(
        "--problem_statement", type=str, required=True, help="Problem statement that will be passed to the agent"
    )
    args = parser.parse_args()

    contract_path = Path(args.contract)

    load_dotenv(contract_path / "env")

    agent_contract = get_contract(contract_path)
    agent = AgentRunner(agent_contract)

    # TODO: add task id and kwargs or rework the Task object
    task = Task(id="", input=[TextInput(text=args.problem_statement)])
    asyncio.run(agent.run(task))


if __name__ == "__main__":
    cli()
