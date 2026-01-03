# Contract Instructions

This guide explains how to create and use agent contracts in the agentic harness.

---

The agent contract allows us to hotswap agent scaffolds between benchmarks. The harness handles the evaluation logic, so you only need to implement the `run` method in your contract. This separation allows us to maintain a clear boundary between agent execution and result evaluation, keeping the harness flexible and generic.

## Contract Interface

Below is the contract interface that you will be implementing:

For examples, see:
- `contracts/claude_code/contract.py`
- `contracts/finance_agent/contract.py`

```python
from abc import ABC, abstractmethod

from model_library.base import QueryResult
from agentic_harness.base.types import Task, AgentConfig


class AgentContract(ABC):
    """
    Agent contract that all submodules must implement.
    This allows us to substitute different agent scaffolds with ease.
    """

    _config: AgentConfig

    def __init__(self, config: AgentConfig):
        self._config = config

    @property
    def config(self) -> AgentConfig:
        return self._config

    @abstractmethod
    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task and return a model response."""
```

## Contract Directory Structure

Each contract should be organized in its own directory under `contracts/`:

```
contracts/
  └── your_contract_name/
      ├── contract.py         # Your contract implementation
      ├── submodule/          # Optional: agent submodules your contract depends on
      │   └── your_agent/
      │       ├── pyproject.toml
      │       └── src/...
      └── setup.sh           # Optional: additional setup steps for your agent
```

### Submodules

If your contract depends on any agent submodules, they should be placed inside a `submodules/` directory within your contract directory. These will be automatically installed when your contract is deployed.

### Setup Script

If your agent requires additional setup beyond installing Python packages (e.g., installing CLI tools, system dependencies, or configuring the environment), create a `setup.sh` file in your contract directory.

Example `setup.sh`:
```bash
#!/bin/bash
set -euo pipefail

echo "Installing Claude Code..."
curl -fsSL https://claude.ai/install.sh | bash
export PATH=$HOME/.local/bin:$PATH

echo "Setup complete!"
```

**Note:** The harness automatically installs the `agentic_harness` package and any Python packages found in the `submodule/` directory. Your `setup.sh` should only contain agent-specific setup steps.
