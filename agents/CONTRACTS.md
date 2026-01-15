# Agent Contracts

This guide explains how to create agent contracts for the agentic harness.

## Overview

An agent contract defines how to install and run an agent in a sandbox environment. The harness handles bundling, deployment, and evaluation - you just need to specify how your agent is set up and executed.

## Contract Definition

Create a `contract.py` file in your agent directory that exports a `contract` object:

```python
import os
from agentic_harness import AgentContract
from dotenv import load_dotenv

load_dotenv()

contract = AgentContract(
    name="my_agent",
    artifacts=[
        "submodules/my_agent",
        "setup.sh",
    ],
    install_cmd="bash setup.sh",
    run_cmd="my_agent -p {{problem_statement}}",
    env={
        "API_KEY": os.getenv("API_KEY"),
    },
)
```

## Contract Fields

```python
class AgentContract(BaseModel):
    name: str
    """Name of the agent."""

    artifacts: list[str] = []
    """Paths to artifacts."""

    install_cmd: str
    """Command to install the agent."""

    run_cmd: str
    """Command to run the agent."""

    env: dict[str, str] = {}
    """Environment variables required to run the agent."""
```

## Directory Structure

```
agents/
  my_agent/
    contract.py           # Contract definition (required)
    setup.sh              # Installation script (optional)
    submodules/           # Agent code and dependencies (optional)
      my_agent/
        pyproject.toml
        main.py
```

## Artifacts

The `artifacts` list specifies which files and directories to bundle. Paths are relative to your contract directory.

```python
artifacts=[
    "setup.sh",                    # Single file
    "submodules/my_agent",         # Directory with agent code
    "config/settings.yaml",        # Config files
]
```

## Install Command

The `install_cmd` runs inside the sandbox with the working directory set to your contract folder. Use it to install dependencies and set up your agent.

```python
install_cmd="bash setup.sh"
```

Example `setup.sh`:
```bash
#!/bin/bash
set -euo pipefail

# Install CLI tools
curl -fsSL https://example.com/install.sh | bash

# Install Python dependencies
cd submodules/my_agent && uv sync
```

## Run Command

The `run_cmd` specifies how to execute your agent. Use `{{problem_statement}}` as a placeholder - it will be replaced with the actual task prompt.

```python
run_cmd="my_agent -p {{problem_statement}}"
```

The command runs inside the sandbox. If your agent needs a virtual environment, create a wrapper script during installation.

## Creating Wrapper Scripts

If your agent requires a virtual environment or specific setup before running, create a wrapper script in `/usr/local/bin/` during installation:

```bash
# In setup.sh
cat > /usr/local/bin/my_agent << 'WRAPPER'
#!/bin/bash
source $CONTRACT_DIR/submodules/my_agent/.venv/bin/activate
exec python $CONTRACT_DIR/submodules/my_agent/main.py "$@"
WRAPPER
chmod +x /usr/local/bin/my_agent
```

The `$CONTRACT_DIR` environment variable points to your contract's location in the sandbox (e.g., `/bundle/my_agent`).

## Environment Variables

The `env` field passes environment variables to the sandbox. Load secrets from your local environment:

```python
env={
    "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
    "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
}
```

## Complete Example

See `agents/claude_code/` for a complete example:

```
agents/claude_code/
  contract.py
  setup.sh
  submodules/
    claude_code/
      pyproject.toml
      main.py
```
