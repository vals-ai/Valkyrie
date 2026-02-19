# Agent Contracts

This guide explains how to create agent contracts for the agentic harness.

## Overview

An agent contract defines how to install and run an agent in a sandbox environment. The harness handles bundling, deployment, and evaluation - you just need to specify how your agent is set up and executed.

## Contract Definition

Create a `contract.py` file in your agent directory that defines a contract class inheriting from `BaseAgentContract`:

```python
import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Any

from agentic_harness.contract import BaseAgentContract
from agentic_harness.schemas import AgentConfig

load_dotenv()


class MyAgentContract(BaseAgentContract):
    """My Agent Contract"""

    @property
    def name(self) -> str:
        return "my_agent"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh", "submodule/my_agent"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def final_output(self) -> Path | str:
        return Path("path/to/output")

    @property
    def env(self) -> dict[str, str]:
        return {"API_KEY": os.getenv("API_KEY")}

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        return f"my_agent --task {problem_statement_path}"


# Export the contract class
contract = MyAgentContract
```

## Required Properties

Your contract class must implement these abstract properties:

### `name: str`

The name of your agent contract.

```python
@property
def name(self) -> str:
    return "my_agent"
```

### `install_cmd: str`

Command to install the agent and its dependencies. Runs once during sandbox setup with the working directory set to `/bundle/<agent_name>/`.

```python
@property
def install_cmd(self) -> str:
    return "bash setup.sh"
```

### `final_output: Path | None`

Path to the file with the final output you want to parse. The artifact found here will be copied into the corresponding s3 bucket that can be found at benchmark/benchmark_id/task_id/

Supported path could be to a directory or a file. Will be copied as a tar file

```python
@property
def final_output(self) -> Path | None:
    return Path("/absolute/path/to/output")
```

### `run_cmd(problem_statement_path, task_id, extra_args) -> str`

Method to build the shell command that runs the agent on a task. The harness calls this method at serialization time with literal placeholder strings — `problem_statement_path` will be `"{problem_statement_path}"` and `task_id` will be `"{task_id}"`. The tracker substitutes the real values at runtime just before executing the command in the sandbox.

> **Do not splice, slice, or transform `problem_statement_path` or `task_id`.** Use them only as-is inside an f-string or string concatenation. If you manipulate the strings (e.g. `problem_statement_path.split("/")[-1]`) the placeholder will be destroyed and the tracker will not be able to substitute the real value.

```python
@override
def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
    return f"my_agent --task {problem_statement_path}"
```

## Optional Properties

### `artifacts: list[str]`

Files and directories to bundle with the agent (default: empty list). Paths are relative to your agent directory.

```python
@property
def artifacts(self) -> list[str]:
    return ["setup.sh", "submodule/my_agent", "config/settings.yaml"]
```

### `env: dict[str, str]`

Environment variables required by the agent (default: empty dict). Load secrets from your local environment.

```python
@property
def env(self) -> dict[str, str]:
    return {
        "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
    }
```

## Using AgentConfig for Dynamic Configuration

The `AgentConfig` parameter allows you to pass runtime configuration (like model selection) from the CLI to your agent:

```python
class MyAgentContract(BaseAgentContract):
    @abstractmethod
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        # Make agent_config required by removing the default None
        if not agent_config:
            raise ValueError("AgentConfig is required")

        # Use the model from agent_config
        model = self._agent_config.model
        return f"my_agent --model {model} --task {{problem_statement}}"
```

Then run from the CLI with:

```bash
harness start-benchmark --agent agents/my_agent --model openai/gpt-4o --benchmark swebench
```

## Directory Structure

```
agents/
  my_agent/
    contract.py           # Contract definition (required)
    setup.sh              # Installation script (optional)
    submodule/           # Agent code and dependencies (optional)
      my_agent/
        pyproject.toml
        main.py
```

## Installation Scripts

The `install_cmd` runs inside the sandbox with the working directory set to `/bundle/<agent_name>/`. Use it to install dependencies and set up your agent.

Example `setup.sh`:

```bash
#!/bin/bash
set -euo pipefail

# Install CLI tools
curl -fsSL https://example.com/install.sh | bash

# Add to PATH if needed
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc

# Install Python dependencies
cd submodule/my_agent && uv sync
```

## Creating Wrapper Scripts

If your agent requires a virtual environment or specific setup before running, create a wrapper script in `/usr/local/bin/` during installation:

```bash
# In setup.sh
cat > /usr/local/bin/my_agent << 'WRAPPER'
#!/bin/bash
source /bundle/my_agent/submodule/my_agent/.venv/bin/activate
exec python /bundle/my_agent/submodule/my_agent/main.py "$@"
WRAPPER
chmod +x /usr/local/bin/my_agent
```

Your agent artifacts are bundled to `/bundle/<agent_name>/` in the sandbox.

## Placeholder Syntax

Use **single braces** `{problem_statement}` in your `run_cmd`. The placeholder will be replaced with the actual task prompt at runtime:

```python
@property
def run_cmd(self) -> str:
    # ✅ Correct - single braces
    return "my_agent --task {problem_statement}"

    # ❌ Wrong - double braces (use only in f-strings)
    return f"my_agent --task {{problem_statement}}"
```

If you're using an f-string to include dynamic values, use double braces for the placeholder:

```python
@property
def run_cmd(self) -> str:
    model = self._agent_config.model
    # Double braces in f-string become single braces in output
    return f"my_agent --model {model} --task {{problem_statement}}"
```

## Complete Examples

See these agent implementations for reference:

- `agents/claude_code/` - CLI tool installation
- `agents/sweagent/` - Python agent with dynamic model configuration
