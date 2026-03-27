# Agent Contracts

This guide explains how to create agent contracts for Valkyrie.

## Overview

An agent contract defines how to install and run an agent in a sandbox environment. Valkyrie handles bundling, deployment, and evaluation - you just need to specify how your agent is set up and executed.

## Contract Definition

Create a `contract.py` file in your agent directory that defines a contract class inheriting from `BaseAgentContract`:

```python
from pathlib import Path
from typing import Any, override

from valkyrie.contract import BaseAgentContract


class MyAgentContract(BaseAgentContract):
    """My Agent Contract"""

    @property
    def name(self) -> str:
        return "my_agent"

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def secrets(self) -> dict[str, str]:
        return {"API_KEY": "myAwsSecretName"}

    @property
    def final_output(self) -> Path | None:
        return Path("/logs/my_agent")

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        return f"my_agent --task {problem_statement_path}"


# Export the contract class (not an instance)
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

Absolute path to the final output to collect. The artifact found here will be copied into the corresponding S3 bucket at `benchmark/benchmark_id/task_id/`. Can be a directory or a file (copied as a tar).

```python
@property
def final_output(self) -> Path | None:
    return Path("/logs/my_agent")
```

### `run_cmd(problem_statement_path, task_id, kwargs) -> str`

Method that builds the shell command to run the agent on a task. Valkyrie calls this at serialization time with placeholder strings (`"{problem_statement_path}"` and `"{task_id}"`). The tracker substitutes the real values at runtime before executing in the sandbox.

> **Do not transform `problem_statement_path` or `task_id`.** Use them as-is in an f-string or concatenation. If you manipulate the strings (e.g. `problem_statement_path.split("/")[-1]`) the placeholder will be destroyed.

```python
@override
def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
    return f"my_agent --task {problem_statement_path}"
```

## Optional Properties

### `secrets: dict[str, str]`

Default secrets required by the agent (default: empty dict). Maps environment variable names to AWS Secrets Manager secret names. These are resolved at sandbox creation time - raw values are never stored.

```python
@property
def secrets(self) -> dict[str, str]:
    return {"ANTHROPIC_API_KEY": "devEvalInfraAnthropicKey"}
```

Secrets can also be passed (or overridden) at runtime via the CLI:

```bash
valkyrie run start --agent agents/my_agent -s API_KEY myAwsSecretName
```

CLI secrets are merged with contract defaults. If both define the same key, the CLI value wins.

## Using AgentConfig

The `AgentConfig` parameter allows you to pass runtime configuration (like model selection) from the CLI to your agent:

```python
class MyAgentContract(BaseAgentContract):
    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model = self._agent_config.model
        if not model:
            raise ValueError("Model is required. Use --model to specify one.")

        return f"my_agent --model {model} --task {problem_statement_path}"
```

Then run from the CLI with:

```bash
valkyrie run start --agent agents/my_agent --model openai/gpt-4o --benchmark swebench
```

Extra key-value pairs can be passed with `-k` and accessed via `kwargs`:

```bash
valkyrie run start --agent agents/my_agent --benchmark swebench -k temperature 0.7
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

The entire agent directory is bundled to `/bundle/<agent_name>/` in the sandbox (`contract.py` will be excluded).

## Complete Examples

See these agent implementations for reference:

- `agents/claude_code/` - CLI tool installation
- `agents/sweagent/` - Python agent with dynamic model configuration
