# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install
make install                 # CLI deps in venv
make tracker-install         # Tracker service deps (separate venv)
make update-submodules       # Init git submodules

# Code quality (run before committing)
make style                   # ruff format + ruff check --fix
make typecheck               # basedpyright

# Build
make build                   # PyInstaller binary → dist/

# Local services
make tracker-service         # Start tracker Docker container
```

Tests use pytest with async support:
```bash
uv run pytest tests/
uv run pytest tests/test_foo.py::test_bar   # single test
```

## Architecture

**Agentic Harness** is a benchmark orchestration platform for testing AI agents against standardized benchmarks.

### Key layers

1. **CLI** (`src/agentic_harness/cli/`) — Click-based tool for managing benchmark runs. Communicates with the Tracker service via `tracker_service.py` (HTTP client). Commands are organized into `benchmark` and `agent` groups.

2. **Tracker Service** (`services/tracker/`) — FastAPI backend that orchestrates benchmark runs, manages task lifecycle (PENDING → BUILDING → IN_PROGRESS → EVALUATING → FINISHED/ERROR), stores artifacts in S3, and interfaces with Daytona for sandbox provisioning.

3. **Agent Contracts** (`agents/`) — Each agent subdirectory contains a `contract.py` implementing `BaseAgentContract` (defined in `src/agentic_harness/contract.py`). The CLI bundles the contract and uploads it to the tracker to run in sandboxes.

4. **Infrastructure** (`infra/`) — AWS CDK stacks for ECS-deployed tracker service.

### Agent contract pattern

Every agent must implement `BaseAgentContract` and export `contract = MyAgentContract()`:

```python
from agentic_harness.contract import BaseAgentContract

class MyAgentContract(BaseAgentContract):
    @property
    def name(self) -> str: ...

    @property
    def install_cmd(self) -> str: ...        # runs once in sandbox

    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict) -> str:
        # Return shell command; use {problem_statement_path} and {task_id} as-is
        ...

    @property
    def final_output(self) -> str: ...       # path to collect for S3

    # Optional overrides
    @property
    def artifacts(self) -> list[str]: ...    # files to bundle (relative to agent dir)

    @property
    def secrets(self) -> dict[str, str]: ...  # {ENV_VAR: aws_secret_name} resolved at runtime

contract = MyAgentContract()
```

The `artifacts` are bundled by `cli/bundler.py` into a zip and uploaded. The placeholder strings `{problem_statement_path}` and `{task_id}` in `run_cmd` are substituted at execution time — do not transform them.

The `secrets` property maps environment variable names to AWS Secrets Manager secret names. The tracker resolves these references at sandbox creation time — raw secret values are never sent from the CLI or stored in the database.

### Configuration

Key environment variables:
- `TRACKER_SERVICE_URL` — tracker backend (default: `https://benchmark-tracker.vals.ai`)

### Code style

- Python 3.12+, managed with `uv`
- Line length: 120, double quotes
- `basedpyright` in strict mode (contracts excluded from type checking)
- `ruff` for formatting and linting

### Versioning

Commit message tags control automatic release bumps on the `dev` branch:
- No tag → patch bump
- `#minor` → minor bump
- `#major` → major bump