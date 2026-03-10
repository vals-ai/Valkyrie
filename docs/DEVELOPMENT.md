# Development

Local development guide for the Agentic Harness.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager (`brew install uv`)

### Environment

Add inside of `.env`

```env
TRACKER_SERVICE_URL=http://localhost:8000
```

## Installation

### CLI

```bash
make install
```

Creates `.venv` and installs dependencies from `pyproject.toml`.

### Submodules

```bash
make update-submodules
```

Initializes git submodules for agents and services.

### Install as a tool globally

```bash
make tool-install
```

Installs `harness` as a standalone executable so you can run it without the `uv run` prefix. Uses editable install so code changes take effect immediately. If not added to your PATH, run `uv tool update-shell`.

### Tracker service

```bash
make tracker-service   # Build and run Docker container
```

The service will be available at `http://localhost:8000`.

## Environment Setup

### CLI (harness config)

The CLI reads credentials from `~/.config/harness/harness.yaml`. Run `harness config init` to create it.

```bash
harness config init
```

## Code Quality

```bash
make style       # ruff format + ruff check --fix
make typecheck   # basedpyright (strict mode)
```

## Versioning

Commit message tags control automatic release bumps on the `dev` branch:

| Tag | Effect | Example |
| --- | --- | --- |
| _(none)_ | Patch bump | v0.4.0 -> v0.4.1 |
| `#minor` | Minor bump | v0.4.1 -> v0.5.0 |
| `#major` | Major bump | v0.5.0 -> v1.0.0 |

## Releases

Binary versions are released when commits are tagged:

- **Dev**: Must manually tag a commit to trigger a release
- **Prod**: Automatically tagged and released on push

## Documentation

| Topic | Link |
| --- | --- |
| Lambda integration | [LAMBDA_USAGE.md](LAMBDA_USAGE.md) |
| Agent contracts | [CONTRACTS.md](CONTRACTS.md) |
| Tracker service | [TRACKER.md](../services/tracker/README.md) |
| Database & migrations | [DATABASE.md](../services/tracker/src/tracker/database/README.md) |
| Infrastructure (AWS CDK) | [INFRASTRUCTURE.md](../infra/README.md) |
