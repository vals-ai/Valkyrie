# Agentic Harness

Define your agent in `agents/`, add benchmarks as git submodules in `benchmarks/`, and run evaluations.

```bash
python runner.py --config config/ioi.yaml
```

**Setup:** `make install`
**Style:** `make style`
**Type Check:** `make typecheck`

## Virtual Environment Setup

This project uses separate virtual environments for different components:

- **Root workspace** (`.venv`): Contains the main harness framework and shared dependencies
  - Install with: `make install`
- **Services** (isolated venvs): Each service maintains its own virtual environment. You might have to switch your venv depending on the service you are working on.
  - Tracker service: `make tracker-install` (creates `services/tracker/.venv`)
  - SWE-bench service: `make swebench-install` (creates `services/benchmarks/swebench/.venv`)

### Running Tracker Service Locally

```bash
# Start tracker service (development mode)
make tracker-dev
```
