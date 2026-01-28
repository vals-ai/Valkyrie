## Development

### Prerequisites

- Python 3.12
- UV package manager (brew install uv)

### Environment Setup

Create `services/tracker/.env` with the following configuration:

```env
DAYTONA_API_KEY=dtn_5ebxx_xxxx
DAYTONA_API_URL=https://app.daytona.io/api
DAYTONA_TARGET=us
BENCHMARK_SERVICE_URL=http://98.xx.xx:8000
```

### Installation

**Harness (CLI)**

```bash
make install
```

Creates `.venv` and installs dependencies for CLI and harness from `pyproject.toml`.

**Services**

Each service maintains its own isolated virtual environment:

- **Tracker service**: `make tracker-service` — Cleans, builds, and runs Docker container
- **SWE-bench service**: `make swebench-install` — Creates `services/benchmarks/swebench/.venv`

### Usage

#### Start a benchmark

```bash
# With specific task IDs:
uv run harness start-benchmark \
  --contract <contract_path> \
  --benchmark <benchmark_name> \
  --concurrency 1 \
  --task-ids "task_1_id,task_2_id" \
  --slice "start:stop:step"

# Or run whole benchmark (not recommended for development):
uv run harness start-benchmark \
  --contract <contract_path> \
  --benchmark <benchmark_name> \
  --concurrency 1 \
  --slice "start:stop:step"
```

Starts the benchmark and exits once successfully created.

#### Monitor benchmark status

```bash
# Live updates every 60 seconds
uv run harness fetch-benchmark --benchmark-id <benchmark_id> --connect

# One-time status check
uv run harness fetch-benchmark --benchmark-id <benchmark_id>
```

#### Download results

```bash
uv run harness retrieve-results --benchmark-id <benchmark_id> --path ./results.json
```

#### Stop a benchmark

```bash
uv run harness stop-run --benchmark-id <benchmark_id>
```

Flags

```
--force: Force stops all tasks in progress or evaluating (default: false)
```

#### Resume a benchmark

```bash
uv run harness resume-run --benchmark-id <benchmark_id>
```

Flags

```
--retry: Retry tasks with the status `error`
```

#### List and filter benchmarks

```bash
uv run harness fetch-benchmarks --contract-name <contract_name> --benchmark-name <benchmark_name> --status <benchmark_status> --order-by <preferred_order>
```

```
# Status Options (Case insensitive)
> IN_PROGRESS
> STOPPING
> STOPPED
> FINISHED
> ERROR

# Order by options based off when the benchmark was started (Case insensitive)
> DESC - default
> ASC
```

### Supported Benchmarks

- SWE-bench
