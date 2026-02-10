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

**Harness (Tool)**

```bash
make tool-install
```

Installs an executable into the bin which allows the cli to be ran without the prefix `uv run ...`. Installed using -e, for developing changes will update the executable. `make install` is still required for development.

**Services**

Each service maintains its own isolated virtual environment:

- **Tracker service**: `make tracker-service` — Cleans, builds, and runs Docker container
- **SWE-bench service**: `make swebench-install` — Creates `services/benchmarks/swebench/.venv`

### Usage

!!!! If installed with `make tool-install`, the prefix `uv run ...` is not nessecary, remove it and run from just `harness ...`. Confirm installation works using `harness --help`. If it was not added to the path, run `uv tool update-shell` and it will be.

#### Start a benchmark

```bash
# With specific task IDs:
uv run harness start-benchmark \
  --agent <agent_path> \
  --benchmark <benchmark_name> \
  --concurrency 1 \
  --task-ids "task_1_id,task_2_id" \
  --slice "start:stop:step"

# Or run whole benchmark (not recommended for development):
uv run harness start-benchmark \
  --agent <agent_path> \
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
uv run harness stop-benchmark --benchmark-id <benchmark_id>
```

Flags

```
--force: Force stops all tasks in progress or evaluating (default: false)
```

#### Resume a benchmark

```bash
uv run harness resume-benchmark --benchmark-id <benchmark_id>
```

#### Retry a benchmark

```bash
uv run harness retry-benchmark --benchmark-id <benchmark_id>
```

Flags

```
--retry: Retry tasks with the status `error`
--force task_1 task_2: force retry tasks with the specified space separated ids (the retry flag is separate and not required to use this flag)
```

#### List and filter benchmarks

```bash
uv run harness fetch-benchmarks --agent-name <agent_name> --benchmark-name <benchmark_name> --status <benchmark_status> --order-by <preferred_order>
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

#### Download all agent outputs from benchmark

```
uv run harness fetch-agent-outputs --benchmark-id <benchmark_id> --output-dir <download_directory>
```

Flags

```
--output-dir: where you would like the agent outputs to be downloaded (default: ./agent_outputs/<benchmark-id>)
```

### Supported Benchmarks

- SWE-bench
