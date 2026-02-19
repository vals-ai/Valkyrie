## Binary package

Instructions may vary depending on architecture

### Download Binary

1. Select latest release from branch [prod](https://github.com/vals-ai/agentic-harness/tree/prod)
2. Find the [latest release version](https://github.com/vals-ai/agentic-harness/releases)
3. Download binary, currently only `harness-darwin-amd64` is offered.

### Add to path

You may be able to skip steps 1 and 5 if you already have the directory `~/.local/bin` created. If you are using a shell other than zsh you will need to change the paths.

1. Create local bin if it does not exist, `mkdir -p ~/.local/bin`
2. Move installed binary to bin `mv harness-darwin-amd64 ~/.local/bin/harness`
3. Make it an executable `chmod +x ~/.local/bin/harness-darwin-amd64`
4. Add to path `echo 'export PATH="$HOME/.local/bin/:$PATH"' >> ~/.zshrc`
5. Source shell `source ~/.zshrc`

If on Macbook you should run `xattr -d com.apple.quarantine ~/.local/bin/harness-darwin-amd64`

Navigate to [usage](#usage) for a list of commands

## Development

### Tagging

On prod deploy tagging will be automated by including the following in the commit message

- `` (Omitted): Patch bump - ex. v0.4.0 → v0.4.1
- `#minor`: Minor bump - ex. v0.4.1 → v0.5.0
- `#major`: Major bump - ex. v0.5.0 → v1.0.0

### Binary Release

Binary verisons of the harness are released when commits are tagged

- Dev: Must manually tag a commit to release the binary
- Prod: Will automatically be tagged and released

### Deploy

On prod push all changes will be deployed, slack notifications are setup

### Prerequisites

- Python 3.12
- UV package manager (brew install uv)

### Environment Setup

Create `services/tracker/.env` with the following configuration:

```env
# Sandbox credentials
DAYTONA_API_KEY=dtn_5ebxx_xxxx
DAYTONA_API_URL=https://app.daytona.io/api
DAYTONA_TARGET=us

# AWS credentials
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=...

# Where the benchmark service is hosted
BENCHMARK_SERVICE_URL=http://98.xx.xx:8000
```

### Installation

**Harness (CLI)**

```bash
make install
```

Creates `.venv` and installs dependencies for CLI and harness from `pyproject.toml`.

```bash
make update-submodules
```

Install submodules for agents and services

**Harness (Tool)**

```bash
make tool-install
```

Installs an executable into the bin which allows the cli to be ran without the prefix `uv run ...`. Installed using -e, for developing changes will update the executable. `make install` is still required for development. If not added to the path, run `uv tool update-shell` and it will be.

**Services**

Each service maintains its own isolated virtual environment:

- **Tracker service**: `make tracker-service` — Cleans, builds, and runs Docker container

### Usage

#### Start a benchmark

```bash
# With specific task IDs:
harness benchmark start \
  --agent <agent_path> \
  --model <model_key> \
  -k temperature 7 -k max_tokens 1000 \
  --benchmark <benchmark_name> \
  --concurrency 1 \
  --task-ids "task_1_id,task_2_id" \
  --slice "start:stop:step"
```

Flags

```
--agent: Path to the directory with the agent
--model: Model key
--benchmark: Name of the benchmark
--concurrency: The amount of concurrent tasks that are processed concurrently
--slice: Takes a slice from the dataset between the given values "start:stop:step"
--kwarg (-k): Single kwarg that is passed into the agent run command
--task-ids: comma separated list of task ids to run
```

Starts the benchmark and exits once successfully created.

#### Monitor benchmark status

```bash
# Live updates every 60 seconds
harness benchmark fetch --benchmark-id <benchmark_id> --connect

# One-time status check
harness benchmark fetch --benchmark-id <benchmark_id>
```

#### Download results

```bash
harness benchmark results --benchmark-id <benchmark_id>
```

Flags

```
--path: Downloads the final view to disk (default: ./results.json)
--s3: Download the final view to s3, found in bucket://benchmarks/benchmark_id/results.json
```

#### Stop a benchmark

```bash
harness benchmark stop --benchmark-id <benchmark_id>
```

Flags

```
--force: Force stops all tasks in progress or evaluating (default: false)
```

#### Resume a benchmark

```bash
harness benchmark resume --benchmark-id <benchmark_id>
```

#### Retry a benchmark

```bash
harness benchmark retry --benchmark-id <benchmark_id>
```

Flags

```
--retry: Retry tasks with the status `error`
--force task_1 task_2: force retry tasks with the specified space separated ids (the retry flag is separate and not required to use this flag)
--concurrency: change the previous concurrency when starting the benchmark, will be modified for all future runs
```

#### List and filter benchmarks

```bash
harness benchmark list --agent-name <agent_name> --benchmark-name <benchmark_name> --status <benchmark_status> --order-by <preferred_order>
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

```bash
harness agent outputs --benchmark-id <benchmark_id> --output-dir <download_directory>
```

Flags

```
--output-dir: where you would like the agent outputs to be downloaded (default: ./agent_outputs/<benchmark-id>)
```

### Supported Benchmarks

- SWE-bench
