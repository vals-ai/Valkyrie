# Agentic Harness

Benchmark orchestration platform for testing AI agents against standardized benchmarks.

## Pre requisites

- AWS account
- S3 bucket created for the purpose of storing all artifacts produced by benchmarks ran
- API key for sandbox provider supported (daytona). [Documentation for setting that up](docs/PROVIDER.md)

## Configuration

```bash
harness config init
```

This will prompt for required credentials (AWS, S3 bucket, Daytona secret name) and write them to `~/.config/harness/harness.yaml`. Values can be sourced from the environment or an existing config. These are required to run the harness and be in any environment that you use the harness in.

To update a single key:

```bash
harness config modify <KEY> <VALUE>
```

## Agent Management

Before running benchmarks, you need to install and upload agents to the harness. These commands manage agent lifecycle. All agents are installed inside of the S3 bucket provided by `harness config init` at `agents/`.

All agents will need to already be configured to work with the agentic harness. Please reference the [contract documentation](docs/CONTRACTS.md) to learn more.

### Install an agent from GitHub

```bash
harness agent install https://github.com/user/my-agent
harness agent install https://github.com/user/my-agent --name my-custom-name
```

Clones an agent repository from GitHub, bundles it, and pushes it to your S3 bucket.

| Option | Description |
| --- | --- |
| `--name, -n` | Agent name (defaults to repository name) |

### Push a local agent to S3

```bash
harness agent push ./agents/sweagent
harness agent push ./agents/sweagent --name my-agent
```

Uploads an agent on your local machine to S3.

| Option | Description |
| --- | --- |
| `--name, -n` | Agent name (defaults to directory name) |

### List installed agents

```bash
harness agent list
```

View all installed agents with date and time last modified. Supports paginated navigation ([h] previous, [l] next, [q] quit).

### Remove an agent

```bash
harness agent remove sweagent
```

Removes an agent from the S3 bucket. Cannot be reversed, will be requested to confirm before deleting.

### Download an agent

```bash
harness agent download sweagent
harness agent download sweagent -o ./agents
```

Downloads an agent from S3 to your local machine and unzips it.

| Option | Description |
| --- | --- |
| `--output-dir, -o` | Output directory for downloaded agent (default: current directory) |

## Custom Benchmark Services

Vals provides a set of hosted benchmark services by default. If you are developing your own benchmark service you will need to add support for that. We provide a set of utilities that allow you to interact with benchmark services outside of the ones that are provided.

If hosting locally please use the [documentation](https://github.com/vals-ai/create-benchmark-service?tab=readme-ov-file#reverse-tunnel-setup) on the reverse tunnel that is needed.

### Set a custom benchmark service

```bash
harness config service set swebench https://my-tunnel.ngrok.io
harness config service set external-service https://endpoint
```

Creates or updates a benchmark service. This maps the benchmark name to the endpoint we can reach it at. This will override any service that we already provide.

### List custom benchmark services

```bash
harness config service list
```

Displays all custom benchmark services in a paginated table. Supports navigation ([h] previous, [l] next, [q] quit).

### Remove a custom benchmark service

```bash
harness config service remove swebench
```

Removes a custom benchmark service.

## Usage

### Start a benchmark

```bash
harness benchmark start \
  --agent sweagent \
  --benchmark swebench \
  --model kimi/kimi-k2.5-thinking \
  --concurrency 5 \
  -s ANTHROPIC_API_KEY devEvalInfraAnthropicKey \
  -k temperature 7 \
  --task-ids "task_1,task_2" \
  --slice "0:10"
```

| Flag | Description |
| --- | --- |
| `--agent` | Agent name from S3 or path to agent directory (e.g., `sweagent` or `./agents/sweagent`). Agents on users machine are automatically uploaded to S3 before the benchmark starts. |
| `--benchmark` | Benchmark name (e.g. `swebench`) |
| `--model` | Model key (e.g. `openai/gpt-4o`) |
| `--concurrency` | Number of concurrent sandbox tasks (default: 5) |
| `-s` / `--secret` | Secret pair as `ENV_VAR aws_secret_name`. Repeatable. Merged with contract defaults (CLI wins on conflict) |
| `-k` / `--kwarg` | Key-value pair passed to the agent run command. Repeatable |
| `--lambda` | AWS Lambda function to invoke after the run completes |
| `--task-ids` | Comma-separated task IDs to run |
| `--task-ids-file` | Path to a text file with one task ID per line |
| `--slice` | Slice the benchmark dataset (`start:stop:step`) |

### Monitor a benchmark

```bash
# Stream live updates
harness benchmark fetch --benchmark-id <id> --connect

# One-time status check
harness benchmark fetch --benchmark-id <id>
```

### Download results

```bash
# Download to disk (default: ./<benchmark>.json)
harness benchmark results --benchmark-id <id> --path ./results.json

# Upload to S3
harness benchmark results --benchmark-id <id> --s3
```

### Stop a benchmark

```bash
harness benchmark stop --benchmark-id <id>

# Force stop all in-flight tasks immediately
harness benchmark stop --benchmark-id <id> --force
```

### Resume / Retry a benchmark

```bash
# Resume pending tasks
harness benchmark resume --benchmark-id <id>

# Retry errored tasks
harness benchmark retry --benchmark-id <id>

# Override concurrency on resume (works on retry)
harness benchmark resume --benchmark-id <id> --concurrency 20
```

### List benchmarks

```bash
harness benchmark list \
  --agent-name claude_code \
  --benchmark-name swebench \
  --status IN_PROGRESS \
  --order-by DESC
```

Status options: `IN_PROGRESS`, `STOPPING`, `STOPPED`, `FINISHED`, `ERROR`. Supports paginated navigation ([h] previous, [l] next, [q] quit).

### Download agent outputs

```bash
harness agent outputs --benchmark-id <id> --output-dir ./outputs
```

## Documentation

| Topic | Link |
| --- | --- |
| Local development | [DEVELOPMENT.md](docs/DEVELOPMENT.md) |
| Lambda integration | [LAMBDA_USAGE.md](docs/LAMBDA_USAGE.md) |
| Agent contracts | [CONTRACTS.md](docs/CONTRACTS.md) |
| Tracker service | [TRACKER.md](services/tracker/README.md) |
| Database & migrations | [DATABASE.md](services/tracker/src/tracker/database/README.md) |
| Infrastructure (AWS CDK) | [INFRASTRUCTURE.md](infra/README.md) |
| Sandbox secrets | [PROVIDER.md](docs/PROVIDER.md) |
| Contribute benchmark services | [Create benchmark service](https://github.com/vals-ai/create-benchmark-service)
