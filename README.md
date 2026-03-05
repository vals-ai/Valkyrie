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

## Usage

### Start a benchmark

```bash
harness benchmark start \
  --agent agents/sweagent \
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
| `--agent` | Path to the agent directory (e.g. `agents/claude_code`) |
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

Status options: `IN_PROGRESS`, `STOPPING`, `STOPPED`, `FINISHED`, `ERROR`

### Download agent outputs

```bash
harness agent outputs --benchmark-id <id> --output-dir ./outputs
```

## Documentation

| Topic | Link |
| --- | --- |
| Local development | [DEVELOPMENT.md](docs/DEVELOPMENT.md) |
| Lambda integration | [LAMBDA_USAGE.md](docs/LAMBDA_USAGE.md) |
| Agent contracts | [CONTRACTS.md](agents/CONTRACTS.md) |
| Tracker service | [TRACKER.md](services/tracker/README.md) |
| Database & migrations | [DATABASE.md](services/tracker/src/tracker/database/README.md) |
| Infrastructure (AWS CDK) | [INFRASTRUCTURE.md](infra/README.md) |
| Sandbox secrets | [PROVIDER.md](docs/PROVIDER.md) |
| Contribute benchmark services | [Create benchmark service](https://github.com/vals-ai/create-benchmark-service)