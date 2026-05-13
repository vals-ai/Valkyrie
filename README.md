# Valkyrie

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/vals-ai/Valkyrie)
[![Test Coverage](https://codecov.io/gh/vals-ai/Valkyrie/branch/dev/graph/badge.svg)](https://codecov.io/gh/vals-ai/Valkyrie)
[![Doc Coverage](https://vals-ai.github.io/Valkyrie/docstr-coverage.svg)](https://github.com/vals-ai/Valkyrie)

Benchmark orchestration platform for testing AI agents against standardized benchmarks.

> **Note:** The CLI is named **Valkyrie**. You can invoke it using either `valkyrie` or the shorter alias `valk`. For example: `valkyrie run start` or `valk run start`.

## Pre requisites

Valkyrie supports **hosted** and **self-hosted** modes. Both require your own AWS credentials. See [Hosted vs Self-Hosted Mode](docs/HOSTED_MODE.md) for full details.

- AWS account with S3, CloudWatch, and Secrets Manager access
- S3 bucket for storing benchmark artifacts and agents
- API key for sandbox provider (Daytona). [Setup docs](docs/PROVIDER.md)
- **Hosted mode only:** Descope API key (provided by Vals)

## Installation

```bash
uv tool install git+https://github.com/vals-ai/Valkyrie@prod
```

## Configuration

```bash
valkyrie config init
```

This will prompt you to choose between **hosted** and **self-hosted** mode, then collect the required credentials. See [Hosted vs Self-Hosted Mode](docs/HOSTED_MODE.md) for detailed setup instructions.

To upsert a single key:

```bash
valkyrie config set <KEY> <VALUE>
```

## Agent Management

Before running benchmarks, you need to install and upload agents to Valkyrie. These commands manage agent lifecycle. All agents are installed inside of the S3 bucket provided by `valkyrie config init` at `agents/`.

All agents will need to already be configured to work with Valkyrie. Please reference the [contract documentation](docs/CONTRACTS.md) to learn more.

### Install an agent from GitHub

```bash
valkyrie agent install https://github.com/user/my-agent
valkyrie agent install https://github.com/user/my-agent --name my-custom-name
```

Clones an agent repository from GitHub, bundles it, and pushes it to your S3 bucket.

| Option | Description |
| --- | --- |
| `--name, -n` | Agent name (defaults to repository name) |

### Push a local agent to S3

```bash
valkyrie agent push ./agents/sweagent
valkyrie agent push ./agents/sweagent --name my-agent
```

Uploads an agent on your local machine to S3.

| Option | Description |
| --- | --- |
| `--name, -n` | Agent name (defaults to directory name) |

### List installed agents

```bash
valkyrie agent list
```

View all installed agents with date and time last modified. Supports paginated navigation ([h] previous, [l] next, [q] quit).

### Remove an agent

```bash
valkyrie agent remove sweagent
```

Removes an agent from the S3 bucket. Cannot be reversed, will be requested to confirm before deleting.

### Download an agent

```bash
valkyrie agent download sweagent
valkyrie agent download sweagent -o ./agents
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
valkyrie config service set swebench https://my-tunnel.ngrok.io
valkyrie config service set external-service https://endpoint
```

Creates or updates a benchmark service. This maps the benchmark name to the endpoint we can reach it at. This will override any service that we already provide.

### List custom benchmark services

```bash
valkyrie config service list
```

Displays all custom benchmark services in a paginated table. Supports navigation ([h] previous, [l] next, [q] quit).

### Remove a custom benchmark service

```bash
valkyrie config service remove swebench
```

Removes a custom benchmark service.

## Authentication & Custom Headers

Benchmark services may require authentication. Valkyrie stores per-benchmark credentials and sends them as the `Authorization` header automatically. You can also pass arbitrary headers at runtime with `-H`.

### Managing auth credentials

```bash
# Store a credential — sent as the Authorization header on every request to that benchmark
valkyrie config auth set <benchmark-name> <credential>

# List stored credentials (values are masked)
valkyrie config auth list

# Remove a stored credential
valkyrie config auth remove <benchmark-name>
```

Credentials are saved in `~/.config/valkyrie/valkyrie.yaml` under `benchmark_auth`:

```yaml
benchmark_auth:
  swebench: "Bearer sk-my-secret-token"
  finance: "my-api-key"
```

### Runtime headers

Pass additional headers to the benchmark service with `-H` / `--header`. Each flag takes a name and value. Repeatable. These are merged with any stored auth credential — if you pass `-H Authorization <value>` it overrides the stored one for that run.

```bash
valkyrie run start --benchmark my-benchmark --agent sweagent \
  -H X-Custom-Header my-value \
  -H X-Another-Header another-value
```

## Slack Notifications

Valkyrie can send Slack webhook notifications as benchmark runs progress. Store an AWS Secrets Manager secret name (pointing to your Slack webhook URL) and get notified automatically when runs hit defined thresholds or reach a terminal state (finished, error, stopped).

### Setting up the webhook

```bash
# Store the AWS secret name containing your Slack webhook URL
valkyrie config set webhook SLACK_WEBHOOK_SECRET

# Remove the webhook secret
valkyrie config remove webhook
```

The secret in AWS Secrets Manager should contain the raw Slack webhook URL as a plain string (not JSON).

### Starting a run with notifications

```bash
valkyrie run start --agent sweagent --benchmark swebench -i 25 -i 75
```

| Flag | Description |
| --- | --- |
| `-i` / `--interval` | Progress percentage threshold for a notification. Repeatable. Max 3, must be divisible by 5, range 5–100 |

If a webhook secret is configured but no `-i` flags are provided, Valkyrie defaults to `-i 100` (notify on completion only). If `-i` flags are provided but no webhook secret is configured, the intervals are ignored with a warning.

Webhook configuration is persisted per-benchmark in the database. On resume or retry, the webhook secret and intervals are read from the original benchmark — no local config needed.

### Notification triggers

| Trigger | Description |
| --- | --- |
| **In Progress** | Run has crossed a defined interval threshold |
| **Finished** | All tasks within the benchmark have completed (includes final score) |
| **Error** | Run has errored out |
| **Stopped** | User has stopped the run |

## Usage

### Start a run

```bash
valkyrie run start \
  --agent sweagent \
  --benchmark swebench \
  --model anthropic/claude-sonnet-4-6 \
  --concurrency 5 \
  --dataset default \
  -s ANTHROPIC_API_KEY devEvalInfraAnthropicKey \
  -k temperature 1 \
  -H X-Custom-Header my-value \
  --task-ids "task_1,task_2" \
  --slice "0:10" \
  -i 25 -i 75 \
  --ignore-custom-services
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
| `--task-ids-file` | Local path or http(s) URL to a text file with one task ID per line |
| `--slice` | Slice the benchmark dataset (`start:stop:step`) |
| `--dataset` | Dataset variant to run from the benchmark service. A single benchmark can expose multiple datasets (e.g. `default`, `test`, `validation`, `train`, `lite`) representing different task splits or difficulty levels. Defaults to `default` |
| `-H` / `--header` | Custom header for benchmark service requests as `NAME VALUE`. Repeatable. See [Authentication & Custom Headers](#authentication--custom-headers) |
| `-i` / `--interval` | Progress percentage threshold for Slack notification. Repeatable. Max 3, must be divisible by 5, range 5–100. See [Slack Notifications](#slack-notifications) |
| `--ignore-custom-services` / `--ics` | Ignore custom benchmark services that have been configured. Provides opt-out for custom services. |

### Monitor a run

```bash
# Stream live updates
valkyrie run fetch <id> --connect

# One-time status check
valkyrie run fetch <id>
```

### Download results

```bash
# Download to disk (default: ./<benchmark>.json)
valkyrie run results <id> --path ./results.json

# Upload to S3
valkyrie run results <id> --s3

# Score over a task-id subset (recomputed via the benchmark service;
# stored full results are unchanged)
valkyrie run results <id> --task-ids task_1,task_2
valkyrie run results <id> --task-ids-file https://example.com/subset.txt
```

| Option | Description |
| --- | --- |
| `--path` | Local path to save results (default: `./<benchmark>.json`) |
| `--s3` | Upload to S3 instead of downloading. With `--task-ids` / `--task-ids-file` the subset view overwrites the canonical S3 key — re-run without filters to restore |
| `--task-ids` | Comma-separated task IDs to score the subset over (recomputes `final_score` over the filtered set) |
| `--task-ids-file` | Local path or http(s) URL to a text file with one task ID per line |

### Stop a run

```bash
valkyrie run stop <id>

# Force stop all in-flight tasks immediately
valkyrie run stop <id> --force
```

### Resume / Retry a run

```bash
# Resume pending tasks
valkyrie run resume <id>

# Retry errored tasks
valkyrie run retry <id>

# Override concurrency on resume (works on retry)
valkyrie run resume <id> --concurrency 20
```

| Option | Description |
| --- | --- |
| `--concurrency` | Override concurrency level |
| `--task-ids` | Comma-separated task IDs to resume/retry. Any id without an existing row is created as fresh `PENDING` if valid in the current dataset — lets you grow scope without starting a new run. |
| `--task-ids-file` | Local path or http(s) URL to a text file with one task ID per line |
| `--update-agent, -u` | Refresh the frozen agent copy from the current `agents/<name>.zip` in S3 before resuming |
| `--from-scratch` | Clear stored eval resume state and rerun generation for retried tasks |

### List runs

```bash
valkyrie run list \
  --agent-name claude_code \
  --benchmark-name swebench \
  --status IN_PROGRESS \
  --order-by DESC
```

Status options: `IN_PROGRESS`, `STOPPING`, `STOPPED`, `FINISHED`, `ERROR`. Supports paginated navigation ([h] previous, [l] next, [q] quit).

### Download agent outputs

```bash
# Download all task outputs for a run
valkyrie agent outputs <id> --output-dir ./outputs

# Download specific tasks (comma-separated)
valkyrie agent outputs <id> --task-ids astropy__astropy-7606,django__django-10880
```

### Download a specific file or folder from a run

```bash
valkyrie agent output <id> [subpath] [-o ./output-dir]
```

| Argument / Option | Description |
| --- | --- |
| `BENCHMARK_ID` | UUID of the benchmark run |
| `SUBPATH` | Optional file or folder within the benchmark directory |
| `-o` / `--output-dir` | Local destination directory (defaults to `./<benchmark_id>`) |

## Documentation

| Topic | Link |
| --- | --- |
| Hosted vs self-hosted | [HOSTED_MODE.md](docs/HOSTED_MODE.md) |
| Local development | [DEVELOPMENT.md](docs/DEVELOPMENT.md) |
| Lambda integration | [LAMBDA_USAGE.md](docs/LAMBDA_USAGE.md) |
| Agent contracts | [CONTRACTS.md](docs/CONTRACTS.md) |
| Tracker service | [TRACKER.md](services/tracker/README.md) |
| Database & migrations | [DATABASE.md](services/tracker/src/tracker/database/README.md) |
| Infrastructure (AWS CDK) | [INFRASTRUCTURE.md](infra/README.md) |
| Sandbox secrets | [PROVIDER.md](docs/PROVIDER.md) |
| Contribute benchmark services | [Create benchmark service](https://github.com/vals-ai/create-benchmark-service) |
