# Valkyrie

[![Paper](https://img.shields.io/badge/Paper-alphaXiv-B31B1B)](https://www.alphaxiv.org/abs/valkyrie)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/vals-ai/Valkyrie)
[![Test Coverage](https://codecov.io/gh/vals-ai/Valkyrie/branch/dev/graph/badge.svg)](https://codecov.io/gh/vals-ai/Valkyrie)
[![Doc Coverage](https://vals-ai.github.io/Valkyrie/docstr-coverage.svg)](https://github.com/vals-ai/Valkyrie)

Valkyrie is an orchestration platform for running agentic benchmarks.


## Hosting Modes

Valkyrie supports **hosted** and **self-hosted** modes. 
- **Self-hosted Mode** requires you to create AWS infrastructure using the IaC provided.
- **Hosted Mode** allows you to use Vals-hosted infrastructure. Reach out to the Vals team for access (contact@vals.ai)

Both modes require you to provide certain credentials and configuration:  
- AWS API Key: Authentication for an AWS account with S3, CloudWatch, and Secrets Manager access (to store benchmarking logs and results)
- S3 Bucket Name: The S3 bucket to be used for storing benchmark artifacts and agents
- Sandbox provider config: named AWS Secrets Manager entries for sandbox providers. [Setup docs](docs/PROVIDER.md)
- **Hosted mode only:** Vals API Key

See [Hosted vs Self-Hosted Mode](docs/HOSTED_MODE.md) for more details.

## Installation

```bash
uv tool install git+https://github.com/vals-ai/Valkyrie@prod
```

## Configuration

> **Note:** The Valkyrie can be invoked using either `valkyrie` or the alias `valk`. For example: `valkyrie run start` or `valk run start`.

```bash
valkyrie config init
```

This will prompt you to choose between **hosted** and **self-hosted** mode, then collect the required credentials. See [Hosted vs Self-Hosted Mode](docs/HOSTED_MODE.md) for detailed setup instructions.

To upsert a single key:

```bash
valkyrie config set <KEY> <VALUE>
```

To configure sandbox providers:

```bash
valkyrie config provider set daytona DaytonaSecrets
valkyrie config provider set modal ModalSecrets
valkyrie config provider default modal
```

The first configured provider is used by default unless you set one with `valkyrie config provider default <name>`. Use `valkyrie run start --provider <name>` to select another configured provider for one run.

## Agent Management

Before running benchmarks, you need to install and upload agents to Valkyrie. These commands manage agent lifecycle. All agents are installed inside of the S3 bucket provided by `valkyrie config init` at `agents/`.

All agents will need to already be in the Valkyrie format. Please reference the [contract documentation](docs/CONTRACTS.md) to learn more.

You can browse Valkyrie-compatible community agents in the [public agent registry](https://github.com/vals-ai/public-agent-registry).

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

## Running Benchmarks 

### Start a run

To run a specific agent on a given benchmark, use the command
```
valkyrie run start --agent <agent id> --benchmark <benchmark id>
```

You can pass `--concurrency` to control the number of tasks that run in parallel, and `--task-ids` or `--slice` to run only a subset of tasks. Specific agents may take additional parameters as well, most commonly, a parameter to set the model. 

To pass secrets to the agent environment, use `-s <ENVIRONMENT_VARIABLE> <AWS SECRET NAME>`. This will map the value stored in AWS SECRET NAME to ENVIRONMENT_VARIABLE inside the agent container.

Here is an example of how to run the first ten tasks of SWE-Bench Verified:
```bash
valkyrie run start \
  --agent sweagent \
  --benchmark swebench \
  --model anthropic/claude-sonnet-4-6 \
  --concurrency 10 \
  --dataset default \
  -s ANTHROPIC_API_KEY devEvalInfraAnthropicKey \
  -k temperature 1 \
  --slice "0:10" \
  --label swebench_sweagent
```

Add `--connect` to stream updates after the run starts:

```bash
valkyrie run start --agent sweagent --benchmark swebench --connect
```

To start three independent runs of the same benchmark:

```console
$ valkyrie run start --agent sweagent --benchmark swebench --count 3
Run ID: <id-1>
Run ID: <id-2>
Run ID: <id-3>
3 / 3 requested runs successfully started.
Track progress: valkyrie run status --ids <id-1>,<id-2>,<id-3>
```

Start requests are sent sequentially, but accepted runs execute independently and may overlap. Approximate simultaneous task pressure and cost can scale with `count × concurrency`. `--connect` is only supported when count is `1`; it is rejected when count is greater than `1`.

Starts are fail-fast: if a request fails, no later requests are attempted. The command exits nonzero and reports confirmed progress with a combined status command. After a transport/server response failure, the latest request's outcome can be unknown; verify with `valkyrie run list`.

| Flag | Description |
| --- | --- |
| `--agent` | Agent name from S3 or path to agent directory (e.g., `sweagent` or `./agents/sweagent`). Agents on users machine are automatically uploaded to S3 before the benchmark starts. |
| `--benchmark` | Benchmark name (e.g. `swebench`) |
| `--model` | Model key (e.g. `openai/gpt-4o`) |
| `--concurrency` | Number of concurrent sandbox tasks (default: 5) |
| `-n` / `--count` | Number of independent runs to start (default: 1; hard maximum: 10) |
| `-s` / `--secret` | Secret pair as `ENV_VAR aws_secret_name`. Repeatable. Merged with contract defaults (CLI wins on conflict) |
| `-k` / `--kwarg` | Key-value pair passed to the agent run command. Repeatable |
| `--lambda` | AWS Lambda function to invoke after the run completes |
| `--task-ids` | Comma-separated task IDs to run |
| `--task-ids-file` | Local path or http(s) URL to a text file with one task ID per line |
| `--slice` | Slice the benchmark dataset (`start:stop:step`) |
| `--dataset` | Dataset variant to run from the benchmark service. A single benchmark can expose multiple datasets (e.g. `default`, `test`, `validation`, `train`, `lite`) representing different task splits or difficulty levels. Defaults to `default` |
| `-l` / `--label` | Label to attach to the run, can filter by when using `valk run list -l <LABEL>`. Can only set one label per run.  |
| `-H` / `--header` | Custom header for benchmark service requests as `NAME VALUE`. Repeatable. See [Authentication & Custom Headers](#authentication--custom-headers) |
| `-i` / `--interval` | Progress percentage threshold for Slack notification. Repeatable. Max 3, must be divisible by 5, range 5–100. See [Slack Notifications](#slack-notifications) |
| `--ignore-custom-services` / `--ics` | Ignore custom benchmark services that have been configured. Provides opt-out for custom services. |
| `--connect` | Stream run updates after the run starts |

### Monitor a run

After starting, use the following commands to check the status of a run: 

```bash
# Stream live updates
valkyrie run fetch <id> --connect

# One-time status check
valkyrie run fetch <id>

# One-time machine-readable snapshot
valkyrie run fetch <id> --format json

# Machine-readable stream (one JSON object per line)
valkyrie run fetch <id> --connect --format jsonl

# Lightweight status for several known run IDs
valkyrie run status --ids <id-1>,<id-2> --format json

# Show stored run and current task error messages
valkyrie run errors <id>

# Machine-readable error messages
valkyrie run errors <id> --format json
```

Connected text fetches display the benchmark, agent, model, dataset, run ID, and other run metadata before streaming
progress updates. Machine-readable output uses a versioned, allowlisted schema and does not include stored agent
secrets or kwargs. JSONL begins with a `snapshot` record, followed by zero or more `update` records. Recognized stream
termination emits `complete`, `error`, `stopped`, `disconnect`, or `interrupted`; unexpected clean exhaustion emits
`disconnect` and exits nonzero. Transport or malformed-protocol failures can exit nonzero without a final record, so
stderr and the process exit code remain authoritative for command failures.

Exit code 0 means the CLI handled the response or stream event; it does not mean the benchmark itself succeeded.
Agents must inspect each run's `status` and each JSONL record's `event`. Optional values such as model, starter, label,
finish time, and score can be null. In fetch output, `metadata_available: false` specifically means identity metadata
could not be loaded. All timestamps are UTC ISO 8601 strings, and non-finite scores are normalized to null.

The version 1 machine document shapes are:

- Fetch JSON/JSONL record: `schema_version`, `event`, `observed_at`, run identity, status/progress counts, score, and
  metadata availability.
- Run list document: `schema_version`, `kind: "run_list"`, `observed_at`, `returned_count`, and allowlisted `runs`.
- Batch status document: `schema_version`, `kind: "run_status"`, `observed_at`, request/return counts,
  `missing_run_ids`, and lightweight `runs` containing status and task counts.

Batch status preserves the requested ID order, ignores duplicate IDs, and requests up to 50 IDs at a time. A missing
or inaccessible ID is listed in `missing_run_ids` and makes the command exit nonzero after emitting the JSON document.
Its `finished_tasks` count includes terminal `FINISHED`, `ERROR`, and `STOPPED` tasks. Use `run list --format json --all`
when benchmark, agent, model, or dataset identity is also needed.

### Download results

To view results, you should use the following command to download results to disk:
```bash
# default path: ./<benchmark>.json)
valkyrie run results <id> --path ./results.json
```

You can also score results on only a subset of tasks:
```
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

# Stop only selected tasks
valkyrie run stop <id> --task-ids task-a,task-b
valkyrie run stop <id> --task-ids-file ./task-ids.txt

# Force stop all in-flight tasks immediately
valkyrie run stop <id> --force

# Force stop only selected tasks and remove their sandboxes
valkyrie run stop <id> --task-ids task-a,task-b --force
```

### Resume / Retry a run

```bash
# Resume pending tasks
valkyrie run resume <id>

# Retry errored tasks
valkyrie run retry <id>

# Override concurrency on resume (works on retry)
valkyrie run resume <id> --concurrency 20

# Stream updates after resume
valkyrie run resume <id> --connect

# Stream updates after retry
valkyrie run retry <id> --connect

# Override agent secrets for resumed tasks
valkyrie run resume <id> -s ANTHROPIC_API_KEY newSecretName

# Save every task ID in a benchmark dataset to a text file
valkyrie benchmark tasks swebench --dataset default

# Or choose the output path
valkyrie benchmark tasks swebench --dataset default --output tasks.txt
```

| Option | Description |
| --- | --- |
| `--concurrency` | Override concurrency level |
| `-s` / `--secret` | Secret pair as `ENV_VAR aws_secret_name`. Repeatable. Merged into the stored run contract before resumed/retried tasks are enqueued (CLI wins on conflict) |
| `--task-ids` | Comma-separated task IDs to resume/retry. Any id without an existing row is created as fresh `PENDING` if valid in the current dataset — lets you grow scope without starting a new run. |
| `--task-ids-file` | Local path or http(s) URL to a text file with one task ID per line |
| `--update-agent, -u` | Refresh the frozen agent copy from the current `agents/<name>.zip` in S3 before resuming |
| `--from-scratch` | Clear stored eval resume state and rerun generation for retried tasks |
| `--connect` | Stream run updates after resume/retry |

### List runs

```bash
valkyrie run list \
  --agent-name claude_code \
  --benchmark-name swebench \
  --status IN_PROGRESS \
  --order-by DESC \
  --started-by alice@vals.ai,bob@vals.ai \
  --label swebench_claude_code

# Dump every matching run as one machine-readable JSON document
valkyrie run list --format json --all \
  --model openai/gpt-5 \
  --status IN_PROGRESS
```

| Option | Description |
| --- | --- |
| `--agent-name` | Filter by agent name |
| `--benchmark-name` | Filter by benchmark name |
| `--model` | Filter by model |
| `-l` / `--label` | Filter by run label |
| `--status` | Filter by status: `IN_PROGRESS`, `STOPPING`, `STOPPED`, `FINISHED`, `ERROR` |
| `--order-by` | Order results (`desc` or `asc`) |
| `--started-by` | Comma-separated list of starter emails (case-insensitive) |
| `--format json` | Emit one versioned, allowlisted JSON document instead of a table (requires `--all`) |
| `--all` | Fetch every matching run without interactive paging (requires `--format json`) |

Supports paginated navigation ([h] previous, [l] next, [q] quit).
Machine output exhausts cursor pagination before writing stdout and excludes stored agent secrets, kwargs, and raw
error messages.

### Download run outputs

```bash
# Download all task outputs for a run
valkyrie run outputs <id> --output-dir ./outputs

# Download specific tasks (comma-separated)
valkyrie run outputs <id> --task-ids astropy__astropy-7606,django__django-10880
```

Valkyrie run outputs downloads the files produced during a run, while Valkyrie run results downloads the scores and evaluation.

### Download a specific file or folder from a run

```bash
valkyrie run output <id> [subpath] [-o ./output-dir]
```

| Argument / Option | Description |
| --- | --- |
| `BENCHMARK_ID` | UUID of the benchmark run |
| `SUBPATH` | Optional file or folder within the benchmark directory |
| `-o` / `--output-dir` | Local destination directory (defaults to `./<benchmark_id>`) |


## Adding Benchmarks

Vals provides a set of benchmarks out-of-the-box. If you want to add a new benchmark, you will need to add a new benchmark service. We provide a set of utilities that allow you to create and interact with benchmark services outside of the ones that are provided.

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

## Documentation

| Topic | Link |
| --- | --- |
| Python SDK | [Guide](docs/sdk/README.md) |
| Hosted vs self-hosted | [HOSTED_MODE.md](docs/HOSTED_MODE.md) |
| Local development | [DEVELOPMENT.md](docs/DEVELOPMENT.md) |
| Lambda integration | [LAMBDA_USAGE.md](docs/LAMBDA_USAGE.md) |
| Agent contracts | [CONTRACTS.md](docs/CONTRACTS.md) |
| Tracker service | [TRACKER.md](services/tracker/README.md) |
| Database & migrations | [DATABASE.md](services/tracker/src/tracker/database/README.md) |
| Infrastructure (AWS CDK) | [INFRASTRUCTURE.md](infra/README.md) |
| Sandbox secrets | [PROVIDER.md](docs/PROVIDER.md) |
| Contribute benchmark services | [Create benchmark service](https://github.com/vals-ai/create-benchmark-service) |

## Citation

If you use Valkyrie in your research, please cite our paper:

```bibtex
@inproceedings{forzano2026valkyrie,
  author    = {Forzano, Jarett and Almatov, Omar and Nashold, Langston and Ravi, Nikil and Kassian, Orestes},
  title     = {Valkyrie: A Microservice-Based Framework for Scalable Evaluation of AI Agents},
  booktitle = {Proceedings of the 1st ACM Conference on Agentic and AI Systems},
  series    = {CAIS '26},
  year      = {2026},
  month     = {may},
  address   = {San Jose, CA, USA},
  publisher = {ACM},
  location  = {New York, NY, USA},
  numpages  = {5},
  doi       = {10.1145/3786335.3813231},
  url       = {https://doi.org/10.1145/3786335.3813231}
}
```
