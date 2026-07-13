# Python SDK

Use Valkyrie's async Python SDK to manage runs without invoking the CLI. The SDK currently requires Python 3.12.x.

## Installation from source

The SDK is part of the Valkyrie Python package. A standalone PyPI package is not available yet; publishing one is tracked separately. Until then, install it from the repository:

```bash
git clone https://github.com/vals-ai/Valkyrie.git
cd Valkyrie
uv sync
```

The source checkout lets `uv` install the bundled tracker dependency alongside the SDK.

## Quickstart

The SDK reads the same config as the CLI. See [Hosted vs Self-Hosted Mode](../HOSTED_MODE.md) for setup.

```python
from valkyrie.sdk import ValkyrieClient

async with ValkyrieClient.from_config() as client:
    runs = await client.runs.list()
```

Services can validate the same YAML-shaped mapping in memory instead of reading a local file:

```python
from valkyrie.sdk import ValkyrieClient, ValkyrieConfig

config = ValkyrieConfig.model_validate(config_values)

async with ValkyrieClient(config=config) as client:
    runs = await client.runs.list()
```

Pass `base_url` to `ValkyrieClient` for a self-hosted tracker. Otherwise, the SDK uses `TRACKER_SERVICE_URL` or the hosted tracker URL.

The SDK sends AWS credentials in `X-Harness-*` headers. Only connect to trusted trackers and use HTTPS outside local development.

## Executable examples

The examples use the default Valkyrie config and require explicit command-line arguments before they can start or modify a run:

- [`run_lifecycle.py`](examples/run_lifecycle.py) starts a run, streams updates, fetches its final state, lists visible runs, and retrieves results when the run finishes.
- [`manage_run.py`](examples/manage_run.py) stops, resumes, or retries an existing run through explicit subcommands.

Run them from the repository root:

```bash
uv run python docs/sdk/examples/run_lifecycle.py --help
uv run python docs/sdk/examples/manage_run.py --help
```

## Run lifecycle

Start a run with an uploaded agent name or `AgentContractRequest`:

```python
run = await client.runs.start(
    agent="sweagent",
    benchmark="swebench",
    model="anthropic/claude-sonnet-4-6",
    concurrency=10,
    dataset="default",
    label="nightly-swebench",
)
```

Fetch or stream run updates:

```python
current = await client.runs.fetch(run.benchmark_id)

async for update in client.runs.stream(run.benchmark_id):
    print(update.details.status)
```

Manage the remaining run lifecycle:

```python
page = await client.runs.list()
results = await client.runs.results(run.benchmark_id)
await client.runs.stop(run.benchmark_id)
await client.runs.resume(run.benchmark_id, concurrency=20)
await client.runs.retry(run.benchmark_id, task_ids=["task-1"])
```

## Error handling

All SDK exceptions inherit from `ValkyrieSDKError`:

```python
from valkyrie.sdk import ValkyrieAPIError, ValkyrieSDKError

try:
    run = await client.runs.fetch(run_id)
except ValkyrieAPIError as exc:
    print(exc.status_code, exc.detail)
except ValkyrieSDKError as exc:
    print(exc)
```

| Exception | Description |
| --- | --- |
| `ValkyrieConfigError` | Invalid SDK configuration |
| `ValkyrieRunError` | Invalid input for a run operation |
| `ValkyrieAPIError` | Non-success API response |
| `ValkyrieTransportError` | Connection or timeout failure |
| `ValkyrieStreamError` | Invalid streaming event |
