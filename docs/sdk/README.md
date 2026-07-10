# Python SDK

Use Valkyrie's async Python SDK to manage runs from Python. The SDK requires Python 3.12 or newer.

## Installation

Install the standalone package from PyPI:

```bash
pip install valkyrie-sdk
```

The package does not install the Valkyrie CLI or tracker service.

## Quickstart

The SDK reads the same config as the CLI. See [Hosted vs Self-Hosted Mode](../HOSTED_MODE.md) for setup.

```python
from valkyrie.sdk import ValkyrieClient

async with ValkyrieClient.from_config() as client:
    runs = await client.runs.list()
```

Pass `base_url` to `ValkyrieClient` for a self-hosted tracker. Otherwise, the SDK uses
`TRACKER_SERVICE_URL` or the hosted tracker URL.

The SDK sends AWS credentials in `X-Harness-*` headers. Only connect to trusted trackers, and use
HTTPS outside local development.

## Examples

- [`run_lifecycle.py`](examples/run_lifecycle.py): start, stream, and retrieve a run.
- [`manage_run.py`](examples/manage_run.py): stop, resume, or retry a run.

```bash
python docs/sdk/examples/run_lifecycle.py --help
python docs/sdk/examples/manage_run.py --help
```

Maintainers can follow [RELEASING.md](RELEASING.md) to publish the package.

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

Use the other run methods:

```python
page = await client.runs.list()
results = await client.runs.results(run.benchmark_id)
await client.runs.stop(run.benchmark_id)
await client.runs.resume(run.benchmark_id, concurrency=20)
await client.runs.retry(run.benchmark_id, task_ids=["task-1"])
```

## Errors

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
