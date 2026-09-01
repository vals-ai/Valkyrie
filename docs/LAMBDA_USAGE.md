# Lambda Usage

The `--lambda` flag on `run start` lets you invoke AWS Lambda functions after a run completes, and may be repeated.

```bash
valkyrie run start \
  --agent agents/claude_code \
  --benchmark swebench \
  --lambda my-post-benchmark-handler \
  --lambda my-result-archiver
```

Each lambda is invoked once after all tasks finish and results are uploaded, in the order given. A lambda that fails (uncaught exception or `statusCode >= 400`) does not stop the remaining ones; the failure is logged and reported, and the first one is re-raised once every lambda has been attempted. The run status is already terminal by this point, so a failing lambda does not change it.

## Payload

The tracker invokes your lambda with the full `BenchmarkArguments` plus the persisted benchmark ID and name:

```json
{
  "contract": {
    "name": "claude_code",
    "install_cmd": "bash setup.sh",
    "run_cmd": "cat /tmp/problem_statement | claude -p ...",
    "final_output": "/logs",
    "secrets": {
      "ANTHROPIC_API_KEY": "YourAnthropicKeySecret"
    }
  },
  "concurrency": 5,
  "task_ids": ["astropy__astropy-12907"],
  "slice_str": null,
  "lambda_function": "my-post-benchmark-handler",
  "lambda_functions": ["my-post-benchmark-handler", "my-result-archiver"],
  "benchmark_id": "e532551e-d51b-4912-983d-47695bd24174",
  "benchmark_name": "swebench"
}
```

| Field | Type | Description |
| --- | --- | --- |
| `contract` | object | The agent contract used for the run |
| `concurrency` | int | Concurrency level |
| `task_ids` | list or null | Task IDs that were run (null = all) |
| `slice_str` | string or null | Dataset slice if provided |
| `lambda_function` | string | Name of the lambda receiving this payload |
| `lambda_functions` | list | Every lambda invoked at completion, in configured order |
| `benchmark_id` | string | UUID of the completed run |
| `benchmark_name` | string | Persisted name of the completed benchmark |

## Format required by AWS

AWS Lambda expects a `lambda_function.py` with a `lambda_handler` entry point

Example method:

```python
def lambda_handler(event, context):
    benchmark_id = event["benchmark_id"]
    agent_name = event["contract"]["name"]

    # Your post-run logic here
    # e.g. send a Slack notification, trigger evaluation, etc.

    return {"statusCode": 200, "body": f"Processed {agent_name} run {benchmark_id}"}
```

## Deployment

The lambda is invoked using the user's AWS credentials (from `valkyrie config init`). It must be deployed in the user's AWS account and region defined by the user.
