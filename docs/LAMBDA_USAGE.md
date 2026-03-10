# Lambda Usage

The `--lambda` flag on `benchmark start` lets you invoke an AWS Lambda function after a benchmark run completes.

```bash
harness benchmark start \
  --agent agents/claude_code \
  --benchmark swebench \
  --lambda my-post-benchmark-handler
```

The lambda is invoked once after all tasks finish and results are uploaded. If the lambda fails (uncaught exception or `statusCode >= 400`), the benchmark will be marked as `ERROR`.

## Payload

The tracker invokes your lambda with the full `BenchmarkArguments` plus the `benchmark_id`:

```json
{
  "contract": {
    "name": "claude_code",
    "install_cmd": "bash setup.sh",
    "run_cmd": "cat /tmp/problem_statement | claude -p ...",
    "final_output": "/logs",
    "secrets": {
      "ANTHROPIC_API_KEY": "devEvalInfraAnthropicKey"
    }
  },
  "concurrency": 5,
  "task_ids": ["astropy__astropy-12907"],
  "slice_str": null,
  "lambda_function": "my-post-benchmark-handler",
  "benchmark_id": "e532551e-d51b-4912-983d-47695bd24174"
}
```

| Field | Type | Description |
| --- | --- | --- |
| `contract` | object | The agent contract used for the run |
| `concurrency` | int | Concurrency level |
| `task_ids` | list or null | Task IDs that were run (null = all) |
| `slice_str` | string or null | Dataset slice if provided |
| `lambda_function` | string | Name of this lambda function |
| `benchmark_id` | string | UUID of the completed benchmark |

## Format required by AWS

AWS Lambda expects a `lambda_function.py` with a `lambda_handler` entry point

Example method:

```python
def lambda_handler(event, context):
    benchmark_id = event["benchmark_id"]
    agent_name = event["contract"]["name"]

    # Your post-benchmark logic here
    # e.g. send a Slack notification, trigger evaluation, etc.

    return {"statusCode": 200, "body": f"Processed {agent_name} benchmark {benchmark_id}"}
```

## Deployment

The lambda is invoked using the user's AWS credentials (from `harness config init`). It must be deployed in the user's AWS account and region defined by the user.
