# Lambda Usage

The `--lambda` flag on `run start` lets you invoke an AWS Lambda function after a run completes.

```bash
valkyrie run start \
  --agent agents/claude_code \
  --benchmark swebench \
  --lambda my-post-benchmark-handler
```

The lambda is invoked once after all tasks finish and results are uploaded. If the lambda fails (uncaught exception or `statusCode >= 400`), the run will be marked as `ERROR`.

`run results --lambda` invokes a lambda on the results it just uploaded, replacing the one the run was started with. There is no status gate, so a subset can be exported while the run is still in progress:

```bash
valkyrie run results e532551e-d51b-4912-983d-47695bd24174 \
  --s3 \
  --task-ids astropy__astropy-12907,django__django-11099 \
  --lambda my-post-benchmark-handler
```

The callback upload uses a request-specific key under `benchmarks/<run-id>/result-callbacks/results/`. It does not overwrite the canonical result object used by ordinary `--s3` retrieval. The tracker returns links for this object and passes its exact bucket and key to the Lambda. In hosted deployments, only the credential that started the run can trigger the callback.

## Payload

For `run results --lambda`, the tracker invokes the Lambda with the full `BenchmarkArguments`, the selected task IDs, the persisted benchmark ID and name, and the exact callback object location:

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
  "benchmark_id": "e532551e-d51b-4912-983d-47695bd24174",
  "benchmark_name": "swebench",
  "s3_bucket": "my-valkyrie-bucket",
  "s3_key": "benchmarks/e532551e-d51b-4912-983d-47695bd24174/result-callbacks/results/3bb3f1f7-f64f-42b3-a2c7-a96e0b2ea27a.json"
}
```

| Field | Type | Description |
| --- | --- | --- |
| `contract` | object | The agent contract used for the run |
| `concurrency` | int | Concurrency level |
| `task_ids` | list or null | Task IDs that were run (null = all) |
| `slice_str` | string or null | Dataset slice if provided |
| `lambda_function` | string | Name of this lambda function |
| `benchmark_id` | string | UUID of the completed run |
| `benchmark_name` | string | Persisted name of the completed benchmark |
| `s3_bucket` | string | Bucket containing this callback's result view |
| `s3_key` | string | Exact object key for this callback's result view |

## Format required by AWS

AWS Lambda expects a `lambda_function.py` with a `lambda_handler` entry point

Example method:

```python
def lambda_handler(event, context):
    benchmark_id = event["benchmark_id"]
    agent_name = event["contract"]["name"]
    results_bucket = event["s3_bucket"]
    results_key = event["s3_key"]

    # Read this callback's result view from results_bucket/results_key.
    # e.g. send a Slack notification, trigger evaluation, etc.

    return {"statusCode": 200, "body": f"Processed {agent_name} run {benchmark_id}"}
```

## Deployment

The lambda is invoked using the user's AWS credentials (from `valkyrie config init`). It must be deployed in the user's AWS account and region defined by the user.
