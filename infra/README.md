# Infrastructure

AWS CDK infrastructure for the Agentic Harness benchmark platform.

## Architecture

- **Shared Stack**: VPC, ECS cluster, service discovery namespace, S3 bucket, Route53 hosted zone
- **Tracker Stack**: Public-facing API (benchmark-tracker.vals.ai) with ALB, Fargate, and Redis/Postgres sidecars

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

## Setup

Install cdk

```bash
brew install cdk
```

Install dev dependencies

```bash
make install
```

## Deployment

```bash
# Deploy all stacks
make deploy

# Deploy individual stacks
make deploy-shared
make deploy-tracker
# Preview changes
make diff

# Fast deployment (skips CloudFormation for code changes)
# ONLY FOR CODE CHANGES
make hotswap
```

## Benchmark Catalog

`BENCHMARK_CATALOG_URL` points tracker-service at a benchmark catalog API. Set it for deployed tracker-service so `valkyrie config service list` can show the catalog of benchmarks hosted at that endpoint.

```bash
export BENCHMARK_CATALOG_URL=https://<api-id>.execute-api.us-east-1.amazonaws.com
```

## Daytona cleanup schedule

Production includes an hourly EventBridge Scheduler target that asynchronously invokes a container-image Lambda. The
schedule is disabled and the function is in dry-run mode by default. The Lambda is not attached to the VPC, has a
14-minute timeout, and reserves one concurrent execution so cleanup sweeps cannot overlap.

The sweep covers every sandbox visible through the `AgenticHarnessSecrets` credentials and their configured target that
is strictly older than 48 hours. This includes sandboxes provisioned by Harbor and benchmark services outside the
Valkyrie tracker lifecycle. Only the exact `clean-up` label key exempts a sandbox when its value equals `false` after
trimming and case-folding; a missing key remains eligible.

At invocation time, the Lambda reads the `DAYTONA_API_KEY`, `DAYTONA_API_URL`, and `DAYTONA_TARGET` JSON fields from the
fixed `AgenticHarnessSecrets` Secrets Manager secret. Configure rollout with GitHub repository variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DAYTONA_CLEANUP_ENABLED` | `false` | Enables the hourly production schedule |
| `DAYTONA_CLEANUP_DRY_RUN` | `true` | Reports eligible sandboxes without deleting them |

Safe rollout:

1. Validate that `AgenticHarnessSecrets` contains the three required Daytona fields, uses the same Daytona API and target
   as every producer being swept, and can see those producers' sandboxes. Give every legitimate sandbox expected to
   exceed 48 hours the exact `clean-up=false` label before enabling deletion.
2. Deploy with the default disabled, dry-run configuration.
3. Run the isolated live cleanup integration test against approved test credentials.
4. Invoke the Lambda once in dry-run mode, then inspect `/valkyrie/daytona-cleanup` logs and the cleanup DLQ.
5. Assign an owner for those signals, set `DAYTONA_CLEANUP_ENABLED=true` while leaving dry-run enabled, redeploy the same
   revision, and inspect the logs and DLQ after at least one scheduled invocation.
6. With explicit approval, set `DAYTONA_CLEANUP_DRY_RUN=false` and redeploy the same revision.

The encrypted dead-letter queue receives both Scheduler delivery failures and Lambda asynchronous handler failures;
their message formats identify which stage failed. Individual sandbox deletion failures are also reported in CloudWatch,
and make the invocation fail after the sweep so the failure reaches the queue without preventing later candidates from
being attempted.

## Teardown

- Don't do this

```bash
make destroy
```
