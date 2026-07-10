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

Production includes an hourly EventBridge Scheduler target that launches a small, one-off Fargate task. The schedule is
disabled and the command is in dry-run mode by default. It only considers sandboxes carrying the explicit Valkyrie,
production, target, and `clean-up=true` metadata added by the tracker.

The task reads `DAYTONA_API_KEY`, `DAYTONA_API_URL`, and `DAYTONA_TARGET` JSON fields from an existing Secrets Manager
secret. Configure the production deploy with GitHub repository variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DAYTONA_CLEANUP_SECRET_NAME` | `DaytonaSecrets` | Existing JSON secret used by the cleanup task |
| `DAYTONA_CLEANUP_ENABLED` | `false` | Enables the hourly production schedule |
| `DAYTONA_CLEANUP_DRY_RUN` | `true` | Reports eligible sandboxes without deleting them |

Safe rollout:

1. Set `DAYTONA_CLEANUP_SECRET_NAME` to the intended service-owned secret and validate its fields and KMS access.
2. Deploy with the default disabled, dry-run configuration.
3. Run the isolated live cleanup integration test against approved test credentials.
4. Run the production task manually in dry-run mode and inspect `/valkyrie/daytona-cleanup` logs.
5. Set `DAYTONA_CLEANUP_ENABLED=true` while leaving dry-run enabled, then observe a scheduled invocation.
6. Set `DAYTONA_CLEANUP_DRY_RUN=false` and redeploy the same revision.

Legacy sandboxes without the new ownership labels are intentionally excluded and require a separately audited cleanup.
The scheduler dead-letter queue captures invocation failures; cleanup process failures are reported in its CloudWatch log.

## Teardown

- Don't do this

```bash
make destroy
```
