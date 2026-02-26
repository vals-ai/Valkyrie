# Infrastructure

AWS CDK infrastructure for the Agentic Harness benchmark platform.

## Architecture

- **Shared Stack**: VPC, ECS cluster, service discovery namespace, S3 bucket, Route53 hosted zone
- **Tracker Stack**: Public-facing API (benchmark-tracker.vals.ai) with ALB, Fargate, and Redis/Postgres sidecars

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Daytona API key stored in AWS Secrets Manager as `prodAgenticHarnessDaytonaKey`

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

## Utilities

```bash
# Show service endpoints
./scripts/service-ips.sh

# View and whitelist IPs
./scripts/whitelist-ip.sh
```

## Connecting to Private Services

Private services can be accessed directly via their task's public IP for debugging:

1. Whitelist your IP:

   ```bash
   ./scripts/whitelist-ip.sh --add
   ```

2. Get the service IP:

   ```bash
   ./scripts/service-ips.sh
   ```

3. Connect directly:
   ```bash
   curl http://<task-ip>:8000/health
   ```

## Teardown

- Don't do this

```bash
make destroy
```
