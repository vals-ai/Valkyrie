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

## Teardown

- Don't do this

```bash
make destroy
```
