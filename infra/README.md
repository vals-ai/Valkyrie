# Infrastructure

AWS CDK infrastructure for the Agentic Harness benchmark platform.

## Architecture

- **Shared Stack**: VPC, ECS cluster, service discovery namespace, S3 bucket, and Redis
- **Tracker Stack**: Public API, load balancer, and PostgreSQL database
- **Worker Stack**: Background benchmark execution workers
- **Monitoring Stack**: Service, load balancer, database, and Redis alarms

Production imports the existing `vals.ai` hosted zone. Development imports the
account-local `benchmark-tracker-dev.vals.ai` child zone and certificate.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- AWS CDK CLI

The dev account must be bootstrapped before deploying this application. Account
setup owns the GitHub OIDC role, child hosted zone, and certificate. Configure the
protected `dev` GitHub Environment with `DEV_ACCOUNT_ID`, `AWS_DEPLOY_ROLE_ARN`,
`AWS_REGION=us-east-1`, and `DESCOPE_MANAGEMENT_SECRET_NAME`. To enable Sentry in dev, also
set `SENTRY_DSN_SECRET_NAME` to the name of an account-local Secrets Manager
secret containing the DSN.

Dashboard users need no personal Valkyrie credential. Managed runs use the
Tracker and worker task roles plus one Tracker-owned benchmark-service access
key per organization. Configure these Environment variables before enabling
managed submissions:

- `AWS_MANAGED_TENANT_IDS`: optional strict override for the managed Descope tenant allowlist (defaults to `vals.ai`)
- `AWS_DEPLOYMENT_SANDBOX_PROVIDER`: for example, `daytona`
- `AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME`: account-local provider secret
- `AWS_MANAGED_AGENT_SECRET_NAMES`: comma-separated exact secret names used by managed agent contracts
- `AWS_MANAGED_SUBMISSIONS_ENABLED`: `true` only after both services support the current queue protocol

Deploy Tracker and worker once with submissions disabled, then set
`AWS_MANAGED_SUBMISSIONS_ENABLED=true` and deploy again. This keeps an older
worker from receiving a newer managed job during a rolling deployment.
Production reads the same names from repository variables because its deploy
role trusts the `prod` branch directly.

The application imports these account-local values:

- `/valkyrie/dev/dns/tracker/hosted-zone-id`
- `/valkyrie/dev/dns/tracker/certificate-arn`
- the Secrets Manager secret named by `DESCOPE_MANAGEMENT_SECRET_NAME`

## Setup

Install the CDK CLI with `brew install cdk`, then install the locked Python
dependencies:

```bash
make install
```

## Deployment

Production deploys automatically when changes reach `prod`. Development deploys
automatically on `dev` pushes and also supports the manual **Deploy to AWS**
workflow. Use `credentials-only` to verify the Environment and OIDC role,
`plan` to review a CDK diff, and `deploy` after approval.

For a local plan or an administrator break-glass deployment, pass the target
explicitly. The preflight rejects the wrong account, Region, or STS identity.

```bash
export DEV_ACCOUNT_ID=123456789012
export DESCOPE_MANAGEMENT_SECRET_NAME="dev-descope-management-key"

make plan STAGE=dev SCOPE=all AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin

make deploy STAGE=dev SCOPE=shared AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin
```

`SCOPE` accepts `shared`, `tracker`, `worker`, `monitoring`, or `all`. CDK follows
the existing stack dependencies when an individual stack is selected. The
Makefile defaults `PRODUCTION_ACCOUNT_ID` to the Vals production account so a
dev target cannot select it accidentally. Self-hosted operators can override
that value for their own production account.

## Benchmark Catalog

`BENCHMARK_CATALOG_URL` points tracker-service at a benchmark catalog API. Set it for deployed tracker-service so `valkyrie config service list` can show the catalog of benchmarks hosted at that endpoint.

```bash
export BENCHMARK_CATALOG_URL=https://<api-id>.execute-api.us-east-1.amazonaws.com
```
