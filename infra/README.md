# Infrastructure

AWS CDK infrastructure for the Agentic Harness benchmark platform.

## Architecture

- **Shared Stack**: VPC, ECS cluster, service discovery, benchmark storage, and Redis
- **Tracker Stack**: Public API, load balancer, and PostgreSQL
- **Executor Stack**: Stable ExecutorHost, executor release storage, sealed release control, and retained Worker logs
- **Monitoring Stack**: Tracker, load balancer, database, and Redis alarms

`ExecutorStack` is the Python owner and `executor` is the deployment scope. Its
physical CloudFormation name remains `WorkerStack` so deployments update the
existing stack and retained resources in place.

Production imports the existing `vals.ai` hosted zone. Development imports only
the account-local `benchmark-tracker-dev.vals.ai` child zone. Each production
and development Tracker stack creates and owns its ACM certificate.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- AWS CDK CLI

The deployment account must be bootstrapped before deploying this application.
Account setup owns the GitHub OIDC provider and deploy role and the child hosted
zone. Before deploying the development Tracker, the production root zone must
delegate `benchmark-tracker-dev.vals.ai` to the account-local child hosted zone
so CDK DNS validation can complete. Configure the protected `dev` GitHub
Environment with `DEV_ACCOUNT_ID`, `AWS_DEPLOY_ROLE_ARN`,
`AWS_REGION=us-east-1`, `DESCOPE_PROJECT_ID`, and
`DESCOPE_MANAGEMENT_KEY_SECRET_NAME` (the name of an account-local Secrets
Manager secret holding the Descope management key). To enable Sentry in dev,
also set `SENTRY_DSN_SECRET_NAME` to the name of an account-local Secrets
Manager secret containing the DSN. Production requires
`SENTRY_DSN_SECRET_NAME`, and `DESCOPE_MANAGEMENT_KEY_SECRET_NAME` whenever
`AUTH_REQUIRED` is `true`.

Before production executor activation, configure the protected `prod` GitHub
Environment used by the production executor job. Both AWS accounts must already
have the account-owned `token.actions.githubusercontent.com` OIDC provider with
the `sts.amazonaws.com` audience. The stacks import that provider and create
separate environment-bound executor release roles; they do not create a fallback
provider.

The application imports these account-local values:

- `/valkyrie/dev/dns/tracker/hosted-zone-id`
- the Secrets Manager secret named by `DESCOPE_MANAGEMENT_KEY_SECRET_NAME`

## Setup

Install the CDK CLI with `brew install cdk`, then install the locked Python
dependencies:

```bash
make install
```

## Deployment

Production and development deploy automatically when changes reach `prod` and
`dev`. The manual **Deploy to AWS** workflow on `dev` supports only
`credentials-only` and `plan`; deployments come from branch pushes.

Core and executor changes use separate jobs but one deployment mutex per stage.
A core-only change never builds an executor artifact, enters executor maintenance,
deploys the physical `WorkerStack`, or activates a release. A stale queued
executor job exits before using AWS credentials. Production approval is a
separate no-op job, so waiting for approval does not hold the deployment mutex.
Manual partial, plan, and credentials-only workflow operations never activate a
release.

Direct `make deploy` is CDK-only: it does not build, upload, or activate an
executor release. The first executor-dispatch rollout uses the Monitoring-only
pre-deployment, manual outage, legacy-queue drain, and separately authorized
physical `WorkerStack` bootstrap documented in `docs/executor-releases/README.md`. Automated
executor work fails closed until the bootstrap publishes the stage's sealed
release-control SSM parameter. Later workflow deployments keep existing
executions pinned while previous releases drain normally.

For a local plan or an administrator break-glass deployment, pass the target
explicitly. The preflight rejects the wrong account, Region, or STS identity.

```bash
export DEV_ACCOUNT_ID=123456789012
export DESCOPE_PROJECT_ID="dev-project-id"
export DESCOPE_MANAGEMENT_KEY_SECRET_NAME="dev-descope-management-key-secret"

make plan STAGE=dev SCOPE=all AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin

make deploy STAGE=dev SCOPE=shared AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin
```

`release-test` is an isolated, dev-sized deployment in the account selected by
`DEV_ACCOUNT_ID`. It uses `-release-test` resource names and
`/valkyrie/release-test/` output parameters. Its Tracker uses an internal ALB;
the ALB DNS output is reachable from the VPC instead of creating a DNS record
or certificate. Its benchmark-service base is
`benchmarks.vals.ai`; no separate benchmark-service stack is created. Unlike
`dev`, the target guard permits `release-test` to be explicitly deployed in the
production account when coexistence validation requires it.

```bash
make plan STAGE=release-test SCOPE=all AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin
```

`SCOPE` accepts `shared`, `tracker`, `executor`, `monitoring`, `driver`
(`release-test` only), `core`, or `all`. The `executor` scope targets the
historical physical `WorkerStack` name. CDK follows the existing stack
dependencies when an individual stack is selected. The
Makefile defaults `PRODUCTION_ACCOUNT_ID` to the Vals production account so a
dev target cannot select it accidentally. Self-hosted operators can override
that value for their own production account.

## Benchmark Catalog

`BENCHMARK_CATALOG_URL` points tracker-service at a benchmark catalog API. Set it for deployed tracker-service so `valkyrie config service list` can show the catalog of benchmarks hosted at that endpoint.

```bash
export BENCHMARK_CATALOG_URL=https://<api-id>.execute-api.us-east-1.amazonaws.com
```

## Sandbox cleanup schedule

Production includes an hourly EventBridge schedule for a singleton, 14-minute cleanup Lambda. The schedule is disabled
unless `SANDBOX_CLEANUP_ENABLED` is exactly `true`; Scheduler delivery and asynchronous Lambda failures go to an encrypted
dead-letter queue.

The Lambda loads the selected Create Benchmark Service (CBS) provider configuration from Secrets Manager and uses the
provider directly to list, refresh, and delete sandboxes in its configured scope. Sandboxes strictly older than 48 hours
are deleted unless their exact `clean-up` label is `false` after trimming and case-folding. The secret must contain the
JSON fields required by the selected CBS provider. Providers must support creation-time-filtered inventory metadata;
currently Daytona is the only compatible provider and uses `DAYTONA_API_KEY`, `DAYTONA_API_URL`, and `DAYTONA_TARGET`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `SANDBOX_CLEANUP_ENABLED` | `false` | Enables the hourly production schedule |
| `SANDBOX_CLEANUP_PROVIDER` | `daytona` | Selects a cleanup-compatible CBS sandbox provider |
| `SANDBOX_CLEANUP_SECRET_NAME` | `YourSandboxProviderSecret` | Selects the provider credentials secret |
