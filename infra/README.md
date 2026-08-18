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
Environment with the `AWS_REGION=us-east-1` variable and the `DEV_ACCOUNT_ID`,
`AWS_DEPLOY_ROLE_ARN`, `DESCOPE_PROJECT_ID`, and
`DESCOPE_MANAGEMENT_KEY_SECRET_NAME` secrets (the last one names an
account-local Secrets Manager secret holding the Descope management key).
Managed AWS execution also requires these dev Environment secrets:

- `AWS_DEPLOYMENT_ROLE_ORG_IDS` -- comma-separated organization UUIDs allowed
  to submit managed runs
- `AWS_TRACKER_SECRET_NAME_PREFIXES` -- comma-separated Secrets Manager name
  prefixes the Tracker may resolve for benchmark-service authentication

The dev ExecutorHost task role can read every Secrets Manager secret in the dev
account and Region. Production and release-test do not receive this access.

To enable Sentry in dev, also set the `SENTRY_DSN_SECRET_NAME` secret to the
name of an account-local Secrets Manager secret containing the DSN. Production
requires `SENTRY_DSN_SECRET_NAME`, and `DESCOPE_MANAGEMENT_KEY_SECRET_NAME`
whenever `AUTH_REQUIRED` is `true`. The dev Environment holds every dev
deployment input as an Environment secret except `AWS_REGION`, which stays a
variable. The production jobs read repository-level secrets; the
`SANDBOX_CLEANUP_ENABLED` and `SANDBOX_CLEANUP_PROVIDER` toggles stay
variables.

Before production executor activation, configure the protected `prod` GitHub
Environment used by both production deploy jobs. Both AWS accounts must already
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

For a local plan or an administrator break-glass deployment, follow these
steps. The preflight rejects the wrong account, Region, or STS identity.

1. Export the target account and deployment-owned managed AWS inventory.

   **Why** -- Local CDK commands cannot read GitHub Environment secrets. The
   deployment must receive the same organization and secret namespaces as CI.

```bash
export DEV_ACCOUNT_ID=123456789012
export DESCOPE_PROJECT_ID="dev-project-id"
export DESCOPE_MANAGEMENT_KEY_SECRET_NAME="dev-descope-management-key-secret"
export AWS_DEPLOYMENT_ROLE_ORG_IDS="00000000-0000-0000-0000-000000000001"
export AWS_TRACKER_SECRET_NAME_PREFIXES="benchmark-services/"
```

   In GitHub, the corresponding values are under **Settings → Environments →
   dev → Environment secrets**. GitHub does not reveal stored secret values, so
   an administrator must obtain the approved values from the deployment owner.

   **Done when** -- All five variables print as non-empty when checked locally;
   do not print their values into shared logs.

2. Plan the intended scope.

   **Why** -- The plan verifies the account boundary and shows the exact
   CloudFormation changes before any resource is modified.

```bash

make plan STAGE=dev SCOPE=all AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin
```

   Console alternative: run **Actions → Deploy to AWS → Run workflow** on
   `dev`, choose `plan`, and select the intended scope.

   **Done when** -- The plan targets the expected dev account and contains only
   the intended stack changes.

3. Deploy the approved scope only when break-glass access is authorized.

   **Why** -- Direct deployment bypasses the normal branch-push workflow and is
   reserved for recovery operations.

```bash

make deploy STAGE=dev SCOPE=shared AWS_REGION=us-east-1 \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" PROFILE=vals-dev-admin
```

   This step is CLI-only because the manual GitHub workflow intentionally does
   not offer deployment. Normal deployments come from a merge to `dev`.

   **Done when** -- CDK reports the selected stack deployed in the expected dev
   account and its CloudFormation events contain no failed resources.

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
