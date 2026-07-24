# Infrastructure

AWS CDK infrastructure for the Agentic Harness benchmark platform.

## Architecture

- **Shared Stack**: VPC, ECS cluster, service discovery, benchmark storage, executor release storage, and Redis
- **Tracker Stack**: Public API, load balancer, PostgreSQL, and the sealed release-control task
- **Worker Stack**: Stable ExecutorHost plus retained historical Worker logs
- **Monitoring Stack**: Tracker, load balancer, database, and Redis alarms

Production imports the existing `vals.ai` hosted zone. Development imports the
account-local `benchmark-tracker-dev.vals.ai` child zone and certificate.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- AWS CDK CLI

The deployment account must be bootstrapped before deploying this application.
Account setup owns the GitHub OIDC provider and deploy role, child hosted zone,
and certificate. Configure the protected `dev` GitHub Environment with
`DEV_ACCOUNT_ID`, `AWS_DEPLOY_ROLE_ARN`, `AWS_REGION=us-east-1`, and
`DESCOPE_PROJECT_ID`. To enable Sentry in dev, also set `SENTRY_DSN_SECRET_NAME`
to the name of an account-local Secrets Manager secret containing the DSN.

Before production executor activation, create a protected `production-release`
GitHub Environment that requires reviewers, permits only `prod`, and defines
`PRODUCTION_RELEASE_APPROVAL_CONFIGURED=true`. Both AWS accounts must already
have the account-owned `token.actions.githubusercontent.com` OIDC provider with
the `sts.amazonaws.com` audience. The stacks import that provider and create
separate environment-bound executor release roles; they do not create a fallback
provider.

The application imports these account-local values:

- `/valkyrie/dev/dns/tracker/hosted-zone-id`
- `/valkyrie/dev/dns/tracker/certificate-arn`
- Secrets Manager secret `devEvalInfraDescopeManagementKey`

## Setup

Install the CDK CLI with `brew install cdk`, then install the locked Python
dependencies:

```bash
make install
```

## Deployment

Production and development deploy automatically when changes reach `prod` and
`dev`. The manual **Deploy to AWS** workflow on `dev` remains available: use
`credentials-only` to verify the Environment and OIDC role, `plan` to review a
CDK diff, and `deploy` for an explicit scope.

Successful all-stack deployments run by the GitHub workflow also build and
activate an immutable executor release. Dev activation is automatic. Production
executor upload and activation wait for `production-release` approval after the
stacks deploy. Manual partial, plan, and credentials-only workflow operations
never activate a release.

Direct `make deploy` is CDK-only: it does not build, upload, or activate an
executor release. The first executor-dispatch rollout uses the Monitoring-only
pre-deployment, manual outage, and legacy-queue drain documented in
`docs/RELEASES.md`. Later workflow deployments keep existing executions pinned
while previous releases drain normally.

For a local plan or an administrator break-glass deployment, pass the target
explicitly. The preflight rejects the wrong account, Region, or STS identity.

```bash
export DEV_ACCOUNT_ID=123456789012
export DESCOPE_PROJECT_ID="dev-project-id"

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

`SCOPE` accepts `shared`, `tracker`, `worker`, `monitoring`, `driver`
(`release-test` only), or `all`. `worker` is the historical stack name for
ExecutorHost and retained Worker logs. CDK follows the existing stack
dependencies when an individual stack is selected. The
Makefile defaults `PRODUCTION_ACCOUNT_ID` to the Vals production account so a
dev target cannot select it accidentally. Self-hosted operators can override
that value for their own production account.

## Benchmark Catalog

`BENCHMARK_CATALOG_URL` points tracker-service at a benchmark catalog API. Set it for deployed tracker-service so `valkyrie config service list` can show the catalog of benchmarks hosted at that endpoint.

```bash
export BENCHMARK_CATALOG_URL=https://<api-id>.execute-api.us-east-1.amazonaws.com
```
