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
`AWS_REGION=us-east-1`, and `DESCOPE_PROJECT_ID`. To enable Sentry in dev, also
set `SENTRY_DSN_SECRET_NAME` to the name of an account-local Secrets Manager
secret containing the DSN.

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

Production deploys automatically when changes reach `prod`. Development deploys
only from the manual **Deploy to AWS** workflow on the `dev` branch. Use
`credentials-only` to verify the Environment and OIDC role, `plan` to review a
CDK diff, and `deploy` after approval.

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

## Sandbox cleanup schedule

Production includes a provider-generic cleanup engine invoked hourly by EventBridge Scheduler through a container-image
Lambda. The schedule is disabled and the function is in dry-run mode by default. The Lambda is not attached to the VPC,
has a 14-minute timeout, and reserves one concurrent execution so cleanup sweeps cannot overlap.

The engine selects a sandbox-provider adapter from `SANDBOX_CLEANUP_PROVIDER`. Daytona is the currently supported adapter
and remains the default. An unsupported provider fails closed before listing or deleting sandboxes. The Daytona adapter
sweeps every sandbox visible through the configured credentials and target that is strictly older than 48 hours. This
includes sandboxes provisioned by Harbor and benchmark services outside the Valkyrie tracker lifecycle. Only the exact
`clean-up` label key exempts a sandbox when its value equals `false` after trimming and case-folding; a missing key remains
eligible. Adding a provider requires an adapter that supplies normalized candidate metadata and implements complete-list,
refresh, and delete operations; the age, opt-out, revalidation, timeout, and reporting policy remains shared.

For Daytona, the Lambda reads the `DAYTONA_API_KEY`, `DAYTONA_API_URL`, and `DAYTONA_TARGET` JSON fields from the selected
Secrets Manager secret. Configure rollout with GitHub repository variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SANDBOX_CLEANUP_ENABLED` | `false` | Enables the hourly production schedule |
| `SANDBOX_CLEANUP_DRY_RUN` | `true` | Reports eligible sandboxes without deleting them |
| `SANDBOX_CLEANUP_PROVIDER` | `daytona` | Selects the sandbox-provider adapter |
| `SANDBOX_CLEANUP_SECRET_NAME` | `AgenticHarnessSecrets` | Selects the provider credentials secret |

Safe rollout:

1. Validate that the selected secret contains the fields required by the selected provider and can see every producer's
   sandboxes. For Daytona, verify the three fields above use the same API and target as every producer being swept. Give
   every legitimate sandbox expected to exceed 48 hours the exact `clean-up=false` label before enabling deletion.
2. Deploy with the default disabled, dry-run configuration.
3. Run the isolated live cleanup integration test against approved test credentials.
4. Invoke the Lambda once in dry-run mode, then inspect `/valkyrie/sandbox-cleanup` logs and the cleanup DLQ.
5. Assign an owner for those signals, set `SANDBOX_CLEANUP_ENABLED=true` while leaving dry-run enabled, redeploy the same
   revision, and inspect the logs and DLQ after at least one scheduled invocation.
6. With explicit approval, set `SANDBOX_CLEANUP_DRY_RUN=false` and redeploy the same revision.

The encrypted dead-letter queue receives both Scheduler delivery failures and Lambda asynchronous handler failures;
their message formats identify which stage failed. Individual sandbox deletion failures are also reported in CloudWatch,
and make the invocation fail after the sweep so the failure reaches the queue without preventing later candidates from
being attempted.
