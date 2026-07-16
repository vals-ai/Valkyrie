# Infrastructure

AWS CDK infrastructure for Valkyrie.

## Stacks

- **SharedStack** -- VPC, ECS cluster, service discovery, S3, and Redis.
- **TrackerStack** -- Tracker API, load balancer, PostgreSQL, and public DNS record.
- **WorkerStack** -- Taskiq worker service.
- **MonitoringStack** -- Dashboards, alarms, and notifications.
- **DeploymentAccessStack** -- Development-account GitHub OIDC provider and Valkyrie deployment role.
- **DnsZoneStack** -- Retained `benchmark-tracker-dev.vals.ai` child hosted zone.

The `all` scope contains only the four application stacks. The deployment-access and DNS-zone stacks are explicit development-account prerequisites.

## Setup and verification

```bash
make install
make test
make lint
make typecheck
```

The AWS targets require an explicit stage, expected account, and Region. `PROFILE` selects local AWS credentials; leave it empty when GitHub Actions has already assumed the deployment role.

```bash
make preflight \
  STAGE=dev \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" \
  AWS_REGION=us-east-1 \
  PROFILE=vals-dev-admin
```

The preflight stops unless the CDK account, CDK Region, and STS caller match the selected target.

## Development deployment

Bootstrap the new account once with administrator credentials:

```bash
make bootstrap \
  DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" \
  AWS_REGION=us-east-1 \
  PROFILE=vals-dev-admin
```

The deployment-access stack is also deployed once with administrator credentials. The GitHub deployment role cannot deploy this stack through the generic workflow path.

```bash
make diff-deployment-access STAGE=dev DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION=us-east-1 PROFILE=vals-dev-admin
make deploy-deployment-access STAGE=dev DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION=us-east-1 PROFILE=vals-dev-admin
make diff-dns-zone STAGE=dev DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION=us-east-1 PROFILE=vals-dev-admin
make deploy-dns-zone STAGE=dev DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION=us-east-1 PROFILE=vals-dev-admin
```

Application plans and deployments use the protected `dev` GitHub Environment and the manual deployment workflow. The workflow accepts `credentials-only`, `plan`, and `deploy` operations with an explicit component scope. A push to `dev` does not deploy infrastructure.

The protected Environment supplies:

- `AWS_DEPLOY_ROLE_ARN`
- `DEV_ACCOUNT_ID`
- `AWS_REGION=us-east-1`

Dev authentication configuration is account-local:

- Descope project ID: SSM `/vals/dev/descope/project-id`
- Descope management key: Secrets Manager `devEvalInfraDescopeManagementKey`
- Tracker certificate ARN: SSM `/valkyrie/dev/dns/tracker/certificate-arn`

## Production deployment

A push to `prod` deploys the four application stacks to account `613431292675` in `us-east-1`. Production continues to use its existing environment-based authentication and Slack configuration.

`BENCHMARK_CATALOG_URL` points the tracker service at the benchmark catalog API used by `valkyrie config service list`.
