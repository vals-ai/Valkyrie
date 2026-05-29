# Hosted vs Self-Hosted Mode

Valkyrie supports two operational modes. Hosted mode uses Vals-managed tracker credentials injected at deploy time; self-hosted mode uses the credentials in your local Valkyrie config.

## Hosted mode

Use Vals-hosted compute infrastructure without client authentication.

### Prerequisites

- Hosted tracker URL, if you are not using the default tracker.

### Setup

```bash
valkyrie config init
```

Choose **hosted** when prompted. No client credentials are required.

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: hosted
```

The CLI does not send `X-Api-Key` or `X-Harness-*` headers in unauthenticated hosted mode. AWS, S3, CloudWatch, and Daytona settings are resolved by the hosted tracker service.

## Self-hosted mode

Run your own infrastructure end-to-end, including your own tracker service deployment.

### Prerequisites

- Your own tracker service deployment (see [Infrastructure docs](../infra/README.md))
- AWS account with the [required permissions](#required-aws-permissions)
- S3 bucket for storing benchmark artifacts and agents
- API key for sandbox provider (Daytona). [Setup docs](PROVIDER.md)

### Setup

Set `TRACKER_SERVICE_URL` to point at your tracker instance:

```bash
export TRACKER_SERVICE_URL=https://your-tracker.example.com
```

Then run `config init` and choose **self-hosted**:

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: self-hosted
AWS_ACCESS_KEY_ID: ...
AWS_SECRET_ACCESS_KEY: ...
...
```

No API key or Descope authentication is used. All data belongs to a single default organization.

### Deploy-time tracker configuration

The tracker service resolves hosted credentials from its environment when a request does not include `X-Harness-*` headers:

```env
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=agentic-harness
DAYTONA_SECRET_NAME=AgenticHarnessSecrets
LOG_GROUP=benchmarks
LOG_RETENTION_POLICY=365
```

When running on AWS, the tracker and worker can use their task role for S3, CloudWatch Logs, Lambda, and Secrets Manager instead of static `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` values.

## Required AWS permissions

Self-hosted credentials or the deployed tracker task role must have the following permissions:

| Service | Permissions | Used for |
|---------|------------|----------|
| **S3** | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` | Storing benchmark results, agent artifacts, and agent outputs |
| **S3** | `s3:GetObject` (for presigned URLs) | Generating download links for results |
| **CloudWatch Logs** | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | Streaming task execution logs |
| **Secrets Manager** | `secretsmanager:GetSecretValue` | Retrieving sandbox provider credentials (Daytona) and webhook URLs |
| **Lambda** (optional) | `lambda:InvokeFunction` | Post-benchmark Lambda invocation (only if using `--lambda` flag) |

These permissions should be scoped to the S3 bucket, CloudWatch log group, and Secrets Manager secrets you configure during `valkyrie config init`.
