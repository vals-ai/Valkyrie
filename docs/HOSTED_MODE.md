# Hosted vs Self-Hosted Mode

Valkyrie supports an API-key-only hosted runtime and a caller-owned self-hosted runtime.

## Hosted mode

Use Vals-managed compute, storage, logging, sandbox credentials, and model access. Data is isolated by organization and authenticated with your personal Vals AI API key.

### Prerequisites

- Personal Vals AI API key linked to your user and organization

### Setup

```bash
valkyrie config init
```

Hosted is the default. The CLI validates your key, organization, and managed-runtime readiness before saving it.

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [hosted]:
API Key: <your-vals-ai-api-key>
Organization 'your-org' configured successfully.
```

No AWS, S3, logging, provider, or model-provider key is requested. Existing self-hosted fields remain in the local config so historical legacy runs keep working and rollback is reversible; new starts use the managed runtime explicitly.

Run `valkyrie config init` rather than setting `api_key` manually so readiness is checked. Hosted limitations are intentional: custom benchmark URLs, arbitrary AWS secret references, provider overrides, and Lambda hooks are rejected. Agent uploads are shared within the organization.

Each run records the runtime where it started. Managed runs continue to use managed storage after rollback; pre-cutover legacy runs continue to use the preserved self-hosted configuration.

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
Setup mode (hosted, self-hosted) [hosted]: self-hosted
AWS_ACCESS_KEY_ID: ...
AWS_SECRET_ACCESS_KEY: ...
...
```

No API key or Descope authentication is used. All data belongs to a single default organization.

## Required AWS permissions (self-hosted only)

Your AWS credentials must have the following permissions:

| Service | Permissions | Used for |
|---------|------------|----------|
| **S3** | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` | Storing benchmark results, agent artifacts, and run outputs |
| **S3** | `s3:GetObject` (for presigned URLs) | Generating download links for results |
| **CloudWatch Logs** | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | Streaming task execution logs |
| **Secrets Manager** | `secretsmanager:GetSecretValue` | Retrieving sandbox provider credentials (Daytona) and webhook URLs |
| **Lambda** (optional) | `lambda:InvokeFunction` | Post-benchmark Lambda invocation (only if using `--lambda` flag) |

These permissions should be scoped to the S3 bucket, CloudWatch log group, and Secrets Manager secrets you configure during `valkyrie config init`.
